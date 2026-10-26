"""A loopback Riverbed for unit tests — stdlib-only and in-process.

:class:`FakeRiverbed` serves the versioned ``whitewater-protocol`` wire
contract on a local ``http.server`` so application tests exercise the
real client — the same serialization, idempotency, and error-mapping
paths a live Node sees — without Docker or a running Riverbed::

    from whitewater.testing import FakeRiverbed

    def test_pipeline():
        with FakeRiverbed() as riverbed:
            client = riverbed.client()
            orders = client.feed("orders.events")
            orders.writer("ingest").append("order-1", b"created")
            with orders.reader("audit") as reader:
                page = reader.fetch()          # records arrive in order
                reader.ack()                   # durable progress

The double models the public contract — ``riverbed -> domain -> feed ->
key -> cursor -> subscription`` — with committed-order records, opaque
Cursors, ``request_id``/``mutation_id`` idempotency, Writer session
fencing, at-least-once Reader delivery (an unacknowledged page
redelivers on the next session), and manual StateStores. Feeds
materialize when a Writer/Reader/Subscription declaration names them —
the typed ``Feed`` facade never declares its own name — while Writers,
Readers, and StateStores must be declared before data-plane use, like
the real catalog. It does not model replication, multi-Node routing,
leases, pacing feedback, Indexes, or Feed-sourced StateStore replay:
reads always report a fully applied replica and guidance fields such as
``recommended_capacity`` are static.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

if TYPE_CHECKING:
    from .client import WhitewaterClient


class _Refusal(Exception):
    """One wire refusal: the ``(status, error, code)`` triple
    ``errors.map_error`` decodes into the typed hierarchy."""

    def __init__(self, status: int, error: str, code: Optional[str] = None):
        super().__init__(error)
        self.status = status
        self.error = error
        self.code = code

    @property
    def body(self) -> dict:
        body: Dict[str, Any] = {"error": self.error}
        if self.code is not None:
            body["code"] = self.code
        return body


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(field_name: str, value: Any) -> bytes:
    if not isinstance(value, str):
        raise _Refusal(400, f"{field_name} must be a base64 string")
    try:
        return base64.b64decode(value)
    except (ValueError, binascii.Error) as exc:
        raise _Refusal(400, f"{field_name} is not valid base64") from exc


def _uuid() -> str:
    return str(uuid.uuid4())


def _deterministic_uuid(request_id: str, label: str) -> str:
    """Message identity mints deterministically from request identity, the
    same rule the Node's ``deterministic_uuid`` applies — identical
    retries agree on the id."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"whitewater:{label}:{request_id}"))


def _mint_cursor(feed_id: str, request_id: str) -> str:
    """An opaque committed Cursor bound to (FeedId, request_id) — the same
    inputs the Node's ``feed_cursor`` hashes. Opaque to the SDK; the fake
    resolves it through the feed's cursor index."""
    digest = hashlib.sha256(
        f"whitewater-feed-cursor:{feed_id}:{request_id}".encode("utf-8")
    ).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _i64(field_name: str, value: Any) -> int:
    """Signed 64-bit decimal-string fields; a bad literal is refused the
    way the Node refuses it."""
    try:
        return int(str(value))
    except (TypeError, ValueError) as exc:
        raise _Refusal(
            400, f"{field_name} must be a signed 64-bit decimal string"
        ) from exc


_FEEDBACK = {
    "recommended_batch_count": 100,
    "recommended_batch_bytes": 1_048_576,
    "pressure": 0.0,
    "retry_after_ms": 0,
    "max_frame_bytes": 67_108_864,
    "hot_key": False,
}

_NODE = "fake-node-1"


@dataclass
class _FeedState:
    name: str
    feed_id: str
    domain: str
    created_at_ns: int
    records: List[dict] = field(default_factory=list)
    cursor_index: Dict[str, int] = field(default_factory=dict)
    # request_id -> {"fingerprint": ..., "response": ...} for ad-hoc appends.
    append_requests: Dict[str, dict] = field(default_factory=dict)
    # (writer_session_id, writer_epoch) -> last committed sequence.
    writer_sequences: Dict[Tuple[str, int], int] = field(default_factory=dict)


@dataclass
class _WriterState:
    name: str
    writer_id: str
    feed: str
    created_at_ns: int
    session_epoch: int = 0
    session_active: bool = False
    next_sequence: int = 1
    # (session_epoch, request_id) -> {"fingerprint", "response"}.
    append_requests: Dict[Tuple[int, str], dict] = field(default_factory=dict)


@dataclass
class _ReaderState:
    name: str
    reader_id: str
    feed: str
    feed_id: str
    start: Tuple[str, Any]
    created_at_ns: int
    acknowledged_index: int = 0
    acknowledged_cursor: Optional[str] = None
    delivered_index: int = 0
    session_epoch: int = 0
    session_capacity: int = 0
    session_active: bool = False
    last_fetch_request_id: Optional[str] = None
    last_fetch_response: Optional[dict] = None


@dataclass
class _SubscriptionState:
    name: str
    subscription_id: str
    source: Tuple[str, str]          # ("feed"|"domain", name)
    start: Tuple[str, Any]
    created_at_ns: int


@dataclass
class _StoreState:
    name: str
    store_id: str
    source: Tuple[str, Optional[str]]  # ("manual", None) | ("feed", name)
    named_keys: Dict[str, bool]        # name -> unique
    created_at_ns: int
    rows: Dict[bytes, dict] = field(default_factory=dict)
    committed_seq: int = 0
    # mutation_id -> {"fingerprint", "response"}.
    mutations: Dict[str, dict] = field(default_factory=dict)
    dropped: bool = False


@dataclass
class _PipeState:
    name: str
    pipe_id: str
    subscription: str
    subscription_id: str
    operation: dict
    output_feed_id: Optional[str]
    output_store_id: Optional[str]
    created_at_ns: int
    stage: str = "declared"


@dataclass
class _IndexState:
    name: str
    index_id: str
    feed: str
    fields: List[dict]
    unique: bool
    created_at_ns: int
    stage: str = "declared"


@dataclass
class _NamedResource:
    name: str
    resource_id: str
    created_at_ns: int


def _canon_index_value(node: Any) -> tuple:
    """Normalize one typed IndexValueWire (``kind`` + ``value`` /
    ``value_base64``) to a comparable scalar."""
    if not isinstance(node, dict):
        raise _Refusal(400, "typed values must be objects")
    kind = node.get("kind")
    if kind == "null":
        return ("null",)
    if kind == "bool":
        return ("bool", bool(node.get("value")))
    if kind == "i64":
        return ("i64", _i64("value", node.get("value")))
    if kind == "text":
        return ("text", str(node.get("value")))
    if kind == "bytes":
        return ("bytes", _unb64("value_base64", node.get("value_base64")))
    raise _Refusal(400, f"unsupported typed value kind: {kind}")


class _Riverbed:
    """The fake's shared catalog and committed history — one lock covers
    every route so request threads interleave like a single-Node truth."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._appended = threading.Condition(self._lock)
        self.revision = 0
        self.domains: Dict[str, _NamedResource] = {}
        self.feeds: Dict[str, _FeedState] = {}
        self.writers: Dict[str, _WriterState] = {}
        self.readers: Dict[str, _ReaderState] = {}
        self.subscriptions: Dict[str, _SubscriptionState] = {}
        self.stores: Dict[str, _StoreState] = {}
        self.pipes: Dict[str, _PipeState] = {}
        self.indexes: Dict[str, _IndexState] = {}
        self.roles: Dict[str, _NamedResource] = {}
        # request_id -> committed ControlExecution body (WCL + commands).
        self.control_requests: Dict[str, dict] = {}
        # (path, request_id) -> response body for idempotent session ops.
        self.op_requests: Dict[Tuple[str, str], dict] = {}
        self.calls: List[Tuple[str, str, Any]] = []

    # ---- request entry point ---------------------------------------------

    def handle(
        self, method: str, raw_path: str, body: Any
    ) -> Tuple[int, Any]:
        with self._lock:
            self.calls.append((method, raw_path, body))
            try:
                return self._route(method, raw_path, body or {})
            except _Refusal as refusal:
                return refusal.status, refusal.body
            except (KeyError, IndexError, TypeError, AttributeError, ValueError) as exc:
                return 400, {"error": f"malformed request: {exc}"}

    def _route(
        self, method: str, raw_path: str, body: dict
    ) -> Tuple[int, Any]:
        path = urlsplit(raw_path).path
        if method == "GET":
            if path == "/health":
                return 200, self._health()
            if path == "/v1/feeds/records":
                return 200, self._feed_records(raw_path)
            if (
                path.startswith("/v1/admin/demand/")
                or path.startswith("/v1/feeds/records/")
                or path
                in (
                    "/v1/node/metrics",
                    "/v1/cluster/members",
                    "/v1/subscriptions/members/state",
                    "/v1/admin/subscriptions/progress",
                )
            ):
                raise _Refusal(
                    400, f"FakeRiverbed does not model {method} {path}"
                )
            raise _Refusal(404, f"unknown path: {method} {path}", "not_found")
        handler = self._POSTS.get(path)
        if handler is None:
            if path.startswith("/v1/subscriptions/members/") or path in (
                "/v1/indexes/query",
                "/v1/state-stores/sync",
                "/v1/state-stores/rebuild",
            ):
                raise _Refusal(
                    400, f"FakeRiverbed does not model {method} {path}"
                )
            raise _Refusal(404, f"unknown path: {method} {path}", "not_found")
        return 200, handler(self, body)

    def _health(self) -> dict:
        return {
            "status": "ok",
            "node_id": _NODE,
            "members": 1,
            "protocol": {"major": 1, "minor": 0},
            "limits": {
                "max_frame_bytes": 67_108_864,
                "max_committed_read_bytes": 16_777_216,
                "max_logical_read_frames": 10_000,
                "max_logical_read_ranges": 128,
                "subscription_member_fetch_max_limit": 256,
                "subscription_member_max_lease_ticks": 600,
            },
        }

    # ---- feed data plane --------------------------------------------------

    def _feed_named(self, name: str) -> _FeedState:
        feed = self.feeds.get(name)
        if feed is None:
            raise _Refusal(400, f"Feed does not exist: {name}")
        return feed

    def _ensure_feed(self, name: str) -> _FeedState:
        """Declarations materialize their Feed on first reference — the
        typed ``Feed`` facade never declares it itself."""
        feed = self.feeds.get(name)
        if feed is not None:
            return feed
        domain = name.split(".")[0]
        if domain not in self.domains:
            self.domains[domain] = _NamedResource(domain, _uuid(), time.time_ns())
        feed = _FeedState(name, _uuid(), domain, time.time_ns())
        self.feeds[name] = feed
        return feed

    def _position_after(self, feed: _FeedState, cursor: str) -> int:
        index = feed.cursor_index.get(cursor)
        if index is None:
            raise _Refusal(400, "Cursor does not resolve in this Feed")
        return index + 1

    def _start_index(self, feed: _FeedState, start: Tuple[str, Any]) -> int:
        kind, value = start
        if kind == "beginning":
            return 0
        if kind == "now":
            return len(feed.records)
        if kind == "cursor":
            return self._position_after(feed, str(value))
        if kind == "timestamp":
            nanos = int(value)
            for index, record in enumerate(feed.records):
                if int(record["event_time_ns"]) >= nanos:
                    return index
            return len(feed.records)
        raise _Refusal(400, f"unsupported Reader start: {kind}")

    def _commit_record(
        self,
        feed: _FeedState,
        *,
        request_id: str,
        producer_id: str,
        sequence: int,
        key_base64: Optional[str],
        payload_base64: str,
        event_time_ns: Any,
        metadata_base64: Optional[dict],
    ) -> dict:
        ingest = time.time_ns()
        event = (
            ingest if event_time_ns is None else _i64("event_time_ns", event_time_ns)
        )
        cursor = _mint_cursor(feed.feed_id, request_id)
        record = {
            "cursor": cursor,
            "message_id": _deterministic_uuid(request_id, "message"),
            "producer_id": producer_id,
            "sequence": sequence,
            "event_time_ns": str(event),
            "ingest_time_ns": str(ingest),
            "payload_base64": payload_base64,
            "metadata_base64": dict(metadata_base64 or {}),
            "source": {
                "feed_name": feed.name.rsplit(".", 1)[-1],
                "full_feed_name": feed.name,
                "feed_id": feed.feed_id,
            },
        }
        if key_base64 is not None:
            record["key_base64"] = key_base64
        feed.records.append(record)
        feed.cursor_index[cursor] = len(feed.records) - 1
        self._appended.notify_all()
        return {
            "message_id": record["message_id"],
            "cursor": cursor,
            "deduplicated": False,
            "durability": "majority_committed",
            "feedback": dict(_FEEDBACK),
        }

    @staticmethod
    def _append_fingerprint(body: dict, *extra: Any) -> str:
        """The committed content a ``request_id``/``mutation_id`` binds —
        retries must carry identical content."""
        material = {
            key: body.get(key)
            for key in (
                "key_base64",
                "payload_base64",
                "metadata_base64",
                "event_time_ns",
                "sequence",
                "writer",
                "session_epoch",
                "writer_session_id",
                "writer_epoch",
                "feed",
                "op",
            )
        }
        material.update(enumerate(extra))
        return json.dumps(material, sort_keys=True, default=str)

    def _writer_append(self, body: dict) -> dict:
        writer = self.writers.get(body.get("writer"))
        if writer is None:
            raise _Refusal(400, f"Writer does not exist: {body.get('writer')}")
        feed = self.feeds.get(writer.feed)
        if feed is None:
            raise _Refusal(400, "Writer Feed does not exist")
        if not writer.session_active:
            raise _Refusal(
                409,
                f"Writer {writer.name} has no active session",
                "stale_epoch",
            )
        epoch = body.get("session_epoch")
        if writer.session_epoch != epoch:
            raise _Refusal(
                409,
                "stale Writer session epoch: "
                f"current={writer.session_epoch}, supplied={epoch}",
                "stale_epoch",
            )
        key = _unb64("key_base64", body.get("key_base64"))
        if not key:
            raise _Refusal(400, "key_base64 must contain a non-empty key")
        _unb64("payload_base64", body.get("payload_base64"))
        request_id = str(body.get("request_id"))
        dedupe_key = (writer.session_epoch, request_id)
        fingerprint = self._append_fingerprint(body)
        seen = writer.append_requests.get(dedupe_key)
        if seen is not None:
            if seen["fingerprint"] != fingerprint:
                raise _Refusal(
                    409,
                    f"append request_id {request_id} was already committed "
                    "with different record bytes",
                )
            return {**seen["response"], "deduplicated": True}
        response = self._commit_record(
            feed,
            request_id=request_id,
            producer_id=writer.writer_id,
            sequence=writer.next_sequence,
            key_base64=body.get("key_base64"),
            payload_base64=body["payload_base64"],
            event_time_ns=body.get("event_time_ns"),
            metadata_base64=body.get("metadata_base64"),
        )
        writer.next_sequence += 1
        writer.append_requests[dedupe_key] = {
            "fingerprint": fingerprint,
            "response": response,
        }
        return response

    def _writer_append_batch(self, body: dict) -> dict:
        records = body.get("records")
        if not isinstance(records, list) or not 1 <= len(records) <= 1_000:
            raise _Refusal(
                400, "Writer batch must contain between 1 and 1000 records"
            )
        results = [self._writer_append(record) for record in records]
        return {"results": results, "feedback": dict(_FEEDBACK)}

    def _feed_append(self, body: dict) -> dict:
        feed = self._feed_named(str(body.get("feed")))
        key = _unb64("key_base64", body.get("key_base64"))
        if not key:
            raise _Refusal(400, "key_base64 must contain a non-empty key")
        _unb64("payload_base64", body.get("payload_base64"))
        request_id = str(body.get("request_id"))
        fingerprint = self._append_fingerprint(body)
        seen = feed.append_requests.get(request_id)
        if seen is not None:
            if seen["fingerprint"] != fingerprint:
                raise _Refusal(
                    409,
                    f"append request_id {request_id} was already committed "
                    "with different record bytes",
                )
            return {**seen["response"], "deduplicated": True}
        session_id = str(body.get("writer_session_id"))
        epoch = int(body.get("writer_epoch"))
        sequence = int(body.get("sequence"))
        identity = (session_id, epoch)
        latest = feed.writer_sequences.get(identity, 0)
        if sequence <= latest:
            raise _Refusal(
                409,
                f"Writer sequence {sequence} is stale; "
                f"latest accepted sequence is {latest}",
                "writer.stale_sequence",
            )
        if sequence > latest + 1:
            raise _Refusal(
                409,
                f"Writer sequence {sequence} has a gap; expected {latest + 1}",
                "writer.sequence_gap",
            )
        response = self._commit_record(
            feed,
            request_id=request_id,
            producer_id=session_id,
            sequence=sequence,
            key_base64=body.get("key_base64"),
            payload_base64=body["payload_base64"],
            event_time_ns=body.get("event_time_ns"),
            metadata_base64=body.get("metadata_base64"),
        )
        feed.writer_sequences[identity] = sequence
        feed.append_requests[request_id] = {
            "fingerprint": fingerprint,
            "response": response,
        }
        return response

    def _feed_records(self, raw_path: str) -> List[dict]:
        query = parse_qs(urlsplit(raw_path).query)
        feed = self._feed_named(query.get("feed", [""])[0])
        start = 0
        after = query.get("after", [None])[0]
        if after is not None:
            start = self._position_after(feed, after)
        limit = int(query.get("limit", ["100"])[0])
        return feed.records[start : start + max(0, limit)]

    # ---- reader data plane ------------------------------------------------

    def _reader_named(self, name: str) -> _ReaderState:
        reader = self.readers.get(name)
        if reader is None:
            raise _Refusal(400, "Reader does not exist")
        return reader

    def _reader_session_body(self, reader: _ReaderState) -> dict:
        feed = self.feeds.get(reader.feed)
        delivered: Optional[str] = None
        if feed is not None and reader.delivered_index > 0:
            delivered = feed.records[reader.delivered_index - 1]["cursor"]
        return {
            "reader_id": reader.reader_id,
            "feed_id": reader.feed_id,
            "session_epoch": reader.session_epoch,
            "capacity": reader.session_capacity,
            "delivered_cursor": delivered,
            "acknowledged_cursor": reader.acknowledged_cursor,
        }

    def _dedupe_op(self, path: str, body: dict) -> Optional[dict]:
        if body.get("request_id") is not None:
            return self.op_requests.get((path, str(body["request_id"])))
        return None

    def _reader_open(self, body: dict) -> dict:
        seen = self._dedupe_op("/v1/readers/open", body)
        if seen is not None:
            return seen
        reader = self._reader_named(str(body.get("reader")))
        capacity = int(body.get("capacity") or 0)
        if not 1 <= capacity <= 10_000:
            raise _Refusal(
                400, "Reader capacity must be between 1 and 10000"
            )
        reader.session_epoch += 1
        reader.session_capacity = capacity
        reader.session_active = True
        # Delivered resets to acknowledged progress — unacked work
        # redelivers on the new session.
        reader.delivered_index = reader.acknowledged_index
        reader.last_fetch_request_id = None
        reader.last_fetch_response = None
        response = self._reader_session_body(reader)
        if body.get("request_id") is not None:
            self.op_requests[("/v1/readers/open", str(body["request_id"]))] = response
        return response

    def _reader_session_guard(self, body: dict) -> _ReaderState:
        reader = self._reader_named(str(body.get("reader")))
        if not reader.session_active or reader.session_epoch != body.get("session_epoch"):
            raise _Refusal(
                409, "Reader session is stale or inactive", "stale_epoch"
            )
        return reader

    def _reader_fetch(self, body: dict) -> dict:
        reader = self._reader_session_guard(body)
        request_id = body.get("request_id")
        if (
            reader.last_fetch_response is not None
            and reader.last_fetch_request_id == request_id
        ):
            return reader.last_fetch_response   # replay the committed page
        feed = self._feed_named(reader.feed)
        requested = body.get("limit")
        limit = reader.session_capacity if requested is None else int(requested)
        limit = max(1, min(limit, reader.session_capacity))
        page = feed.records[reader.delivered_index : reader.delivered_index + limit]
        if page:
            reader.delivered_index += len(page)
        session = self._reader_session_body(reader)
        response = {
            "records": page,
            "delivered_cursor": page[-1]["cursor"] if page else session["delivered_cursor"],
            "acknowledged_cursor": reader.acknowledged_cursor,
            "recommended_capacity": limit,
            "retry_after_ms": 0,
        }
        if page:
            reader.last_fetch_request_id = request_id
            reader.last_fetch_response = response
        return response

    def _reader_ack(self, body: dict) -> dict:
        seen = self._dedupe_op("/v1/readers/ack", body)
        if seen is not None:
            return seen
        reader = self._reader_session_guard(body)
        feed = self._feed_named(reader.feed)
        cursor = str(body.get("cursor"))
        reader.acknowledged_index = self._position_after(feed, cursor)
        reader.acknowledged_cursor = cursor
        response = self._reader_session_body(reader)
        if body.get("request_id") is not None:
            self.op_requests[("/v1/readers/ack", str(body["request_id"]))] = response
        return response

    def _reader_close(self, body: dict) -> dict:
        seen = self._dedupe_op("/v1/readers/close", body)
        if seen is not None:
            return seen
        reader = self._reader_session_guard(body)
        reader.session_active = False
        response = self._reader_session_body(reader)
        if body.get("request_id") is not None:
            self.op_requests[("/v1/readers/close", str(body["request_id"]))] = response
        return response

    def _temporary_fetch(self, body: dict) -> dict:
        if body.get("new_only") and body.get("after") is not None:
            raise _Refusal(400, "new_only cannot be combined with after")
        if body.get("after_event_time_ns") is not None and (
            body.get("after") is not None or body.get("new_only")
        ):
            raise _Refusal(
                400,
                "after_event_time_ns cannot be combined with after or new_only",
            )
        after_event_time_ns = body.get("after_event_time_ns")
        if after_event_time_ns is not None:
            after_event_time_ns = _i64("after_event_time_ns", after_event_time_ns)
        feed = self._feed_named(str(body.get("feed")))
        limit = max(1, min(int(body.get("limit") or 100), 10_000))
        tail = bool(body.get("tail"))
        if body.get("new_only"):
            return {
                "records": [],
                "next_cursor": feed.records[-1]["cursor"] if feed.records else None,
            }
        deadline = time.monotonic() + min(int(body.get("wait_ms") or 0), 30_000) / 1000.0
        while True:
            if body.get("after") is not None:
                start = self._position_after(feed, str(body["after"]))
            else:
                start = 0
            if after_event_time_ns is not None:
                frames = [
                    record
                    for record in feed.records[start:]
                    if int(record["event_time_ns"]) >= after_event_time_ns
                ][:limit]
            else:
                frames = feed.records[start : start + limit]
            if tail and len(frames) > limit:
                frames = frames[len(frames) - limit :]
            if frames or time.monotonic() >= deadline:
                return {
                    "records": frames,
                    "next_cursor": frames[-1]["cursor"] if frames else body.get("after"),
                }
            self._appended.wait(
                timeout=min(0.05, max(0.0, deadline - time.monotonic()))
            )

    # ---- state stores ------------------------------------------------------

    def _store_named(self, name: str) -> _StoreState:
        store = self.stores.get(name)
        if store is None or store.dropped:
            raise _Refusal(400, f"StateStore does not exist: {name}")
        return store

    def _store_freshness(self, store: _StoreState) -> dict:
        return {
            "applied_seq": store.committed_seq,
            "committed_seq": store.committed_seq,
            "behind": False,
            "served_by": _NODE,
        }

    def _mutation_response(
        self, store: _StoreState, mutation_id: str, fingerprint: str
    ) -> Optional[dict]:
        """``mutation_id`` is the caller's idempotency identity: an
        identical retry returns the committed outcome; reused identity
        with different content conflicts."""
        seen = store.mutations.get(mutation_id)
        if seen is None:
            return None
        if seen["fingerprint"] != fingerprint:
            raise _Refusal(
                409,
                "StateStore journal has a conflicting identity, "
                "request, or mutation",
                "state_store.conflict",
            )
        return {**seen["response"], "deduplicated": True}

    def _commit_mutation(
        self, store: _StoreState, mutation_id: str, fingerprint: str
    ) -> dict:
        store.committed_seq += 1
        response = {
            "store_id": store.store_id,
            "mutation_id": mutation_id,
            "sequence": store.committed_seq,
            "deduplicated": False,
        }
        store.mutations[mutation_id] = {
            "fingerprint": fingerprint,
            "response": response,
        }
        return response

    def _store_mutate(self, body: dict) -> dict:
        store = self._store_named(str(body.get("store")))
        if store.source[0] != "manual":
            raise _Refusal(
                400,
                "mutations apply only to manual StateStores; "
                "Feed-derived stores replay from their source",
            )
        mutation_id = str(body.get("mutation_id"))
        op = body.get("op") or {}
        fingerprint = self._append_fingerprint({"op": op, "store": store.name})
        seen = self._mutation_response(store, mutation_id, fingerprint)
        if seen is not None:
            return seen
        kind = op.get("kind")
        if kind == "put_row":
            key = _unb64("key_base64", op.get("key_base64"))
            if not key:
                raise _Refusal(400, "key_base64 is empty or exceeds the row-key budget")
            payload = _unb64("payload_base64", op.get("payload_base64"))
            named = op.get("named") or []
            if len(named) > 10:
                raise _Refusal(
                    400, "a row may write at most the store's named-key budget"
                )
            values: Dict[str, List[tuple]] = {}
            for entry in named:
                name = entry.get("name")
                if name not in store.named_keys:
                    raise _Refusal(
                        400,
                        f"StateStore {store.name} does not declare named key {name}",
                    )
                values[name] = [
                    _canon_index_value(v) for v in entry.get("values") or []
                ]
            # Unique named keys claim one row per value, arbitrated
            # first-committed-wins by journal order.
            for name, canon_values in values.items():
                if not store.named_keys.get(name):
                    continue
                for other_key, other in store.rows.items():
                    if other_key == key:
                        continue
                    if any(v in other["named"].get(name, []) for v in canon_values):
                        raise _Refusal(
                            409,
                            "StateStore journal has a conflicting identity, "
                            "request, or mutation",
                            "state_store.conflict",
                        )
            store.rows[key] = {
                "payload": payload,
                "named": values,
                "applied_cursor": f"{store.committed_seq + 1:020d}",
            }
        elif kind == "delete_row":
            key = _unb64("key_base64", op.get("key_base64"))
            store.rows.pop(key, None)
        else:
            raise _Refusal(400, f"unsupported mutation kind: {kind}")
        return self._commit_mutation(store, mutation_id, fingerprint)

    def _store_lookup(self, body: dict) -> dict:
        store = self._store_named(str(body.get("store")))
        key = _unb64("key_base64", body.get("key_base64"))
        if not key:
            raise _Refusal(400, "key_base64 is empty or exceeds the row-key budget")
        row = store.rows.get(key)
        return {
            "store_id": store.store_id,
            "found": row is not None,
            "payload_base64": _b64(row["payload"]) if row is not None else None,
            "applied_cursor": row["applied_cursor"] if row is not None else None,
            **self._store_freshness(store),
        }

    def _store_get_by(self, body: dict) -> dict:
        store = self._store_named(str(body.get("store")))
        named_key = body.get("named_key")
        if named_key not in store.named_keys:
            raise _Refusal(
                400,
                f"StateStore {store.name} does not declare named key {named_key}",
            )
        values = [_canon_index_value(v) for v in body.get("values") or []]
        if not values:
            raise _Refusal(400, "values must carry at least one typed value")
        limit = max(1, min(int(body.get("limit") or 64), 1_000))
        rows = []
        for key, row in sorted(store.rows.items()):
            if row["named"].get(named_key) == values:
                rows.append(
                    {
                        "key_base64": _b64(key),
                        "payload_base64": _b64(row["payload"]),
                        "applied_cursor": row["applied_cursor"],
                    }
                )
            if len(rows) >= limit:
                break
        return {"store_id": store.store_id, "rows": rows, **self._store_freshness(store)}

    def _store_iterate(self, body: dict) -> dict:
        store = self._store_named(str(body.get("store")))
        after = body.get("after_key_base64")
        after_key = _unb64("after_key_base64", after) if after is not None else None
        limit = max(1, min(int(body.get("limit") or 256), 1_000))
        keys = sorted(k for k in store.rows if after_key is None or k > after_key)
        page_keys = keys[:limit]
        rows = [
            {
                "key_base64": _b64(key),
                "payload_base64": _b64(store.rows[key]["payload"]),
                "applied_seq": store.committed_seq,
            }
            for key in page_keys
        ]
        return {
            "store_id": store.store_id,
            "rows": rows,
            "next_key_base64": _b64(page_keys[-1]) if len(keys) > limit else None,
            "pinned_seq": store.committed_seq,
            "served_by": _NODE,
        }

    def _store_clear(self, body: dict) -> dict:
        store = self._store_named(str(body.get("store")))
        if store.source[0] != "manual":
            raise _Refusal(
                400,
                "clear applies only to manual StateStores; "
                "Feed-derived stores replay from their source",
            )
        mutation_id = str(body.get("mutation_id"))
        fingerprint = self._append_fingerprint({"op": {"kind": "clear"}, "store": store.name})
        seen = self._mutation_response(store, mutation_id, fingerprint)
        if seen is not None:
            return seen
        store.rows.clear()
        return self._commit_mutation(store, mutation_id, fingerprint)

    # ---- control plane ------------------------------------------------------

    _RESOURCE_TABLES = {
        "domain": "domains",
        "space": "domains",
        "feed": "feeds",
        "writer": "writers",
        "reader": "readers",
        "subscription": "subscriptions",
        "pipe": "pipes",
        "index": "indexes",
        "state_store": "stores",
        "statestore": "stores",
        "role": "roles",
    }

    _SHOW_TABLES = {
        "domains": "domains",
        "spaces": "domains",
        "feeds": "feeds",
        "writers": "writers",
        "readers": "readers",
        "subscriptions": "subscriptions",
        "pipes": "pipes",
        "indexes": "indexes",
        "state_stores": "stores",
        "statestores": "stores",
        "roles": "roles",
    }

    def _space_id_of(self, resource_name: str) -> Optional[str]:
        """space_id of the Domain owning ``resource_name`` — the fake
        scopes by first dotted segment, matching its domain derivation."""
        item = self.domains.get(resource_name.split(".")[0])
        return item.resource_id if item is not None else None

    def _subscription_space_id(self, subscription_id: Any) -> Optional[str]:
        for sub in self.subscriptions.values():
            if sub.subscription_id == subscription_id:
                if sub.source[0] == "domain":
                    domain = self.domains.get(sub.source[1])
                    return domain.resource_id if domain is not None else None
                return self._space_id_of(sub.source[1])
        return None

    def _resource_row(self, table: str, item: Any) -> dict:
        """The catalog row SHOW/DESCRIBE surfaces for one resource."""
        if table == "domains":
            return {"name": item.name, "space_id": item.resource_id, "domain_id": item.resource_id, "status": "active", "created_at_ns": item.created_at_ns}
        if table == "feeds":
            domain = self.domains.get(item.domain)
            return {"name": item.name, "feed_id": item.feed_id, "space_id": domain.resource_id if domain else None, "domain": item.domain, "status": "active", "created_at_ns": item.created_at_ns}
        if table == "writers":
            feed = self.feeds.get(item.feed)
            return {"name": item.name, "writer_id": item.writer_id, "feed": item.feed, "feed_id": feed.feed_id if feed else None, "status": "active", "created_at_ns": item.created_at_ns, "session_epoch": item.session_epoch, "session_active": item.session_active, "next_sequence": item.next_sequence}
        if table == "readers":
            row = {"name": item.name, "reader_id": item.reader_id, "feed": item.feed, "feed_id": item.feed_id, "start": {"kind": item.start[0]}, "status": "active", "created_at_ns": item.created_at_ns, "session_epoch": item.session_epoch, "session_active": item.session_active, "session_capacity": item.session_capacity}
            row.update({k: v for k, v in self._reader_session_body(item).items() if k in ("delivered_cursor", "acknowledged_cursor")})
            return row
        if table == "subscriptions":
            space_id = None
            if item.source[0] == "domain":
                domain = self.domains.get(item.source[1])
                space_id = domain.resource_id if domain is not None else None
            else:
                space_id = self._space_id_of(item.source[1])
            return {"name": item.name, "subscription_id": item.subscription_id, "space_id": space_id, item.source[0]: item.source[1], "start": {"kind": item.start[0]}, "stage": "declared", "status": "active", "created_at_ns": item.created_at_ns}
        if table == "stores":
            feed_name = item.source[1]
            space_id = self._space_id_of(feed_name) if feed_name else self._space_id_of(item.name)
            return {"name": item.name, "store_id": item.store_id, "space_id": space_id, "source": {"kind": item.source[0], **({"feed": item.source[1]} if item.source[1] else {})}, "named_keys": [{"name": n, "unique": u} for n, u in item.named_keys.items()], "stage": "dropped" if item.dropped else "declared", "status": "active", "created_at_ns": item.created_at_ns}
        if table == "pipes":
            return {"name": item.name, "pipe_id": item.pipe_id, "subscription_id": item.subscription_id, "space_id": self._subscription_space_id(item.subscription_id), "operation": item.operation, "output_feed_id": item.output_feed_id, "output_store_id": item.output_store_id, "stage": item.stage, "status": "active", "created_at_ns": item.created_at_ns}
        if table == "indexes":
            return {"name": item.name, "index_id": item.index_id, "space_id": self._space_id_of(item.feed), "feed": item.feed, "fields": item.fields, "unique": item.unique, "stage": item.stage, "status": "active", "created_at_ns": item.created_at_ns}
        return {"name": item.name, "role_id": item.resource_id, "status": "active", "created_at_ns": item.created_at_ns}

    def _table(self, kind: str) -> Tuple[str, dict]:
        table = self._RESOURCE_TABLES.get(str(kind).lower())
        if table is None:
            raise _Refusal(400, f"unsupported resource type {kind}")
        return table, getattr(self, table)

    def _create_named(self, table: dict, name: str) -> _NamedResource:
        if name in table:
            raise _Refusal(400, f"resource already exists: {name}")
        item = _NamedResource(name, _uuid(), time.time_ns())
        table[name] = item
        return item

    def _create_feed(self, name: str, label: str = "CREATE FEED") -> Tuple[str, str, dict]:
        if name in self.feeds:
            raise _Refusal(400, f"resource already exists: {name}")
        feed = self._ensure_feed(name)
        return label, f"created Feed {name}", self._resource_row("feeds", feed)

    def _create_writer(self, name: str, feed_name: str) -> Tuple[str, str, dict]:
        if name in self.writers:
            raise _Refusal(400, f"resource already exists: {name}")
        feed = self._ensure_feed(feed_name)
        writer = _WriterState(name, _uuid(), feed.name, time.time_ns())
        self.writers[name] = writer
        return "CREATE WRITER", f"created Writer {name}", self._resource_row("writers", writer)

    def _create_reader(self, name: str, feed_name: str, start) -> Tuple[str, str, dict]:
        if name in self.readers:
            raise _Refusal(400, f"resource already exists: {name}")
        feed = self._ensure_feed(feed_name)
        reader = _ReaderState(name, _uuid(), feed.name, feed.feed_id, start, time.time_ns())
        reader.acknowledged_index = reader.delivered_index = self._start_index(feed, start)
        if start[0] == "cursor":
            reader.acknowledged_cursor = str(start[1])
        self.readers[name] = reader
        return "CREATE READER", f"created Reader {name}", self._resource_row("readers", reader)

    def _create_subscription(self, name: str, source, start) -> Tuple[str, str, dict]:
        if name in self.subscriptions:
            raise _Refusal(400, f"resource already exists: {name}")
        if source[0] == "feed":
            self._ensure_feed(source[1])
        elif source[1] not in self.domains:
            self.domains[source[1]] = _NamedResource(source[1], _uuid(), time.time_ns())
        sub = _SubscriptionState(name, _uuid(), source, start, time.time_ns())
        self.subscriptions[name] = sub
        return "CREATE SUBSCRIPTION", f"declared Subscription {name}; shared consumption is not active", self._resource_row("subscriptions", sub)

    def _open_writer_session(self, name: str) -> Tuple[str, str, dict]:
        writer = self.writers.get(name)
        if writer is None:
            raise _Refusal(400, f"resource not found: Writer {name}")
        writer.session_epoch += 1
        writer.next_sequence = 1
        writer.session_active = True
        return "OPEN WRITER SESSION", f"opened Writer session {name}", self._resource_row("writers", writer)

    def _define_state_store(self, name: str, source, named_keys: List[dict]) -> Tuple[str, str, dict]:
        store = self.stores.get(name)
        if store is not None and not store.dropped:
            raise _Refusal(400, f"resource already exists: {name}")
        if store is not None and store.dropped:
            raise _Refusal(400, f"StateStore {name} is tombstoned")
        keys: Dict[str, bool] = {}
        for key in named_keys:
            keys[str(key.get("name"))] = bool(key.get("unique"))
        feed_name = source[1] if source[0] == "feed" else None
        if feed_name is not None:
            self._ensure_feed(feed_name)
        self.stores[name] = _StoreState(
            name, _uuid(), (source[0], feed_name), keys, time.time_ns()
        )
        return "DEFINE STATE STORE", f"declared StateStore {name}", self._resource_row("stores", self.stores[name])

    def _define_pipe(self, command: dict) -> Tuple[str, str, dict]:
        name = str(command.get("name"))
        if name in self.pipes:
            raise _Refusal(400, f"resource already exists: {name}")
        subscription = str(command.get("subscription"))
        sub = self.subscriptions.get(subscription)
        if sub is None:
            raise _Refusal(400, f"resource not found: Subscription {subscription}")
        output_feed = command.get("output_feed")
        output_store = command.get("output_store")
        output_store_id = None
        if output_store:
            target = self.stores.get(str(output_store))
            if target is None or target.dropped:
                raise _Refusal(400, f"resource not found: StateStore {output_store}")
            output_store_id = target.store_id
        self.pipes[name] = _PipeState(
            name=name,
            pipe_id=_uuid(),
            subscription=subscription,
            subscription_id=sub.subscription_id,
            operation=command.get("operation") or {},
            output_feed_id=self._ensure_feed(str(output_feed)).feed_id if output_feed else None,
            output_store_id=output_store_id,
            created_at_ns=time.time_ns(),
        )
        return "DEFINE PIPE", f"declared Pipe {name}; no records flow until a Pipe driver exists", self._resource_row("pipes", self.pipes[name])

    def _set_pipe_stage(self, name: str, stage: str, label: str) -> Tuple[str, str, dict]:
        pipe = self.pipes.get(name)
        if pipe is None:
            raise _Refusal(400, f"resource not found: Pipe {name}")
        pipe.stage = stage
        return label, f"Pipe {name} is {stage}", self._resource_row("pipes", pipe)

    def _seek_reader(self, name: str, start) -> Tuple[str, str, dict]:
        reader = self._reader_named(name)
        feed = self._feed_named(reader.feed)
        reader.acknowledged_index = reader.delivered_index = self._start_index(feed, start)
        reader.acknowledged_cursor = str(start[1]) if start[0] == "cursor" else None
        reader.last_fetch_request_id = None
        reader.last_fetch_response = None
        return "SEEK READER", f"repositioned Reader {name}", self._resource_row("readers", reader)

    def _drop(self, kind: str, name: str) -> Tuple[str, str, dict]:
        table_name, table = self._table(kind)
        if name not in table:
            raise _Refusal(400, f"resource not found: {name}")
        if table_name == "stores":
            # DROP tombstones: the name cannot be recreated and every
            # data path refuses.
            table[name].dropped = True
        elif table_name == "pipes":
            del table[name]
        else:
            del table[name]
        return "DROP", f"dropped {kind} {name}", {}

    def _rename(self, kind: str, current: str, new: str) -> Tuple[str, str, dict]:
        table_name, table = self._table(kind)
        item = table.pop(current, None)
        if item is None:
            raise _Refusal(400, f"resource not found: {current}")
        if new in table:
            table[current] = item
            raise _Refusal(400, f"resource already exists: {new}")
        item.name = new
        table[new] = item
        if table_name == "feeds":
            for writer in self.writers.values():
                if writer.feed == current:
                    writer.feed = new
            for reader in self.readers.values():
                if reader.feed == current:
                    reader.feed = new
        return "RENAME", f"renamed {kind} {current} to {new}", self._resource_row(table_name, item)

    def _show(self, kind: str) -> Tuple[str, str, dict]:
        normalized = str(kind).lower()
        if normalized in ("grants", "api_keys", "storage_nodes"):
            return "SHOW", f"showing {kind}", []
        table_name = self._SHOW_TABLES.get(normalized)
        if table_name is None:
            raise _Refusal(400, f"unsupported SHOW resource {kind}")
        table = getattr(self, table_name)
        rows = [
            self._resource_row(table_name, item)
            for item in table.values()
            if not getattr(item, "dropped", False)
        ]
        return "SHOW", f"showing {kind}", rows

    def _describe(self, kind: str, name: str) -> Tuple[str, str, dict]:
        table_name, table = self._table(kind)
        item = table.get(name)
        if item is None or getattr(item, "dropped", False):
            raise _Refusal(400, f"resource not found: {name}")
        return "DESCRIBE", f"described {kind} {name}", self._resource_row(table_name, item)

    # ---- WCL ---------------------------------------------------------------

    @staticmethod
    def _split_wcl(script: str) -> List[str]:
        """Statements split on ``;`` outside quotes — the same rule the
        Node's ``split_statements`` applies."""
        statements: List[str] = []
        current: List[str] = []
        quoted = False
        index = 0
        while index < len(script):
            char = script[index]
            if char == "'" and quoted and index + 1 < len(script) and script[index + 1] == "'":
                current.append("'")
                index += 2
                continue
            if char == "'":
                quoted = not quoted
                current.append(char)
            elif char == ";" and not quoted:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
            else:
                current.append(char)
            index += 1
        if quoted:
            raise _Refusal(400, "WCL syntax error: unterminated quoted string")
        statement = "".join(current).strip()
        if statement:
            statements.append(statement)
        return statements

    @staticmethod
    def _tokens(statement: str) -> List[str]:
        """Whitespace/``,``/``=`` separate tokens; quotes group and
        ``''`` unescapes — the same rule the Node's ``tokenize`` applies."""
        tokens: List[str] = []
        current: List[str] = []
        quoted = False
        index = 0
        while index < len(statement):
            char = statement[index]
            if char == "'" and quoted and index + 1 < len(statement) and statement[index + 1] == "'":
                current.append("'")
                index += 2
                continue
            if char == "'":
                quoted = not quoted
            elif char in ",=" and not quoted:
                if current:
                    tokens.append("".join(current))
                    current = []
                tokens.append(char)
            elif char.isspace() and not quoted:
                if current:
                    tokens.append("".join(current))
                    current = []
            else:
                current.append(char)
            index += 1
        if quoted:
            raise _Refusal(400, "WCL syntax error: unterminated quoted string")
        if current:
            tokens.append("".join(current))
        return tokens

    def _wcl_start(self, tokens: List[str], index: int) -> Tuple[Tuple[str, Any], int]:
        token = tokens[index].upper() if index < len(tokens) else ""
        if token == "BEGINNING":
            return ("beginning", None), index + 1
        if token == "NOW":
            return ("now", None), index + 1
        if token == "CURSOR":
            return ("cursor", tokens[index + 1]), index + 2
        if token == "TIMESTAMP":
            return ("timestamp", _i64("TIMESTAMP", tokens[index + 1])), index + 2
        raise _Refusal(
            400,
            "WCL syntax error: Reader position must be BEGINNING, NOW, "
            "TIMESTAMP <ns>, or CURSOR '<value>'",
        )

    def _wcl_statement(self, tokens: List[str]) -> Tuple[str, str, dict]:
        def kw(index: int) -> Optional[str]:
            return tokens[index].upper() if index < len(tokens) else None

        if kw(0) == "CREATE":
            kind = kw(1)
            if kind in ("DOMAIN", "SPACE"):
                self._create_named(self.domains, tokens[2])
                return f"CREATE {kind}", f"created Domain {tokens[2]}", self._resource_row("domains", self.domains[tokens[2]])
            if kind == "FEED":
                return self._create_feed(tokens[2])
            if kind == "ROLE":
                self._create_named(self.roles, tokens[2])
                return "CREATE ROLE", f"created Role {tokens[2]}", self._resource_row("roles", self.roles[tokens[2]])
            if kind == "WRITER":
                if kw(3) != "TO":
                    raise _Refusal(400, "WCL syntax error: CREATE WRITER expects TO <feed>")
                return self._create_writer(tokens[2], tokens[4])
            if kind in ("READER", "SUBSCRIPTION"):
                if kw(3) != "FROM":
                    raise _Refusal(400, f"WCL syntax error: CREATE {kind} expects FROM <source>")
                position = 5 if kw(4) == "DOMAIN" else 4
                source_name = tokens[position] if position < len(tokens) else None
                if source_name is None:
                    raise _Refusal(400, f"WCL syntax error: CREATE {kind} expects a source name")
                start = ("beginning", None)
                if kw(position + 1) == "START" and kw(position + 2) == "AT":
                    start, _ = self._wcl_start(tokens, position + 3)
                if kind == "READER":
                    if kw(4) == "DOMAIN":
                        raise _Refusal(400, "Readers consume an exact Feed; FROM DOMAIN applies to SUBSCRIPTION")
                    return self._create_reader(tokens[2], source_name, start)
                source = ("domain", source_name) if kw(4) == "DOMAIN" else ("feed", source_name)
                return self._create_subscription(tokens[2], source, start)
            if kind == "PIPE":
                # CREATE PIPE <name> FROM SUBSCRIPTION <sub> TO FEED|STATESTORE <t> ...
                if kw(3) != "FROM" or kw(4) != "SUBSCRIPTION" or kw(6) != "TO":
                    raise _Refusal(400, "WCL syntax error: CREATE PIPE expects FROM SUBSCRIPTION <sub> TO <target>")
                command = {"name": tokens[2], "subscription": tokens[5], "operation": {"operation": "forward"}}
                if kw(7) == "FEED":
                    command["output_feed"] = tokens[8]
                elif kw(7) == "STATESTORE":
                    command["output_store"] = tokens[8]
                else:
                    raise _Refusal(400, "WCL syntax error: CREATE PIPE target accepts FEED or STATESTORE")
                return self._define_pipe(command)
            if kind == "INDEX" or (kind == "UNIQUE" and kw(2) == "INDEX"):
                offset = 1 if kind == "UNIQUE" else 0
                name = tokens[2 + offset]
                feed_name = tokens[4 + offset] if kw(3 + offset) == "ON" else None
                if feed_name is None:
                    raise _Refusal(400, "WCL syntax error: CREATE INDEX expects ON <feed>")
                self._ensure_feed(feed_name)
                if name in self.indexes:
                    raise _Refusal(400, f"resource already exists: {name}")
                self.indexes[name] = _IndexState(name, _uuid(), feed_name, [], kind == "UNIQUE", time.time_ns())
                return "DEFINE INDEX", f"declared Index {name}", self._resource_row("indexes", self.indexes[name])
            raise _Refusal(
                400,
                "WCL syntax error: CREATE supports DOMAIN, FEED, WRITER, "
                "READER, SUBSCRIPTION, PIPE, ROLE, and INDEX",
            )
        if kw(0) == "DEFINE" and kw(1) == "STATESTORE":
            name = tokens[2]
            position = 3
            source = ("manual", None)
            if kw(position) == "FROM":
                if kw(position + 1) != "FEED":
                    raise _Refusal(400, "WCL syntax error: DEFINE STATESTORE FROM expects FEED <feed>")
                source = ("feed", tokens[position + 2])
                position += 3
            named_keys: List[dict] = []
            if position < len(tokens):
                if kw(position) != "KEYS":
                    raise _Refusal(400, f"WCL syntax error: unexpected token {tokens[position]}")
                position += 1
                while position < len(tokens):
                    key_name = tokens[position]
                    position += 1
                    if kw(position) == "=":
                        # `name = KEY|METADATA 'x'|JSON 'p'` extractor spec.
                        position += 1
                        while position < len(tokens) and kw(position) not in ("UNIQUE", ","):
                            position += 1
                    unique = kw(position) == "UNIQUE"
                    if unique:
                        position += 1
                    named_keys.append({"name": key_name, "unique": unique})
                    if position < len(tokens):
                        if kw(position) != ",":
                            raise _Refusal(400, "WCL syntax error: KEYS entries separate with ','")
                        position += 1
            if not named_keys and any(t.upper() == "KEYS" for t in tokens):
                raise _Refusal(400, "DEFINE STATESTORE KEYS requires at least one key name")
            return self._define_state_store(name, source, named_keys)
        if kw(0) == "OPEN" and kw(1) == "WRITER" and kw(2) == "SESSION":
            return self._open_writer_session(tokens[3])
        if kw(0) == "REVOKE" and kw(1) == "WRITER" and kw(2) == "SESSION":
            name = tokens[3]
            epoch = int(tokens[5]) if kw(4) == "EPOCH" else None
            writer = self.writers.get(name)
            if writer is None:
                raise _Refusal(400, f"resource not found: Writer {name}")
            if epoch is not None and writer.session_epoch != epoch:
                raise _Refusal(409, f"stale Writer session epoch: current={writer.session_epoch}, supplied={epoch}", "stale_epoch")
            writer.session_active = False
            return "REVOKE WRITER SESSION", f"revoked Writer session {name}", self._resource_row("writers", writer)
        if kw(0) == "ALLOCATE" and kw(1) == "WRITER" and kw(2) == "SEQUENCE":
            name = tokens[3]
            epoch = int(tokens[5]) if kw(4) == "EPOCH" else None
            writer = self.writers.get(name)
            if writer is None:
                raise _Refusal(400, f"resource not found: Writer {name}")
            if not writer.session_active or writer.session_epoch != epoch:
                raise _Refusal(409, f"stale Writer session epoch: current={writer.session_epoch}, supplied={epoch}", "stale_epoch")
            sequence = writer.next_sequence
            writer.next_sequence += 1
            return "ALLOCATE WRITER SEQUENCE", f"allocated Writer sequence {sequence} for {name}", {"writer_id": writer.writer_id, "feed_id": self.feeds[writer.feed].feed_id, "session_epoch": writer.session_epoch, "sequence": sequence}
        if kw(0) == "SEEK" and kw(1) in ("READER", "SUBSCRIPTION") and kw(3) == "TO":
            start, _ = self._wcl_start(tokens, 4)
            if kw(1) == "READER":
                return self._seek_reader(tokens[2], start)
            sub = self.subscriptions.get(tokens[2])
            if sub is None:
                raise _Refusal(400, f"resource not found: Subscription {tokens[2]}")
            return "SEEK SUBSCRIPTION", f"repositioned Subscription {tokens[2]}", self._resource_row("subscriptions", sub)
        if kw(0) == "DESCRIBE":
            return self._describe(tokens[1], tokens[2])
        if kw(0) == "RENAME":
            if kw(3) != "TO":
                raise _Refusal(400, "WCL syntax error: RENAME expects TO <name>")
            return self._rename(tokens[1], tokens[2], tokens[4])
        if kw(0) == "DROP":
            return self._drop(tokens[1], tokens[2])
        if kw(0) == "SHOW":
            if kw(1) == "STORAGE" and kw(2) == "NODES":
                return "SHOW", "showing STORAGE NODES", []
            if kw(1) == "API" and kw(2) == "KEYS":
                return "SHOW", "showing API KEYS", []
            if kw(1) == "GRANTS":
                return "SHOW", "showing GRANTS", []
            return self._show(tokens[1])
        if kw(0) in ("PAUSE", "RESUME") and kw(1) == "PIPE":
            label = f"{kw(0)} PIPE"
            return self._set_pipe_stage(tokens[2], "paused" if kw(0) == "PAUSE" else "declared", label)
        if kw(0) in ("GRANT", "DENY"):
            return kw(0), f"recorded {kw(0)}", {}
        if kw(0) in (
            "MOVE", "EXPLAIN", "INSPECT", "TRANSFER", "REGISTER", "RETIRE",
            "DRAIN", "UNDRAIN", "REBUILD", "RECOVER", "PREPARE", "ABANDON",
            "ACTIVATE", "RECORD", "CLEAR", "REPORT", "COMPLETE", "ISSUE",
            "REVOKE",
        ):
            return kw(0), f"{kw(0)} accepted", {}
        raise _Refusal(400, f"unsupported statement: {' '.join(tokens)}")

    def _admin_wcl(self, body: dict) -> dict:
        request_id = str(body.get("request_id") or _uuid())
        seen = self.control_requests.get(request_id)
        if seen is not None:
            return seen
        script = body.get("script")
        if not isinstance(script, str):
            raise _Refusal(400, "malformed request: 'script'")
        results = [
            {"statement": label, "message": message, "data": data}
            for label, message, data in (
                self._wcl_statement(self._tokens(statement))
                for statement in self._split_wcl(script)
            )
        ]
        self.revision += len(results)
        execution = {
            "request_id": request_id,
            "revision": self.revision,
            "authority": "control_plane",
            "warning": "",
            "results": results,
        }
        self.control_requests[request_id] = execution
        return execution

    # ---- typed commands -----------------------------------------------------

    @staticmethod
    def _typed_start(node: Any) -> Tuple[str, Any]:
        if not isinstance(node, dict):
            return ("beginning", None)
        kind = node.get("kind")
        if kind == "now":
            return ("now", None)
        if kind == "cursor":
            return ("cursor", node.get("cursor"))
        if kind == "timestamp":
            return ("timestamp", int(node.get("nanos", node.get("cursor", 0))))
        return ("beginning", None)

    def _typed_command(self, command: dict) -> Tuple[str, str, dict]:
        kind = command.get("command")
        if kind == "show":
            return self._show(str(command.get("kind")))
        if kind == "describe":
            return self._describe(str(command.get("kind")), str(command.get("name")))
        if kind == "drop":
            return self._drop(str(command.get("kind")), str(command.get("name")))
        if kind == "rename":
            return self._rename(str(command.get("kind")), str(command.get("current")), str(command.get("new")))
        if kind == "define_state_store":
            source = command.get("source") or {"kind": "manual"}
            return self._define_state_store(
                str(command.get("name")),
                (str(source.get("kind", "manual")), source.get("feed")),
                command.get("named_keys") or [],
            )
        if kind == "define_index":
            name = str(command.get("name"))
            if name in self.indexes:
                raise _Refusal(400, f"resource already exists: {name}")
            feed_name = str(command.get("feed"))
            self._ensure_feed(feed_name)
            self.indexes[name] = _IndexState(name, _uuid(), feed_name, command.get("fields") or [], bool(command.get("unique")), time.time_ns())
            return "DEFINE INDEX", f"declared Index {name}", self._resource_row("indexes", self.indexes[name])
        if kind == "define_pipe":
            return self._define_pipe(command)
        if kind == "pause_pipe":
            return self._set_pipe_stage(str(command.get("name")), "paused", "PAUSE PIPE")
        if kind == "resume_pipe":
            return self._set_pipe_stage(str(command.get("name")), "declared", "RESUME PIPE")
        if kind == "open_writer_session":
            return self._open_writer_session(str(command.get("writer")))
        if kind == "revoke_writer_session":
            name = str(command.get("writer"))
            writer = self.writers.get(name)
            if writer is None:
                raise _Refusal(400, f"resource not found: Writer {name}")
            writer.session_active = False
            return "REVOKE WRITER SESSION", f"revoked Writer session {name}", self._resource_row("writers", writer)
        if kind == "create_domain" or kind == "create_space":
            name = str(command.get("name"))
            self._create_named(self.domains, name)
            return "CREATE DOMAIN", f"created Domain {name}", self._resource_row("domains", self.domains[name])
        if kind == "create_feed":
            return self._create_feed(str(command.get("name")))
        if kind == "create_role":
            name = str(command.get("name"))
            self._create_named(self.roles, name)
            return "CREATE ROLE", f"created Role {name}", self._resource_row("roles", self.roles[name])
        if kind == "create_writer":
            return self._create_writer(str(command.get("name")), str(command.get("feed")))
        if kind == "create_reader":
            return self._create_reader(str(command.get("name")), str(command.get("feed")), self._typed_start(command.get("start")))
        if kind == "create_subscription":
            feed, domain = command.get("feed"), command.get("domain")
            source = ("feed", str(feed)) if feed is not None else ("domain", str(domain))
            return self._create_subscription(str(command.get("name")), source, self._typed_start(command.get("start")))
        if kind == "seek_reader":
            return self._seek_reader(str(command.get("reader") or command.get("name")), self._typed_start(command.get("start")))
        if kind == "seek_subscription":
            sub = self.subscriptions.get(str(command.get("name")))
            if sub is None:
                raise _Refusal(400, f"resource not found: Subscription {command.get('name')}")
            return "SEEK SUBSCRIPTION", f"repositioned Subscription {command.get('name')}", self._resource_row("subscriptions", sub)
        if kind in ("grant", "revoke_grant", "deny", "issue_api_key", "revoke_api_key"):
            return str(kind).upper(), f"{kind} accepted", {}
        raise _Refusal(400, f"unsupported command: {kind}")

    def _admin_commands(self, body: dict) -> dict:
        request_id = str(body.get("request_id") or _uuid())
        seen = self.control_requests.get(request_id)
        if seen is not None:
            return seen
        commands = body.get("commands")
        if not isinstance(commands, list):
            raise _Refusal(400, "malformed request: 'commands'")
        results = [
            {"statement": label, "message": message, "data": data}
            for label, message, data in (self._typed_command(c) for c in commands)
        ]
        self.revision += len(results)
        execution = {
            "request_id": request_id,
            "revision": self.revision,
            "authority": "control_plane",
            "warning": "",
            "results": results,
        }
        self.control_requests[request_id] = execution
        return execution

    _POSTS = {}  # populated after method definitions


_Riverbed._POSTS = {
    "/v1/writers/append": _Riverbed._writer_append,
    "/v1/writers/append-batch": _Riverbed._writer_append_batch,
    "/v1/feeds/append": _Riverbed._feed_append,
    "/v1/readers/open": _Riverbed._reader_open,
    "/v1/readers/fetch": _Riverbed._reader_fetch,
    "/v1/readers/ack": _Riverbed._reader_ack,
    "/v1/readers/close": _Riverbed._reader_close,
    "/v1/readers/temporary/fetch": _Riverbed._temporary_fetch,
    "/v1/state-stores/mutate": _Riverbed._store_mutate,
    "/v1/state-stores/lookup": _Riverbed._store_lookup,
    "/v1/state-stores/get-by": _Riverbed._store_get_by,
    "/v1/state-stores/iterate": _Riverbed._store_iterate,
    "/v1/state-stores/clear": _Riverbed._store_clear,
    "/v1/admin/wcl": _Riverbed._admin_wcl,
    "/v1/admin/commands": _Riverbed._admin_commands,
}


def _handler(riverbed: _Riverbed):
    class _FakeHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _serve(self, method: str) -> None:
            length = int(self.headers.get("content-length") or 0)
            raw = self.rfile.read(length) if length else b""
            body: Any = {}
            if raw:
                try:
                    body = json.loads(raw)
                except ValueError:
                    status, payload = 400, {"error": "request body must be JSON"}
                    self._reply(status, payload)
                    return
            try:
                status, payload = riverbed.handle(method, self.path, body)
            except Exception as exc:  # a fake bug should read as a server failure
                status, payload = 500, {"error": f"FakeRiverbed internal error: {exc}"}
            self._reply(status, payload)

        def _reply(self, status: int, payload: Any) -> None:
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):  # noqa: N802
            self._serve("GET")

        def do_POST(self):  # noqa: N802
            self._serve("POST")

        def log_message(self, *args):
            pass

    return _FakeHandler


class FakeRiverbed:
    """A loopback Riverbed for application unit tests — the public,
    stdlib-only test double.

    The server starts on construction and stops on :meth:`close` (or
    ``with`` exit). Point the client at :attr:`endpoints`, or let
    :meth:`client` build a configured :class:`WhitewaterClient`::

        with FakeRiverbed() as riverbed:
            client = riverbed.client()
            client.feed("orders.events").writer("ingest").append("k", b"v")

    Modeled semantics: committed-order records, opaque Cursors, append
    dedupe by ``(writer, session_epoch, request_id)`` and ad-hoc
    ``request_id``, ``writer.*`` sequence fencing on raw appends, Writer
    session-epoch fencing (stale or revoked epochs refuse as ``409`` /
    ``stale_epoch``, surfaced by the SDK as ``StaleSession``), Reader
    at-least-once delivery (unacked work redelivers after close/reopen;
    a duplicate fetch ``request_id`` replays its committed page), manual
    StateStores with ``mutation_id`` dedupe, ``GET /health`` capability
    metadata, and idempotent ``/v1/admin/wcl`` + ``/v1/admin/commands``
    declarations. Unknown paths return an SDK-decodable 404 envelope.

    Not modeled: multi-Node replication and routing, subscription member
    leases, Index queries, Feed-sourced StateStore replay, auth, and
    pacing feedback.
    """

    def __init__(self) -> None:
        self._riverbed = _Riverbed()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(self._riverbed))
        self._server.daemon_threads = True
        self._closed = False
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            name="whitewater-fake-riverbed",
            daemon=True,
        )
        self._thread.start()

    @property
    def endpoints(self) -> List[str]:
        """The endpoint list ``WhitewaterClient`` consumes."""
        return [f"http://127.0.0.1:{self._server.server_port}"]

    def client(
        self, api_key: str = "test-key", timeout: float = 60.0
    ) -> "WhitewaterClient":
        """A configured :class:`WhitewaterClient` against this fake. The
        key is accepted verbatim — the fake does not model auth."""
        from .client import WhitewaterClient

        return WhitewaterClient(api_key=api_key, endpoints=self.endpoints, timeout=timeout)

    @property
    def requests(self) -> List[Tuple[str, str, Any]]:
        """Every served request as ``(method, path, body)`` — for tests
        that assert what application code put on the wire."""
        return list(self._riverbed.calls)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def __enter__(self) -> "FakeRiverbed":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
