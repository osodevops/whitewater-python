"""Whitewater client facades: resource management, Writer session,
Reader session, Subscription member, and Feed operations.

All share one :class:`_transport.Transport`, so endpoint
round-robining and error classification behave identically. Method
shapes mirror the conformance fixture operation vocabulary.
"""

from __future__ import annotations

import re
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional, Union

from . import _wire
from ._transport import Transport
from .errors import AmbiguousResult, Conflict, Refused, Unavailable, WhitewaterError
from .models import (
    MAX_NAMED_KEYS,
    AppendBatchResult,
    AppendResult,
    ControlExecution,
    Cursor,
    FeedAppend,
    Joined,
    MemberPage,
    MutationResult,
    NamedKey,
    ReaderPage,
    ReaderSession,
    ReaderStart,
    Record,
    StorePage,
    StoreRow,
    StoreRows,
    StoreValue,
    TemporaryFetchOptions,
    TemporaryFetchPage,
    WorkLease,
)

UuidLike = Union[uuid.UUID, str]

_DOTTED_RE = re.compile(r"^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$")
_RESOURCE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")


def _dotted(name: str) -> str:
    """Lowercase dotted names (domains, feeds), ≤512 bytes."""
    if not name or len(name) > 512 or not _DOTTED_RE.match(name):
        raise ValueError(f"invalid dotted name: {name!r}")
    return name


def _resource(name: str) -> str:
    """Single-segment resource names (writers, readers, subscriptions, roles)."""
    if not name or not _RESOURCE_RE.match(name):
        raise ValueError(f"invalid resource name: {name!r}")
    return name


class WriterSessionClient:
    """One named-Writer session: idempotent appends fenced by epoch."""

    def __init__(self, transport: Transport, writer: str, session_epoch: int):
        self._transport = transport
        self._writer = writer
        self._session_epoch = session_epoch

    def append(
        self,
        key: Optional[bytes],
        payload: bytes,
        *,
        request_id: Optional[uuid.UUID] = None,
        event_time_ns: Optional[int] = None,
        metadata: Optional[Dict[str, bytes]] = None,
    ) -> AppendResult:
        body = _wire.append_request(
            request_id or uuid.uuid4(),
            self._writer,
            self._session_epoch,
            key,
            payload,
            event_time_ns,
            metadata or {},
        )
        return _wire.append_result(self._transport.post("/v1/writers/append", body))

    def append_batch(self, records: List[dict]) -> AppendBatchResult:
        """Append many records; each entry takes request_id/key/payload/
        event_time_ns/metadata as in :meth:`append`."""
        body = {
            "records": [
                _wire.append_request(
                    r.get("request_id") or uuid.uuid4(),
                    self._writer,
                    self._session_epoch,
                    r.get("key"),
                    r["payload"],
                    r.get("event_time_ns"),
                    r.get("metadata") or {},
                )
                for r in records
            ]
        }
        response = self._transport.post("/v1/writers/append-batch", body)
        return AppendBatchResult(
            results=[_wire.append_result(item) for item in response.get("results") or []],
            feedback=_wire.feedback(response.get("feedback")),
        )


class ReaderSessionClient:
    """One named-Reader session: open/fetch/ack/close with durable resume."""

    def __init__(self, transport: Transport, reader: str):
        self._transport = transport
        self._reader = reader

    def open(self, capacity: int = 64, request_id: Optional[uuid.UUID] = None) -> ReaderSession:
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "reader": self._reader,
            "capacity": capacity,
        }
        return _wire.reader_session(self._transport.post("/v1/readers/open", body))

    def fetch(
        self,
        session: ReaderSession,
        limit: Optional[int] = None,
        request_id: Optional[uuid.UUID] = None,
    ) -> ReaderPage:
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "reader": self._reader,
            "session_epoch": session.session_epoch,
        }
        if limit is not None:
            body["limit"] = limit
        return _wire.reader_page(self._transport.post("/v1/readers/fetch", body))

    def ack(
        self,
        session: ReaderSession,
        cursor,
        request_id: Optional[uuid.UUID] = None,
    ) -> ReaderSession:
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "reader": self._reader,
            "session_epoch": session.session_epoch,
            "cursor": str(cursor),
        }
        return _wire.reader_session(self._transport.post("/v1/readers/ack", body))

    def close(self, session: ReaderSession) -> ReaderSession:
        body = {
            "request_id": str(uuid.uuid4()),
            "reader": self._reader,
            "session_epoch": session.session_epoch,
        }
        return _wire.reader_session(self._transport.post("/v1/readers/close", body))


class SubscriptionMemberClient:
    """One cooperating member of a named Subscription.

    Delivery is at-least-once and fenced at commit: fetch under a held
    lease, then ack the page's ``cursor``/``positions`` verbatim.
    """

    def __init__(self, transport: Transport, subscription: str):
        self._transport = transport
        self._subscription = subscription

    def _lease_body(
        self,
        member: Joined,
        work_id: UuidLike,
        lease_epoch: Optional[int],
    ) -> dict:
        body = {
            "subscription": self._subscription,
            "request_id": str(uuid.uuid4()),
            "member_id": str(member.member_id),
            "member_epoch": member.member_epoch,
            "work_id": str(work_id),
        }
        if lease_epoch is not None:
            body["lease_epoch"] = lease_epoch
        return body

    def join(self, member_id: UuidLike) -> Joined:
        body = {
            "subscription": self._subscription,
            "request_id": str(uuid.uuid4()),
            "member_id": str(member_id),
        }
        response = self._transport.post("/v1/subscriptions/members/join", body)
        return Joined(
            member_id=uuid.UUID(str(member_id)),
            member_epoch=response["member_epoch"],
            ownership_epoch=response.get("ownership_epoch", 0),
            tick=response.get("tick", 0),
        )

    def claim(
        self,
        member: Joined,
        work_id: UuidLike,
        lease_ticks: Optional[int] = None,
    ) -> WorkLease:
        return self._lease("/v1/subscriptions/members/claim", member, work_id, None, lease_ticks)

    def renew(
        self,
        member: Joined,
        lease: WorkLease,
        lease_ticks: Optional[int] = None,
    ) -> WorkLease:
        return self._lease(
            "/v1/subscriptions/members/renew",
            member,
            lease.work_id,
            lease.lease_epoch,
            lease_ticks,
        )

    def _lease(
        self,
        path: str,
        member: Joined,
        work_id: UuidLike,
        lease_epoch: Optional[int],
        lease_ticks: Optional[int],
    ) -> WorkLease:
        body = self._lease_body(member, work_id, lease_epoch)
        if lease_ticks is not None:
            body["lease_ticks"] = lease_ticks
        response = self._transport.post(path, body)
        lease = response.get("lease")
        if not lease:
            raise Conflict(409, "lease.refused", "work claim was not granted")
        return WorkLease(
            work_id=uuid.UUID(lease["work_id"]),
            member_id=uuid.UUID(lease["member_id"]),
            member_epoch=lease["member_epoch"],
            lease_epoch=lease["lease_epoch"],
            expires_at_tick=lease.get("expires_at_tick", 0),
        )

    def release(self, member: Joined, lease: WorkLease) -> None:
        self._transport.post(
            "/v1/subscriptions/members/release",
            self._lease_body(member, lease.work_id, lease.lease_epoch),
        )

    def fetch(
        self,
        member: Joined,
        lease: WorkLease,
        limit: Optional[int] = None,
    ) -> MemberPage:
        body = self._lease_body(member, lease.work_id, lease.lease_epoch)
        if limit is not None:
            body["limit"] = limit
        return _wire.member_page(self._transport.post("/v1/subscriptions/members/fetch", body))

    def ack(self, member: Joined, lease: WorkLease, page: MemberPage) -> None:
        body = self._lease_body(member, lease.work_id, lease.lease_epoch)
        if page.cursor is not None:
            body["cursor"] = str(page.cursor)
        body["positions"] = dict(page.positions)
        self._transport.post("/v1/subscriptions/members/ack", body)


class FeedClient:
    """Ad-hoc Feed operations: committed record reads, anonymous
    temporary fetches, and single-shot appends with a caller-supplied
    writer identity — no named session required."""

    def __init__(self, transport: Transport):
        self._transport = transport

    def records(
        self,
        feed: str,
        limit: Optional[int] = None,
        after: Optional[Cursor] = None,
    ) -> List[Record]:
        """Committed record history via ``GET /v1/feeds/records``."""
        path = "/v1/feeds/records?feed=" + feed
        if limit is not None:
            path += "&limit=" + str(limit)
        if after is not None:
            path += "&after=" + str(after)
        return _wire.records(self._transport.get(path))

    def temporary_fetch(
        self,
        feed: str,
        options: Optional[TemporaryFetchOptions] = None,
    ) -> TemporaryFetchPage:
        """Anonymous temporary Reader fetch: bounded page after a Cursor,
        ``new_only`` positioning, or ``tail`` + ``wait_ms`` long-poll."""
        options = options or TemporaryFetchOptions()
        body: Dict[str, Any] = {"feed": feed}
        if options.after is not None:
            body["after"] = str(options.after)
        if options.limit is not None:
            body["limit"] = options.limit
        if options.new_only:
            body["new_only"] = True
        if options.tail:
            body["tail"] = True
        if options.wait_ms is not None:
            body["wait_ms"] = options.wait_ms
        if options.after_event_time_ns is not None:
            body["after_event_time_ns"] = str(options.after_event_time_ns)
        return _wire.temporary_fetch_page(
            self._transport.post("/v1/readers/temporary/fetch", body)
        )

    def append(self, feed: str, record: FeedAppend) -> AppendResult:
        """Ad-hoc append; identical ``request_id`` retries dedupe."""
        return _wire.append_result(
            self._transport.post(
                "/v1/feeds/append", _wire.feed_append_request(feed, record)
            )
        )

    _LOAD_PAGE = 512

    def load(
        self,
        feed: str,
        start: ReaderStart,
        sink: Callable[[List[Record]], None],
    ) -> int:
        """Bulk-read committed Feed history once, from ``start``, delivering
        each page to ``sink`` until the current frontier is drained. Returns
        the number of records delivered. Use
        :meth:`WhitewaterClient.subscribe` when the sink should keep
        receiving.

        ``ReaderStart.now()`` loads nothing (the frontier is empty at call
        time); ``cursor``/``timestamp_ns`` resume from that point.
        """
        options = _load_first_page(start)
        loaded = 0
        while True:
            page = self.temporary_fetch(feed, options)
            if not page.records:
                return loaded
            sink(page.records)
            loaded += len(page.records)
            options = TemporaryFetchOptions(
                after=page.next_cursor, limit=self._LOAD_PAGE
            )


def _load_first_page(start: ReaderStart) -> TemporaryFetchOptions:
    if start.kind == "now":
        return TemporaryFetchOptions(new_only=True)
    if start.kind == "cursor":
        return TemporaryFetchOptions(after=start.cursor_value, limit=FeedClient._LOAD_PAGE)
    if start.kind == "timestamp":
        return TemporaryFetchOptions(
            after_event_time_ns=start.nanos, limit=FeedClient._LOAD_PAGE
        )
    return TemporaryFetchOptions(limit=FeedClient._LOAD_PAGE)


class Subscription:
    """A running feed subscription: a named Reader session polled on a
    background thread, invoking ``on_records`` per fetched page and
    acknowledging durable progress after each delivered batch.

    The thread is non-daemon, so a live Subscription keeps the process
    running — call :meth:`close` (or use a ``with`` block) to exit.

    Error policy: transient refusals (:class:`Unavailable`,
    :class:`AmbiguousResult`) retry after a short pause.
    :class:`StaleSession` — another client opened the same Reader —
    terminates the subscription rather than fencing back. Any other
    failure also terminates. Terminal errors reach ``on_error`` and
    :attr:`failure`.
    """

    _EMPTY_POLL_S = 0.25
    _RETRY_S = 1.0

    def __init__(
        self,
        client: "WhitewaterClient",
        reader_name: str,
        feed: str,
        start: ReaderStart,
        capacity: int,
        on_records: Callable[[List[Record]], None],
        on_error: Optional[Callable[[WhitewaterError], None]],
    ):
        self._stop = threading.Event()
        self._failure: Optional[WhitewaterError] = None
        self._thread = threading.Thread(
            target=self._run,
            args=(client, reader_name, feed, start, capacity, on_records, on_error),
            name=f"whitewater-subscription-{reader_name}",
            daemon=False,
        )
        self._thread.start()

    @property
    def running(self) -> bool:
        """False once the loop has exited (closed or failed)."""
        return not self._stop.is_set() and self._thread.is_alive()

    @property
    def failure(self) -> Optional[WhitewaterError]:
        """The terminal error, when the loop stopped on a failure."""
        return self._failure

    def close(self) -> None:
        """Signal stop, release the Reader session, and join the thread."""
        self._stop.set()
        self._thread.join(timeout=10)

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def _run(
        self,
        client: "WhitewaterClient",
        reader_name: str,
        feed: str,
        start: ReaderStart,
        capacity: int,
        on_records: Callable[[List[Record]], None],
        on_error: Optional[Callable[[WhitewaterError], None]],
    ) -> None:
        try:
            self._ensure_reader(client, reader_name, feed, start)
            reader = client.reader(reader_name)
            session = self._open_with_retry(reader, capacity)
            if session is None:
                return          # stopped while waiting to open
            try:
                while not self._stop.is_set():
                    try:
                        page = reader.fetch(session, capacity)
                        if page.records:
                            on_records(page.records)
                            if page.delivered_cursor is not None:
                                session = reader.ack(session, page.delivered_cursor)
                        else:
                            self._stop.wait(self._EMPTY_POLL_S)
                    except WhitewaterError as error:
                        if not _retryable(error):
                            raise
                        self._stop.wait(self._RETRY_S)
            finally:
                try:
                    reader.close(session)
                except WhitewaterError:
                    pass    # Session may already be superseded.
        except WhitewaterError as error:
            if not self._stop.is_set():
                self._failure = error
                if on_error is not None:
                    on_error(error)
        finally:
            self._stop.set()

    @staticmethod
    def _ensure_reader(
        client: "WhitewaterClient", reader_name: str, feed: str, start: ReaderStart
    ) -> None:
        """Declare the Reader when missing; an existing Reader is reused."""
        try:
            client.create_reader(reader_name, feed, start)
        except Refused as error:
            if "exists" not in str(error):
                raise

    def _open_with_retry(self, reader, capacity: int) -> Optional[ReaderSession]:
        while not self._stop.is_set():
            try:
                return reader.open(capacity)
            except WhitewaterError as error:
                if not _retryable(error):
                    raise
                self._stop.wait(self._RETRY_S)
        return None


def _retryable(error: WhitewaterError) -> bool:
    return isinstance(error, (Unavailable, AmbiguousResult))


class WhitewaterClient:
    """Entry point: resource management (WCL, typed commands, health) plus
    the Writer, Reader, Subscription-member, and Feed facades."""

    def __init__(
        self,
        api_key: str,
        endpoints: List[str],
        timeout: float = 60.0,
    ):
        self._transport = Transport(endpoints, api_key, timeout)
        self.feeds = FeedClient(self._transport)
        self.state_stores = StateStoresClient(self._transport)

    def wcl(self, script: str, request_id: Optional[uuid.UUID] = None) -> ControlExecution:
        """Execute a WCL script; identical ``request_id`` retries return the
        original committed execution."""
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "script": script,
        }
        return _wire.control_execution(self._transport.post("/v1/admin/wcl", body))

    def commands(
        self,
        commands: List[dict],
        request_id: Optional[uuid.UUID] = None,
    ) -> ControlExecution:
        """Execute typed control commands, e.g.
        ``[{"command": "show", "kind": "feeds"}]``."""
        body: Dict[str, Any] = {"commands": commands}
        if request_id is not None:
            body["request_id"] = str(request_id)
        return _wire.control_execution(self._transport.post("/v1/admin/commands", body))

    def healthy(self) -> bool:
        """Liveness of the contacted Node."""
        health = self._transport.get("/health")
        return isinstance(health, dict) and health.get("status") == "ok"

    # ---- Resource management --------------------------------------------------
    # Typed convenience over :meth:`wcl`: each method builds one WCL statement
    # and executes it. Names are validated before interpolation, so a bad value
    # fails locally rather than reaching the Control Plane.

    def create_domain(self, name: str) -> ControlExecution:
        """``CREATE DOMAIN <name>``"""
        return self.wcl("CREATE DOMAIN " + _dotted(name))

    def create_feed(self, name: str) -> ControlExecution:
        """``CREATE FEED <dotted.name>``"""
        return self.wcl("CREATE FEED " + _dotted(name))

    def create_writer(self, name: str, feed: str) -> ControlExecution:
        """``CREATE WRITER <name> TO <feed>``"""
        return self.wcl(f"CREATE WRITER {_resource(name)} TO {_dotted(feed)}")

    def create_reader(
        self, name: str, feed: str, start: Optional["ReaderStart"] = None
    ) -> ControlExecution:
        """``CREATE READER <name> FROM <feed> [START AT <start>]`` — BEGINNING
        when ``start`` is omitted."""
        return self._create_from("READER", name, feed, start or ReaderStart.beginning())

    def create_subscription(
        self, name: str, feed: str, start: Optional["ReaderStart"] = None
    ) -> ControlExecution:
        """``CREATE SUBSCRIPTION <name> FROM <feed> [START AT <start>]``"""
        return self._create_from(
            "SUBSCRIPTION", name, feed, start or ReaderStart.beginning()
        )

    def _create_from(
        self, kind: str, name: str, feed: str, start: "ReaderStart"
    ) -> ControlExecution:
        return self.wcl(
            f"CREATE {kind} {_resource(name)} FROM {_dotted(feed)} START AT {start.wcl}"
        )

    def create_role(self, name: str) -> ControlExecution:
        """``CREATE ROLE <name>``"""
        return self.wcl("CREATE ROLE " + _resource(name))

    def open_writer_session(self, writer: str) -> ControlExecution:
        """``OPEN WRITER SESSION <name>``; the returned epoch feeds
        :meth:`writer`."""
        return self.wcl("OPEN WRITER SESSION " + _resource(writer))

    def seek_reader(self, reader: str, start: "ReaderStart") -> ControlExecution:
        """``SEEK READER <name> TO <start>`` — repositions a durable Reader."""
        return self.wcl(f"SEEK READER {_resource(reader)} TO {start.wcl}")

    def describe_domain(self, name: str) -> ControlExecution:
        """``DESCRIBE DOMAIN <name>``"""
        return self._describe("DOMAIN", name, True)

    def describe_feed(self, name: str) -> ControlExecution:
        """``DESCRIBE FEED <dotted.name>``"""
        return self._describe("FEED", name, True)

    def describe_writer(self, name: str) -> ControlExecution:
        """``DESCRIBE WRITER <name>``"""
        return self._describe("WRITER", name, False)

    def describe_reader(self, name: str) -> ControlExecution:
        """``DESCRIBE READER <name>``"""
        return self._describe("READER", name, False)

    def describe_subscription(self, name: str) -> ControlExecution:
        """``DESCRIBE SUBSCRIPTION <name>``"""
        return self._describe("SUBSCRIPTION", name, False)

    def describe_role(self, name: str) -> ControlExecution:
        """``DESCRIBE ROLE <name>``"""
        return self._describe("ROLE", name, False)

    def rename_domain(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME DOMAIN <current> TO <renamed>``"""
        return self._rename("DOMAIN", current, renamed, True)

    def rename_feed(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME FEED <current> TO <renamed>``"""
        return self._rename("FEED", current, renamed, True)

    def rename_writer(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME WRITER <current> TO <renamed>``"""
        return self._rename("WRITER", current, renamed, False)

    def rename_reader(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME READER <current> TO <renamed>``"""
        return self._rename("READER", current, renamed, False)

    def rename_subscription(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME SUBSCRIPTION <current> TO <renamed>``"""
        return self._rename("SUBSCRIPTION", current, renamed, False)

    def rename_role(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME ROLE <current> TO <renamed>``"""
        return self._rename("ROLE", current, renamed, False)

    def drop_domain(self, name: str) -> ControlExecution:
        """``DROP DOMAIN <name>``"""
        return self._drop("DOMAIN", name, True)

    def drop_feed(self, name: str) -> ControlExecution:
        """``DROP FEED <dotted.name>``"""
        return self._drop("FEED", name, True)

    def drop_writer(self, name: str) -> ControlExecution:
        """``DROP WRITER <name>``"""
        return self._drop("WRITER", name, False)

    def drop_reader(self, name: str) -> ControlExecution:
        """``DROP READER <name>``"""
        return self._drop("READER", name, False)

    def drop_subscription(self, name: str) -> ControlExecution:
        """``DROP SUBSCRIPTION <name>``"""
        return self._drop("SUBSCRIPTION", name, False)

    def drop_role(self, name: str) -> ControlExecution:
        """``DROP ROLE <name>``"""
        return self._drop("ROLE", name, False)

    def list_domains(self) -> ControlExecution:
        """``SHOW DOMAINS`` — rows live in ``results[0].data``."""
        return self.wcl("SHOW DOMAINS")

    def list_feeds(self) -> ControlExecution:
        """``SHOW FEEDS``"""
        return self.wcl("SHOW FEEDS")

    def list_writers(self) -> ControlExecution:
        """``SHOW WRITERS``"""
        return self.wcl("SHOW WRITERS")

    def list_readers(self) -> ControlExecution:
        """``SHOW READERS``"""
        return self.wcl("SHOW READERS")

    def list_subscriptions(self) -> ControlExecution:
        """``SHOW SUBSCRIPTIONS``"""
        return self.wcl("SHOW SUBSCRIPTIONS")

    def list_roles(self) -> ControlExecution:
        """``SHOW ROLES``"""
        return self.wcl("SHOW ROLES")

    def list_grants(self) -> ControlExecution:
        """``SHOW GRANTS``"""
        return self.wcl("SHOW GRANTS")

    def _describe(self, kind: str, name: str, dotted: bool) -> ControlExecution:
        return self.wcl(f"DESCRIBE {kind} {self._name(name, dotted)}")

    def _rename(
        self, kind: str, current: str, renamed: str, dotted: bool
    ) -> ControlExecution:
        return self.wcl(
            f"RENAME {kind} {self._name(current, dotted)} TO {self._name(renamed, dotted)}"
        )

    def _drop(self, kind: str, name: str, dotted: bool) -> ControlExecution:
        return self.wcl(f"DROP {kind} {self._name(name, dotted)}")

    @staticmethod
    def _name(name: str, dotted: bool) -> str:
        return _dotted(name) if dotted else _resource(name)

    def writer(self, name: str, session_epoch: int) -> WriterSessionClient:
        return WriterSessionClient(self._transport, name, session_epoch)

    def reader(self, name: str) -> ReaderSessionClient:
        return ReaderSessionClient(self._transport, name)

    def subscription_member(self, subscription: str) -> SubscriptionMemberClient:
        return SubscriptionMemberClient(self._transport, subscription)

    _SUBSCRIBE_CAPACITY = 128

    def subscribe(
        self,
        reader: str,
        feed: str,
        start: ReaderStart,
        on_records: Callable[[List[Record]], None],
        on_error: Optional[Callable[[WhitewaterError], None]] = None,
    ) -> "Subscription":
        """Subscribe durably to a Feed: ensures a Reader named ``reader``
        exists (creating it at ``start`` when missing), opens a session,
        and invokes ``on_records`` per fetched page on a background thread,
        acknowledging after each batch.

        The returned :class:`Subscription` keeps the process running until
        ``close()`` (or a ``with`` block exits); terminal errors reach
        ``on_error`` and ``subscription.failure``::

            with client.subscribe("hr-view", "hr.employees",
                                  ReaderStart.beginning(), handle_records) as sub:
                time.sleep(...)             # or just keep working
        """
        return Subscription(
            self, reader, feed, start, self._SUBSCRIBE_CAPACITY, on_records, on_error
        )

    def state_store(self, name: str) -> "StateStore":
        """A thin facade over the node-side StateStore ``name`` — rows
        live on the Nodes (RF3 + Fjall); this client holds no state."""
        return StateStore(self._transport, name)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "WhitewaterClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()




class StateStore:
    """A thin facade over one node-side StateStore — the durable RF3 +
    Fjall store on the Nodes is the only store; this object holds no
    rows. Declare it with named keys, then put/get/delete/iterate::

        store = (client.state_store("accounts.employees")
                 .add_key("passport", unique=True)
                 .add_key("department"))
        client.state_stores.define(store)

        store.put(b"user-1", b"profile-1",
                  named={"passport": "P111", "department": "eng"})
        store.get(b"user-1")                    # Optional[StoreRow]
        store.get_by("department", "eng").rows  # every matching row
        for row in store.iterate(): ...         # primary-key order pages
        store.clear()
        client.state_stores.drop(store.name)

    ``add_key`` records declaration intent used by ``define()``; it does
    not mutate an already-declared store (named keys are declared at
    creation). Named-key values are typed scalars — ``str``, ``int``,
    ``bool``, ``bytes``, or ``None``; a list value carries one typed
    value per posting component.
    """

    MAX_NAMED_KEYS = MAX_NAMED_KEYS

    def __init__(self, transport: Optional[Transport], name: str):
        self._transport = transport
        self.name = _dotted(name)
        self._named_keys: List[NamedKey] = []

    @staticmethod
    def named(name: str) -> "StateStore":
        """An unbound declaration builder for
        :meth:`StateStoresClient.define`."""
        return StateStore(None, name)

    def add_key(self, named_key: str, unique: bool = False) -> "StateStore":
        """Declare a named secondary key for ``define`` (at most
        ``MAX_NAMED_KEYS``). Non-unique keys map one value to every
        matching primary row; ``unique`` enforces one row per value."""
        if not named_key or not named_key.strip():
            raise ValueError("named key is required")
        if any(key.name == named_key for key in self._named_keys):
            raise ValueError(f"duplicate named key: {named_key}")
        if len(self._named_keys) >= self.MAX_NAMED_KEYS:
            raise ValueError(
                f"a StateStore declares at most {self.MAX_NAMED_KEYS} named keys"
            )
        self._named_keys.append(NamedKey(name=named_key, unique=unique))
        return self

    @property
    def named_keys(self) -> List[NamedKey]:
        return list(self._named_keys)

    def _bound(self) -> Transport:
        if self._transport is None:
            raise ValueError(
                "unbound StateStore spec — pass it to client.state_stores.define() "
                "or get a bound facade via client.state_store(name)"
            )
        return self._transport

    # ---- data operations -------------------------------------------------

    def put(
        self,
        key: Union[bytes, str],
        payload: Union[bytes, str],
        named: Optional[Dict[str, Any]] = None,
        mutation_id: Optional[uuid.UUID] = None,
    ) -> MutationResult:
        """Insert or replace one row. ``named`` maps a declared named key
        to a typed value (or a list for multi-component postings)."""
        body: Dict[str, Any] = {
            "store": self.name,
            "mutation_id": str(mutation_id or uuid.uuid4()),
            "op": {
                "kind": "put_row",
                "key_base64": _wire.b64(_bytes(key)),
                "payload_base64": _wire.b64(_bytes(payload)),
            },
        }
        if named:
            body["op"]["named"] = _wire.named_values(named)
        return _wire.mutation_result(
            self._bound().post("/v1/state-stores/mutate", body)
        )

    def get(
        self,
        key: Union[bytes, str],
        strict: bool = False,
        wait_ms: Optional[int] = None,
    ) -> Optional[StoreRow]:
        """Primary-key read with freshness evidence on the returned row.
        ``strict`` bounded-waits for the serving replica to apply its
        committed frontier rather than answering stale."""
        body: Dict[str, Any] = {
            "store": self.name,
            "key_base64": _wire.b64(_bytes(key)),
            "strict": strict,
        }
        if wait_ms is not None:
            body["wait_ms"] = wait_ms
        response = self._bound().post("/v1/state-stores/lookup", body)
        if not response.get("found"):
            return None
        return StoreRow(
            key=_bytes(key),
            payload=_wire.unb64(response["payload_base64"]),
            applied_cursor=response.get("applied_cursor"),
            freshness=_wire.store_freshness(response),
        )

    def get_by(
        self,
        named_key: str,
        value: Union[StoreValue, List[StoreValue]],
        limit: Optional[int] = None,
        strict: bool = False,
        wait_ms: Optional[int] = None,
    ) -> StoreRows:
        """Named-key lookup: every primary row the value points at, plus
        the serving replica's freshness evidence."""
        values = value if isinstance(value, list) else [value]
        body: Dict[str, Any] = {
            "store": self.name,
            "named_key": named_key,
            "values": [_wire.index_value(v) for v in values],
            "strict": strict,
        }
        if limit is not None:
            body["limit"] = limit
        if wait_ms is not None:
            body["wait_ms"] = wait_ms
        return _wire.store_rows(self._bound().post("/v1/state-stores/get-by", body))

    def delete(
        self, key: Union[bytes, str], mutation_id: Optional[uuid.UUID] = None
    ) -> MutationResult:
        body: Dict[str, Any] = {
            "store": self.name,
            "mutation_id": str(mutation_id or uuid.uuid4()),
            "op": {"kind": "delete_row", "key_base64": _wire.b64(_bytes(key))},
        }
        return _wire.mutation_result(
            self._bound().post("/v1/state-stores/mutate", body)
        )

    def page(
        self,
        after_key: Optional[Union[bytes, str]] = None,
        limit: Optional[int] = None,
    ) -> StorePage:
        """One bounded iteration page in primary-key order — the same row
        set replica transfer serves, pinned to ``pinned_seq``."""
        body: Dict[str, Any] = {"store": self.name}
        if after_key is not None:
            body["after_key_base64"] = _wire.b64(_bytes(after_key))
        if limit is not None:
            body["limit"] = limit
        return _wire.store_page(self._bound().post("/v1/state-stores/iterate", body))

    def iterate(self, limit: Optional[int] = None):
        """Yield every row in primary-key order across bounded pages."""
        after: Optional[bytes] = None
        while True:
            page = self.page(after_key=after, limit=limit)
            yield from page.rows
            if page.next_key is None:
                return
            after = page.next_key

    def clear(self, mutation_id: Optional[uuid.UUID] = None) -> MutationResult:
        """Empty the store as one journaled mutation — durable and
        idempotent under ``mutation_id`` retries."""
        body: Dict[str, Any] = {
            "store": self.name,
            "mutation_id": str(mutation_id or uuid.uuid4()),
        }
        return _wire.mutation_result(
            self._bound().post("/v1/state-stores/clear", body)
        )

    def drop(self, request_id: Optional[uuid.UUID] = None) -> ControlExecution:
        """Tombstone the store: the name cannot be recreated and every
        data path refuses. Applied rows remain on replicas until reclaim."""
        body: Dict[str, Any] = {
            "commands": [
                {"command": "drop", "kind": "state_store", "name": self.name}
            ]
        }
        if request_id is not None:
            body["request_id"] = str(request_id)
        return _wire.control_execution(
            self._bound().post("/v1/admin/commands", body)
        )


def _bytes(value: Union[bytes, str]) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


class StateStoresClient:
    """Collection-level StateStore operations: declaration and drop. Row
    operations live on the bound :class:`StateStore` facade returned by
    ``client.state_store(name)``."""

    def __init__(self, transport: Transport):
        self._transport = transport

    def define(
        self,
        spec: "StateStore",
        source: Optional[str] = None,
        request_id: Optional[uuid.UUID] = None,
    ) -> "StateStore":
        """Declare ``spec`` on the Control Plane and return it bound to
        this client. ``source`` names a same-Domain Feed the store
        materializes from; omitted means a manual store (required when
        the spec declares named keys)."""
        if not isinstance(spec, StateStore):
            raise TypeError("define() takes a StateStore spec — use StateStore.named(name)")
        if spec.named_keys and source is not None:
            raise ValueError(
                "named keys require a manual StateStore until a declared "
                "source-Feed projection exists"
            )
        command: Dict[str, Any] = {
            "command": "define_state_store",
            "name": spec.name,
            "source": {"kind": "feed", "feed": _dotted(source)}
            if source is not None
            else {"kind": "manual"},
            "named_keys": [
                {"name": key.name, "unique": key.unique} for key in spec.named_keys
            ],
        }
        body: Dict[str, Any] = {"commands": [command]}
        if request_id is not None:
            body["request_id"] = str(request_id)
        _wire.control_execution(self._transport.post("/v1/admin/commands", body))
        spec._transport = self._transport
        return spec

    def drop(
        self, name: str, request_id: Optional[uuid.UUID] = None
    ) -> ControlExecution:
        return StateStore(self._transport, name).drop(request_id=request_id)
