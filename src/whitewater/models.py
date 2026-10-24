"""Whitewater wire-domain models.

``Cursor`` is an opaque :class:`str` subclass: the SDK never parses,
orders, or fabricates cursors — they pass back to the Riverbed verbatim.
``event_time_ns``/``ingest_time_ns`` are signed 64-bit nanoseconds; on
the wire they travel as decimal strings (JSON numbers lose i64 range).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Union


class Cursor(str):
    """An opaque stream position. Never parse or order these."""

    @staticmethod
    def of(value: Optional[str]) -> Optional["Cursor"]:
        return None if value is None else Cursor(value)


@dataclass(frozen=True)
class ReaderStart:
    """Where a Reader or Subscription starts consuming a Feed — maps to
    the WCL ``START AT`` clause. Build with the class factories."""

    kind: str
    cursor_value: Optional[Cursor] = None
    nanos: int = 0

    @staticmethod
    def beginning() -> "ReaderStart":
        """From the oldest retained record."""
        return ReaderStart("beginning")

    @staticmethod
    def now() -> "ReaderStart":
        """From records appended after creation."""
        return ReaderStart("now")

    @staticmethod
    def cursor(cursor: Cursor) -> "ReaderStart":
        """From an opaque Cursor previously returned by the Riverbed."""
        return ReaderStart("cursor", cursor_value=cursor)

    @staticmethod
    def timestamp_ns(nanos: int) -> "ReaderStart":
        """From a signed Unix epoch nanosecond timestamp."""
        return ReaderStart("timestamp", nanos=nanos)

    @property
    def wcl(self) -> str:
        """The WCL ``START AT`` operand, e.g. ``BEGINNING``."""
        if self.kind == "now":
            return "NOW"
        if self.kind == "cursor":
            escaped = str(self.cursor_value or "").replace("'", "''")
            return f"CURSOR '{escaped}'"
        if self.kind == "timestamp":
            return f"TIMESTAMP {self.nanos}"
        return "BEGINNING"


@dataclass(frozen=True)
class Record:
    cursor: Cursor
    message_id: uuid.UUID
    producer_id: uuid.UUID
    sequence: int
    event_time_ns: int
    ingest_time_ns: int
    key: Optional[bytes]
    payload: bytes
    metadata: Dict[str, bytes] = field(default_factory=dict)


@dataclass(frozen=True)
class ServerFeedback:
    recommended_batch_count: int
    recommended_batch_bytes: int
    pressure: float
    retry_after_ms: int
    max_frame_bytes: int
    hot_key: bool = False


@dataclass(frozen=True)
class AppendResult:
    message_id: uuid.UUID
    cursor: Cursor
    deduplicated: bool
    feedback: ServerFeedback


@dataclass(frozen=True)
class AppendBatchResult:
    results: List[AppendResult]
    feedback: ServerFeedback


@dataclass(frozen=True)
class ReaderSession:
    reader_id: uuid.UUID
    feed_id: uuid.UUID
    session_epoch: int
    capacity: int
    delivered_cursor: Optional[Cursor]
    acknowledged_cursor: Optional[Cursor]


@dataclass(frozen=True)
class ReaderPage:
    records: List[Record]
    delivered_cursor: Optional[Cursor]
    acknowledged_cursor: Optional[Cursor]
    recommended_capacity: int
    retry_after_ms: int


@dataclass(frozen=True)
class StatementResult:
    statement: str
    message: str
    data: dict


@dataclass(frozen=True)
class ControlExecution:
    request_id: uuid.UUID
    revision: int
    authority: str
    warning: str
    results: List[StatementResult]


@dataclass(frozen=True)
class Joined:
    member_id: uuid.UUID
    member_epoch: int
    ownership_epoch: int
    tick: int


@dataclass(frozen=True)
class WorkLease:
    work_id: uuid.UUID
    member_id: uuid.UUID
    member_epoch: int
    lease_epoch: int
    expires_at_tick: int


@dataclass(frozen=True)
class MemberPage:
    records: List[Record]
    cursor: Optional[Cursor]
    positions: Dict[str, str]
    ownership_epoch: int


@dataclass(frozen=True)
class FeedAppend:
    """One ad-hoc append for ``POST /v1/feeds/append``: the caller
    supplies the writer identity directly instead of holding a named
    Writer session. Identical ``request_id`` retries dedupe server-side."""

    request_id: uuid.UUID
    writer_session_id: uuid.UUID
    writer_epoch: int
    sequence: int
    key: Optional[bytes]
    payload: bytes
    event_time_ns: Optional[int] = None
    metadata: Dict[str, bytes] = field(default_factory=dict)


@dataclass(frozen=True)
class TemporaryFetchOptions:
    """Options for an anonymous temporary Reader fetch. ``after``
    continues after an opaque Cursor; ``after_event_time_ns`` seeks by
    record event time; ``new_only`` positions at the current end without
    returning history; ``tail`` plus ``wait_ms`` long-polls for newly
    committed records."""

    after: Optional[Cursor] = None
    limit: Optional[int] = None
    new_only: bool = False
    tail: bool = False
    wait_ms: Optional[int] = None
    after_event_time_ns: Optional[int] = None

    @staticmethod
    def page(after: Optional[Cursor] = None, limit: int = 100) -> "TemporaryFetchOptions":
        return TemporaryFetchOptions(after=after, limit=limit)

    @staticmethod
    def tailed(after: Optional[Cursor], limit: int, wait_ms: int) -> "TemporaryFetchOptions":
        return TemporaryFetchOptions(after=after, limit=limit, tail=True, wait_ms=wait_ms)

    @staticmethod
    def at_end() -> "TemporaryFetchOptions":
        return TemporaryFetchOptions(new_only=True)


@dataclass(frozen=True)
class TemporaryFetchPage:
    records: List[Record]
    next_cursor: Optional[Cursor]

MAX_NAMED_KEYS = 10

# A named-key value is a single typed scalar or a list of them (one
# per posting component); strings and numbers serialize typed — callers
# never hand-encode bytes for ordinary values.
StoreValue = Union[None, bool, int, str, bytes]


class Extractor:
    """How a committed record projects to a typed field value — the
    ``IndexFieldSpec`` the wire carries. Declared on
    ``StateStore.add_key(..., extractor=)`` for feed-sourced named keys
    and on ``PipeOperation.enrich``/``window`` for the lookup/grouping
    Key; applied by server-side replay and drive. Build with the class
    factories; manual stores leave it unset and take values on each
    ``put`` instead."""

    def __init__(self, body: Dict[str, Any]):
        self._body = body

    @staticmethod
    def record_key() -> "Extractor":
        """The source record's application Key."""
        return Extractor({"kind": "key"})

    @staticmethod
    def metadata(name: str) -> "Extractor":
        """A named record Metadata entry."""
        if not name or not name.strip():
            raise ValueError("metadata extractor name is required")
        return Extractor({"kind": "metadata", "name": name})

    @staticmethod
    def json(field: str) -> "Extractor":
        """A dotted JSON-path projection over the record payload."""
        if not field or not field.strip():
            raise ValueError("json extractor field is required")
        return Extractor(
            {"kind": "projection", "extractor": "json", "version": 1, "field": field}
        )

    def to_wire(self) -> Dict[str, Any]:
        """The ``IndexFieldSpec`` wire object, internally tagged on
        ``kind``."""
        return dict(self._body)


@dataclass(frozen=True)
class NamedKey:
    """One declared secondary key on a StateStore. Non-unique keys map a
    value to every primary row carrying it (the group/list lookup);
    ``unique`` claims enforce one row per value, arbitrated
    first-committed-wins by the store's journal order. ``extractor``
    projects values from a source-Feed record — feed-sourced stores only;
    manual stores take values on each ``put_row`` mutation."""

    name: str
    unique: bool = False
    extractor: Optional["Extractor"] = None


@dataclass(frozen=True)
class StoreFreshness:
    """Freshness evidence every StateStore read reports: the serving
    replica's applied and committed journal positions, whether it still
    lags, and which Node served the response."""

    applied_seq: int
    committed_seq: int
    behind: bool
    served_by: str


@dataclass(frozen=True)
class StoreRow:
    """One StateStore row: its primary key and payload bytes plus the
    journal cursor it last applied at. ``freshness`` carries the serving
    replica's evidence when the row came from a read."""

    key: bytes
    payload: bytes
    applied_cursor: Optional[str] = None
    freshness: Optional[StoreFreshness] = None


@dataclass(frozen=True)
class StoreRows:
    """A named-key lookup result: every primary row the value points at
    plus the serving replica's freshness evidence."""

    rows: List[StoreRow]
    freshness: StoreFreshness


@dataclass(frozen=True)
class StorePage:
    """One bounded iteration page in primary-key order. ``next_key``
    continues the scan; ``pinned_seq`` is the serving replica's applied
    journal position the page was pinned to."""

    rows: List[StoreRow]
    next_key: Optional[bytes]
    pinned_seq: int
    served_by: str


@dataclass(frozen=True)
class MutationResult:
    """A committed StateStore mutation: the journal sequence it landed
    at and whether ``mutation_id`` deduplicated a retry."""

    store_id: uuid.UUID
    mutation_id: uuid.UUID
    sequence: int
    deduplicated: bool
