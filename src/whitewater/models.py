"""Whitewater wire-domain models.

``Cursor`` is an opaque :class:`str` subclass: the SDK never parses,
orders, or fabricates cursors — they pass back to the Riverbed verbatim.
``event_time_ns``/``ingest_time_ns`` are signed 64-bit nanoseconds; on
the wire they travel as decimal strings (JSON numbers lose i64 range).
"""

from __future__ import annotations

import base64
import threading
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional


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


class RecordStore:
    """A named, thread-safe in-memory store of Feed records — the target
    of ``client.feeds.load(...)`` and a typical sink for
    ``client.subscribe(...)``::

        store = RecordStore.named("employee-store")
        client.feeds.load("hr.employees", ReaderStart.beginning(), store)
        with client.subscribe("hr-view", "hr.employees",
                              ReaderStart.now(), store.put_all) as sub:
            ...                 # store stays warm until close()

    Rows key on the record's key bytes; unkeyed records key on their
    Cursor so every record is retained.
    """

    def __init__(self, name: str):
        if not name or not name.strip():
            raise ValueError("store name is required")
        self.name = name
        self._rows: Dict[str, Record] = {}
        self._lock = threading.Lock()

    @staticmethod
    def named(name: str) -> "RecordStore":
        return RecordStore(name)

    def put(self, record: Record) -> None:
        """Insert or replace the row for this record's key."""
        with self._lock:
            self._rows[self._row_key(record)] = record

    def put_all(self, records: List[Record]) -> None:
        with self._lock:
            for record in records:
                self._rows[self._row_key(record)] = record

    def get(self, key: bytes) -> Optional[Record]:
        """The latest record for a key, when one has been loaded."""
        with self._lock:
            return self._rows.get(base64.b64encode(key).decode("ascii"))

    def __len__(self) -> int:
        with self._lock:
            return len(self._rows)

    def values(self) -> List[Record]:
        with self._lock:
            return list(self._rows.values())

    @staticmethod
    def _row_key(record: Record) -> str:
        if record.key is not None:
            return base64.b64encode(record.key).decode("ascii")
        return "cursor:" + str(record.cursor)
