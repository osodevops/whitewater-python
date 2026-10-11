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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Dict, List, NamedTuple, Optional, Union


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


class StoreEntry(NamedTuple):
    """A stored row: its primary key and the record value."""

    primary_key: bytes
    record: Record


class RecordStore:
    """A named, thread-safe in-memory store of Feed records — the target
    of ``client.load_store(store).from_feed(...)`` and a typical sink for
    ``client.subscribe(...)``::

        store = (RecordStore.named("employee-store")
                 .add_key("passport").add_key("department"))
        client.load_store(store).from_feed(
            "hr.employees", ReaderStart.beginning(),
            lambda s, record: s.upsert(record)
                .with_key(record.key)
                .with_named_key("passport", record.metadata["pp"]))
        with client.subscribe("hr-view", "hr.employees",
                              ReaderStart.now(), store.put_all) as sub:
            ...                 # store stays warm until close()

    The primary map keys rows on the key chosen by
    ``upsert(record).with_key(...)`` (the record's own key via
    ``put``/``put_all``, the Cursor for unkeyed records). Each declared
    named key maintains a separate postings map — conceptually
    ``employee-store.passport`` — of value -> primary keys, so named
    lookups are not unique and return every matching row.
    """

    MAX_NAMED_KEYS = MAX_NAMED_KEYS

    def __init__(self, name: str):
        if not name or not name.strip():
            raise ValueError("store name is required")
        self.name = name
        self._rows: Dict[str, Record] = {}
        self._declared_keys: List[str] = []
        # named key -> base64 value -> set of primary ids (non-unique).
        self._named_index: Dict[str, Dict[str, set]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def named(name: str) -> "RecordStore":
        return RecordStore(name)

    def add_key(self, named_key: str) -> "RecordStore":
        """Declare a named secondary key (at most MAX_NAMED_KEYS)."""
        if not named_key or not named_key.strip():
            raise ValueError("named key is required")
        with self._lock:
            if named_key not in self._named_index:
                if len(self._declared_keys) >= MAX_NAMED_KEYS:
                    raise ValueError(
                        f"a RecordStore supports at most {MAX_NAMED_KEYS} named keys")
                self._declared_keys.append(named_key)
                self._named_index[named_key] = {}
        return self

    @property
    def named_keys(self) -> List[str]:
        """The declared named keys, in declaration order."""
        return list(self._declared_keys)

    def upsert(self, record: Record) -> "Upsert":
        """Start a row write: choose the primary key and named-key values."""
        return Upsert(self, record)

    def put(self, record: Record) -> None:
        """Insert or replace the row for this record's key."""
        self._commit_upsert(record, self._default_id(record), {})

    def put_all(self, records: List[Record]) -> None:
        for record in records:
            self.put(record)

    def get(self, key: Union[bytes, str]) -> Optional[Record]:
        """The latest record for a key, when one has been loaded."""
        with self._lock:
            return self._rows.get(self._key_id(key))

    def get_by(self, named_key: str, value: Union[bytes, str]) -> List[Record]:
        """Every row whose named key equals ``value`` — named keys are
        not unique."""
        with self._lock:
            if named_key not in self._named_index:
                raise ValueError(f"undeclared named key: {named_key}")
            ids = self._named_index[named_key].get(self._key_id(value), set())
            return [self._rows[i] for i in ids if i in self._rows]

    def delete(self, key: Union[bytes, str]) -> bool:
        """Remove the row for a key plus its named-key postings."""
        with self._lock:
            row_id = self._key_id(key)
            if row_id not in self._rows:
                return False
            del self._rows[row_id]
            self._remove_postings(row_id)
            return True

    def clear(self) -> None:
        """Empty the primary map and every named-key index."""
        with self._lock:
            self._rows.clear()
            for postings in self._named_index.values():
                postings.clear()

    def copy(self, target: "RecordStore") -> None:
        """Copy every row into ``target``, re-attaching named-key
        postings; missing named keys are declared on the target."""
        for named_key in self.named_keys:
            target.add_key(named_key)
        with self._lock:
            items = list(self._rows.items())
            named = {row_id: self._named_values_of(row_id) for row_id, _ in items}
        for row_id, record in items:
            target._commit_upsert(record, row_id, named[row_id])

    def for_each(self, fn: Callable[[StoreEntry], None]) -> None:
        """Apply ``fn`` to a stable snapshot of every row."""
        for entry in self.entries():
            fn(entry)

    def for_each_parallel(self, workers: int, fn: Callable[[StoreEntry], None]) -> None:
        """``for_each`` spread over ``workers`` threads — Python threads
        interleave I/O-bound work; CPU-bound projections stay GIL-bound."""
        if workers < 1:
            raise ValueError("workers must be >= 1")
        entries = self.entries()
        if not entries:
            return
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(fn, entries))

    def entries(self) -> List[StoreEntry]:
        """A stable snapshot of every stored row."""
        with self._lock:
            return [StoreEntry(base64.b64decode(row_id), record)
                    for row_id, record in self._rows.items()]

    def __len__(self) -> int:
        with self._lock:
            return len(self._rows)

    def values(self) -> List[Record]:
        with self._lock:
            return list(self._rows.values())

    # -- internals ------------------------------------------------------

    def _commit_upsert(self, record: Record, primary_id: str,
                       named: Dict[str, bytes]) -> None:
        with self._lock:
            self._remove_postings(primary_id)
            for key, value in named.items():
                postings = self._named_index.get(key)
                if postings is None:
                    raise ValueError(f"undeclared named key: {key}")
                postings.setdefault(self._key_id(value), set()).add(primary_id)
            self._rows[primary_id] = record

    def _change_primary(self, old_id: str) -> None:
        with self._lock:
            self._rows.pop(old_id, None)
            self._remove_postings(old_id)

    def _named_values_of(self, primary_id: str) -> Dict[str, bytes]:
        named: Dict[str, bytes] = {}
        for key, postings in self._named_index.items():
            for value_id, ids in postings.items():
                if primary_id in ids:
                    named[key] = base64.b64decode(value_id)
        return named

    def _remove_postings(self, primary_id: str) -> None:
        for postings in self._named_index.values():
            for value_id in [v for v, ids in postings.items() if primary_id in ids]:
                postings[value_id].discard(primary_id)
                if not postings[value_id]:
                    del postings[value_id]

    @staticmethod
    def _key_id(key: Union[bytes, str]) -> str:
        if isinstance(key, str):
            key = key.encode("utf-8")
        return base64.b64encode(key).decode("ascii")

    def _default_id(self, record: Record) -> str:
        key = record.key if record.key is not None else f"cursor:{record.cursor}".encode("utf-8")
        return base64.b64encode(key).decode("ascii")


class Upsert:
    """Row-level write builder returned by ``RecordStore.upsert``."""

    def __init__(self, store: RecordStore, record: Record):
        self._store = store
        self._record = record
        self._primary_id = store._default_id(record)
        self._named: Dict[str, bytes] = {}

    def with_key(self, key: Union[bytes, str, None]) -> "Upsert":
        """Choose the row's primary key."""
        row_id = self._store._key_id(key) if key is not None else self._store._default_id(self._record)
        if row_id != self._primary_id:
            self._store._change_primary(self._primary_id)
            self._primary_id = row_id
        self._store._commit_upsert(self._record, self._primary_id, self._named)
        return self

    def with_named_key(self, named_key: str, value: Union[bytes, str]) -> "Upsert":
        """Add a named-key value (must be declared via ``add_key``)."""
        self._named[named_key] = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        self._store._commit_upsert(self._record, self._primary_id, self._named)
        return self
