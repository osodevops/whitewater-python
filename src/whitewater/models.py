"""Whitewater wire-domain models.

``Cursor`` is an opaque :class:`str` subclass: the SDK never parses,
orders, or fabricates cursors — they pass back to the Riverbed verbatim.
``event_time_ns``/``ingest_time_ns`` are signed 64-bit nanoseconds; on
the wire they travel as decimal strings (JSON numbers lose i64 range).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class Cursor(str):
    """An opaque stream position. Never parse or order these."""

    @staticmethod
    def of(value: Optional[str]) -> Optional["Cursor"]:
        return None if value is None else Cursor(value)


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
