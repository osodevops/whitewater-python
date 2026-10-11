"""Wire codec: the only place request/response shapes are interpreted.

Binary fields are base64; signed 64-bit nanoseconds are decimal strings;
UUIDs serialize as lowercase hyphenated strings; ``metadata`` values map
to ``metadata_base64`` entries. Response field names are snake_case and
arrive verbatim — the codec only lifts them into typed models.
"""

from __future__ import annotations

import base64
import uuid
from typing import Any, Dict, List, Optional

from .models import (
    AppendResult,
    ControlExecution,
    Cursor,
    FeedAppend,
    MemberPage,
    ReaderPage,
    ReaderSession,
    Record,
    ServerFeedback,
    StatementResult,
    TemporaryFetchPage,
)


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(text: str) -> bytes:
    return base64.b64decode(text)


def append_request(
    request_id: uuid.UUID,
    writer: str,
    session_epoch: int,
    key: Optional[bytes],
    payload: bytes,
    event_time_ns: Optional[int],
    metadata: Dict[str, bytes],
) -> dict:
    body: Dict[str, Any] = {
        "request_id": str(request_id),
        "writer": writer,
        "session_epoch": session_epoch,
        "payload_base64": b64(payload),
        "metadata_base64": {k: b64(v) for k, v in metadata.items()},
    }
    if key is not None:
        body["key_base64"] = b64(key)
    if event_time_ns is not None:
        body["event_time_ns"] = str(event_time_ns)
    return body


def feedback(node: Any) -> ServerFeedback:
    node = node or {}
    return ServerFeedback(
        recommended_batch_count=node.get("recommended_batch_count", 0),
        recommended_batch_bytes=node.get("recommended_batch_bytes", 0),
        pressure=node.get("pressure", 0.0),
        retry_after_ms=node.get("retry_after_ms", 0),
        max_frame_bytes=node.get("max_frame_bytes", 0),
        hot_key=node.get("hot_key", False),
    )


def append_result(node: dict) -> AppendResult:
    return AppendResult(
        message_id=uuid.UUID(node["message_id"]),
        cursor=Cursor(node["cursor"]),
        deduplicated=bool(node.get("deduplicated", False)),
        feedback=feedback(node.get("feedback")),
    )


def record(node: dict) -> Record:
    return Record(
        cursor=Cursor(node["cursor"]),
        message_id=uuid.UUID(node["message_id"]),
        producer_id=uuid.UUID(node["producer_id"]),
        sequence=node["sequence"],
        event_time_ns=int(node["event_time_ns"]),
        ingest_time_ns=int(node["ingest_time_ns"]),
        key=unb64(node["key_base64"]) if node.get("key_base64") is not None else None,
        payload=unb64(node["payload_base64"]),
        metadata={k: unb64(v) for k, v in (node.get("metadata_base64") or {}).items()},
    )


def records(node: Any) -> List[Record]:
    return [record(item) for item in node] if isinstance(node, list) else []


def reader_session(node: dict) -> ReaderSession:
    return ReaderSession(
        reader_id=uuid.UUID(node["reader_id"]),
        feed_id=uuid.UUID(node["feed_id"]),
        session_epoch=node["session_epoch"],
        capacity=node.get("capacity", 0),
        delivered_cursor=Cursor.of(node.get("delivered_cursor")),
        acknowledged_cursor=Cursor.of(node.get("acknowledged_cursor")),
    )


def reader_page(node: dict) -> ReaderPage:
    return ReaderPage(
        records=records(node.get("records")),
        delivered_cursor=Cursor.of(node.get("delivered_cursor")),
        acknowledged_cursor=Cursor.of(node.get("acknowledged_cursor")),
        recommended_capacity=node.get("recommended_capacity", 0),
        retry_after_ms=node.get("retry_after_ms", 0),
    )


def control_execution(node: dict) -> ControlExecution:
    return ControlExecution(
        request_id=uuid.UUID(node["request_id"]),
        revision=node["revision"],
        authority=node["authority"],
        warning=node.get("warning") or "",
        results=[
            StatementResult(
                statement=item.get("statement", ""),
                message=item.get("message", ""),
                data=item.get("data") or {},
            )
            for item in node.get("results") or []
        ],
    )


def member_page(node: dict) -> MemberPage:
    positions = node.get("positions") or {}
    return MemberPage(
        records=records(node.get("records")),
        cursor=Cursor.of(node.get("cursor")),
        positions={k: str(v) for k, v in positions.items()},
        ownership_epoch=node.get("ownership_epoch", 0),
    )


def feed_append_request(feed: str, record: FeedAppend) -> dict:
    body: Dict[str, Any] = {
        "request_id": str(record.request_id),
        "feed": feed,
        "writer_session_id": str(record.writer_session_id),
        "writer_epoch": record.writer_epoch,
        "sequence": record.sequence,
        "key_base64": b64(record.key if record.key is not None else b""),
        "payload_base64": b64(record.payload),
        "metadata_base64": {k: b64(v) for k, v in record.metadata.items()},
    }
    if record.event_time_ns is not None:
        body["event_time_ns"] = str(record.event_time_ns)
    return body


def temporary_fetch_page(node: dict) -> TemporaryFetchPage:
    return TemporaryFetchPage(
        records=records(node.get("records")),
        next_cursor=Cursor.of(node.get("next_cursor")),
    )
