"""Wire-shape unit tests: golden request encodings and response decodes."""

import uuid

from whitewater import Cursor
from whitewater import _wire


def test_append_request_scalars_on_wire():
    request_id = uuid.UUID("019a0000-0000-7000-8000-000000000001")
    body = _wire.append_request(
        request_id,
        "orders.wr",
        7,
        b"key-1",
        b"payload",
        -(2**63),
        {"source": b"unit"},
    )
    assert body["request_id"] == str(request_id)
    assert body["writer"] == "orders.wr"
    assert body["session_epoch"] == 7
    assert body["key_base64"] == "a2V5LTE="
    assert body["payload_base64"] == "cGF5bG9hZA=="
    # i64 nanoseconds serialize as a decimal string, never a JSON number
    assert body["event_time_ns"] == "-9223372036854775808"
    assert body["metadata_base64"] == {"source": "dW5pdA=="}


def test_record_decodes_opaque_cursor_and_nanosecond_strings():
    node = {
        "cursor": "AAAABBBBCCCCDDDD",
        "message_id": "019a0000-0000-7000-8000-000000000002",
        "producer_id": "019a0000-0000-7000-8000-000000000003",
        "sequence": 42,
        "event_time_ns": "9223372036854775807",
        "ingest_time_ns": "-5",
        "key_base64": "3q2+7w==",
        "payload_base64": "AAEC",
        "metadata_base64": {"m": "eA=="},
    }
    record = _wire.record(node)
    assert record.cursor == "AAAABBBBCCCCDDDD"
    assert isinstance(record.cursor, str)
    assert record.event_time_ns == 2**63 - 1
    assert record.ingest_time_ns == -5
    assert record.key == b"\xde\xad\xbe\xef"
    assert record.payload == b"\x00\x01\x02"
    assert record.metadata == {"m": b"x"}


def test_cursor_is_an_opaque_string():
    opaque = "TSLWCDHsmGBMXtKGSBS3vfBmPrsJnt1cYxWFYi4TpTA"
    cursor = Cursor(opaque)
    assert str(cursor) == opaque
    assert cursor == opaque  # plain str equality, never parsed
    assert Cursor.of(None) is None


def test_feedback_decodes_hot_key_flag_with_default():
    flagged = _wire.feedback(
        {
            "recommended_batch_count": 8,
            "recommended_batch_bytes": 65536,
            "pressure": 0.9,
            "retry_after_ms": 50,
            "max_frame_bytes": 67108864,
            "hot_key": True,
        }
    )
    assert flagged.hot_key is True
    # Older servers omit the field; default is False.
    cold = _wire.feedback({"recommended_batch_count": 100})
    assert cold.hot_key is False
