import uuid
from unittest.mock import Mock

from whitewater.client import FeedClient, WriterSessionClient
from whitewater.models import FeedAppend


RESULT = {"message_id": str(uuid.uuid4()), "cursor": "opaque", "deduplicated": False}


def test_feed_compression_is_opt_in_and_can_be_toggled_without_changing_payload():
    transport = Mock()
    transport.post.return_value = RESULT
    original = FeedClient(transport)
    configured = original
    record = FeedAppend(uuid.uuid4(), uuid.uuid4(), 1, 1, b"key", b"\x00\xff")
    for enabled in (False, True, False, True):
        configured = configured.use_compression(enabled)
        configured.append("orders.events", record)
        path, body = transport.post.call_args.args
        assert path == "/v1/feeds/append"
        assert body.get("use_compression", False) is enabled
        assert body["payload_base64"] == "AP8="
        assert body["request_id"] == str(record.request_id)
    original.append("orders.events", record)
    assert "use_compression" not in transport.post.call_args.args[1]


def test_writer_compression_applies_to_single_and_batch_appends_and_can_be_disabled():
    transport = Mock()
    writer = WriterSessionClient(transport, "orders.writer", 7)
    request_id = uuid.uuid4()
    for enabled in (False, True, False):
        writer = writer.use_compression(enabled)
        transport.post.return_value = RESULT
        writer.append(b"key", b"\x00\xff", request_id=request_id)
        body = transport.post.call_args.args[1]
        assert body.get("use_compression", False) is enabled
        assert body["payload_base64"] == "AP8="
        transport.post.return_value = {"results": [RESULT], "feedback": {}}
        writer.append_batch([{"request_id": request_id, "key": b"key", "payload": b"\x00\xff"}])
        body = transport.post.call_args.args[1]["records"][0]
        assert body.get("use_compression", False) is enabled
        assert body["session_epoch"] == 7
        assert body["request_id"] == str(request_id)
        assert body["payload_base64"] == "AP8="
