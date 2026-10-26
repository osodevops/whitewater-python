"""``client.feed(...)`` binds codecs once so keys/payloads serialize
automatically in both directions — these tests drive the typed Feed,
FeedWriter, FeedReader, subscribe, and codec-bound StateStore paths
against a scripted HTTP stub and assert exact wire shapes."""

import base64
import json
import threading
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from whitewater import (
    Cursor,
    FeedEvent,
    ReaderStart,
    TemporaryFetchOptions,
    WhitewaterClient,
    codecs,
)


@dataclass
class Order:
    id: str
    total: int


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def record(cursor: str, key: str, payload: str) -> dict:
    return {
        "cursor": cursor,
        "message_id": "019a0000-0000-7000-8000-000000000002",
        "producer_id": "019a0000-0000-7000-8000-000000000003",
        "sequence": 7,
        "event_time_ns": "1697040000123456789",
        "ingest_time_ns": "1697040000223456789",
        "key_base64": b64(key.encode()),
        "payload_base64": b64(payload.encode()),
        "metadata_base64": {"tenant": b64(b"acme")},
    }


def order_payload(order_id: str, total: int) -> str:
    return json.dumps({"id": order_id, "total": total}, separators=(",", ":"))


COMMANDS_OK = {
    "request_id": "019a0000-0000-7000-8000-000000000006",
    "revision": 9,
    "authority": "control_plane",
    "results": [{"statement": "s", "message": "ok", "data": {}}],
}

SESSION_OK = {
    "request_id": "019a0000-0000-7000-8000-000000000006",
    "revision": 9,
    "authority": "control_plane",
    "results": [{"statement": "s", "message": "ok", "data": {"session_epoch": 42}}],
}

READER_SESSION = {
    "reader_id": "019a0000-0000-7000-8000-000000000010",
    "feed_id": "019a0000-0000-7000-8000-000000000011",
    "session_epoch": 7,
    "capacity": 64,
    "delivered_cursor": None,
    "acknowledged_cursor": None,
}

APPEND_OK = {
    "message_id": "019a0000-0000-7000-8000-0000000000aa",
    "cursor": "appended-1",
    "deduplicated": False,
    "durability": "committed",
}


@pytest.fixture()
def stub():
    state = {
        "paths": [],
        "bodies": [],
        "refuse_creates": False,
        "fetch_calls": 0,
    }
    lock = threading.Lock()

    def route(path: str, body: str):
        if path == "/v1/admin/wcl":
            if state["refuse_creates"] and (
                "CREATE WRITER" in body or "CREATE READER" in body
            ):
                return 400, {"error": "resource already exists", "code": "conflict"}
            return 200, COMMANDS_OK
        if path == "/v1/admin/commands":
            if "open_writer_session" in body:
                return 200, SESSION_OK
            return 200, COMMANDS_OK
        if path == "/v1/feeds/records":
            return 200, [
                record("c1", "order-1", order_payload("order-1", 10)),
                record("c2", "order-2", order_payload("order-2", 20)),
            ]
        if path == "/v1/readers/temporary/fetch":
            return 200, {
                "records": [record("rec-1", "order-1", order_payload("order-1", 10))],
                "next_cursor": "frontier-9",
            }
        if path in ("/v1/readers/open", "/v1/readers/ack", "/v1/readers/close"):
            return 200, READER_SESSION
        if path == "/v1/readers/fetch":
            state["fetch_calls"] += 1
            if state["fetch_calls"] == 1:
                return 200, {
                    "records": [record("rec-1", "order-1", order_payload("order-1", 10))],
                    "delivered_cursor": "d1",
                    "acknowledged_cursor": None,
                    "recommended_capacity": 64,
                    "retry_after_ms": 0,
                }
            return 200, {
                "records": [],
                "delivered_cursor": "d1",
                "acknowledged_cursor": "d1",
                "recommended_capacity": 64,
                "retry_after_ms": 0,
            }
        if path == "/v1/writers/append":
            return 200, APPEND_OK
        if path == "/v1/writers/append-batch":
            return 200, {"results": [APPEND_OK, APPEND_OK], "feedback": {}}
        if path == "/v1/feeds/append":
            return 200, dict(APPEND_OK, deduplicated=True)
        if path == "/v1/state-stores/mutate":
            return 200, {
                "store_id": "019a0000-0000-7000-8000-000000000007",
                "mutation_id": "019a0000-0000-7000-8000-000000000008",
                "sequence": 3,
                "deduplicated": False,
            }
        return 200, {}

    class Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("content-length", 0))
            body = self.rfile.read(length).decode() if length else ""
            with lock:
                state["paths"].append(self.path.split("?")[0])
                state["bodies"].append(body)
                status, payload = route(self.path.split("?")[0], body)
            out = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

        do_POST = _handle  # noqa: N802
        do_GET = _handle  # noqa: N802

        def log_message(self, *_args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state["port"] = server.server_port
    yield state
    server.shutdown()


@pytest.fixture()
def client(stub):
    return WhitewaterClient(
        api_key="test-key",
        endpoints=[f"http://127.0.0.1:{stub['port']}"],
    )


def last_body(state, path):
    for p, b in zip(reversed(state["paths"]), reversed(state["bodies"])):
        if p == path:
            return json.loads(b)
    raise AssertionError(f"no request to {path}")


def test_default_binding_is_utf8_keys_and_payloads(stub, client):
    feed = client.feed("orders.events")
    assert feed.name == "orders.events"
    assert feed.key_codec.decode(b"k") == "k"
    assert feed.value_codec.decode(b"v") == "v"


def test_bytes_payloads_bind_through_an_explicit_codec(stub, client):
    feed = client.feed("orders.events", value_type=bytes)
    assert feed.value_codec.decode(b"v") == b"v"


def test_records_decode_through_bound_codecs(stub, client):
    orders = client.feed("orders.events", value_type=Order)
    page = orders.records(limit=50, after=Cursor.of("prev"))
    assert len(page) == 2
    assert page[0].key == "order-1"
    assert page[0].value.total == 10
    assert page[1].value.id == "order-2"
    assert page[0].metadata_text("tenant") == "acme"
    assert page[0].event_time_ns == 1697040000123456789


def test_fetch_returns_decoded_page_with_next_cursor(stub, client):
    page = client.feed("orders.events", value_type=Order).fetch(
        TemporaryFetchOptions.page(Cursor.of("after-1"), 100)
    )
    assert str(page.next_cursor) == "frontier-9"
    assert page.records[0].value.total == 10


def test_view_rebinds_codecs_without_a_wire_call(stub, client):
    typed = client.feed("orders.events", value_type=Order)
    raw = typed.view(codecs.utf8(), codecs.raw())
    assert raw.name == "orders.events"
    page = raw.records(limit=10)
    assert page[0].value == order_payload("order-1", 10).encode()
    calls = len(stub["paths"])
    typed.view_types(str, str)
    assert len(stub["paths"]) == calls


def test_writer_declares_opens_and_serializes(stub, client):
    writer = client.feed("orders.events", value_type=Order).writer("orders.ingest")
    assert any("CREATE WRITER" in b for b in stub["bodies"]), "writer was declared"
    assert any("open_writer_session" in b for b in stub["bodies"])

    writer.append("order-1", Order("order-1", 99))
    sent = last_body(stub, "/v1/writers/append")
    assert sent["writer"] == "orders.ingest"
    assert sent["session_epoch"] == 42
    assert base64.b64decode(sent["key_base64"]) == b"order-1"
    wire = base64.b64decode(sent["payload_base64"]).decode()
    assert json.loads(wire) == {"id": "order-1", "total": 99}


def test_writer_tolerates_already_declared(stub, client):
    stub["refuse_creates"] = True
    writer = client.feed("orders.events", value_type=Order).writer("orders.ingest")
    assert writer.session_epoch == 42


def test_append_event_carries_identity_time_and_metadata(stub, client):
    writer = client.feed("orders.events", value_type=Order).writer("orders.ingest")
    rid = uuid.UUID("019a0000-0000-7000-8000-0000000000cc")
    writer.append_event(
        FeedEvent("order-7", Order("order-7", 7))
        .with_request_id(rid)
        .with_event_time_ns(1697040000123456789)
        .with_text_metadata({"tenant": "acme"})
    )
    sent = last_body(stub, "/v1/writers/append")
    assert sent["request_id"] == str(rid)
    assert sent["event_time_ns"] == "1697040000123456789"
    assert base64.b64decode(sent["metadata_base64"]["tenant"]) == b"acme"


def test_append_all_serializes_every_event(stub, client):
    writer = client.feed("orders.events", value_type=Order).writer("orders.ingest")
    result = writer.append_all([
        FeedEvent("o1", Order("o1", 1)),
        FeedEvent("o2", Order("o2", 2)),
    ])
    assert len(result.results) == 2
    sent = last_body(stub, "/v1/writers/append-batch")
    assert len(sent["records"]) == 2
    second = json.loads(base64.b64decode(sent["records"][1]["payload_base64"]))
    assert second == {"id": "o2", "total": 2}


def test_ad_hoc_append_serializes_through_facade(stub, client):
    session = uuid.UUID("019a0000-0000-7000-8000-0000000000bb")
    result = client.feed("orders.events", value_type=Order).append(
        session, 3, 9, "order-1", Order("order-1", 5)
    )
    sent = last_body(stub, "/v1/feeds/append")
    assert sent["writer_session_id"] == str(session)
    wire = json.loads(base64.b64decode(sent["payload_base64"]))
    assert wire == {"id": "order-1", "total": 5}
    assert result.deduplicated


def test_reader_declares_opens_and_decodes(stub, client):
    with client.feed("orders.events", value_type=Order).reader("orders.audit", 64) as reader:
        assert any("CREATE READER" in b for b in stub["bodies"]), "reader declared"
        page = reader.fetch()
        assert len(page.records) == 1
        assert page.records[0].key == "order-1"
        assert page.records[0].value.total == 10
        assert str(page.delivered_cursor) == "d1"
        reader.ack()
        ack = last_body(stub, "/v1/readers/ack")
        assert ack["cursor"] == "d1"
        assert ack["session_epoch"] == 7
    assert "/v1/readers/close" in stub["paths"]


def test_reader_tolerates_already_declared(stub, client):
    stub["refuse_creates"] = True
    with client.feed("orders.events", value_type=Order).reader("orders.audit", 64) as reader:
        assert reader.session_epoch == 7


def test_subscribe_delivers_decoded_batches_and_acks(stub, client):
    delivered = threading.Event()
    seen = {}

    def on_records(batch):
        seen["msg"] = batch[0]
        delivered.set()

    feed = client.feed("orders.events", value_type=Order)
    with feed.subscribe("orders.sub", ReaderStart.beginning(), on_records):
        assert delivered.wait(5), "subscription delivered"
        assert seen["msg"].key == "order-1"
        assert seen["msg"].value.total == 10
        import time
        deadline = time.time() + 5
        while "/v1/readers/ack" not in stub["paths"] and time.time() < deadline:
            time.sleep(0.02)
        assert "/v1/readers/ack" in stub["paths"], "progress committed"
    assert "/v1/readers/close" in stub["paths"]


def test_null_key_and_payload_decode_as_none():
    from whitewater.models import Record
    from whitewater.feed import FeedMessage

    message = FeedMessage.decode(
        Record(
            cursor=Cursor.of("c"),
            message_id=None,
            producer_id=None,
            sequence=0,
            event_time_ns=0,
            ingest_time_ns=0,
            key=None,
            payload=None,
            metadata={},
        ),
        codecs.utf8(),
        codecs.utf8(),
    )
    assert message.key is None and message.value is None


def test_custom_codec_applies_both_directions(stub, client):
    reversed_codec = codecs.of(
        lambda s: s[::-1].encode(),
        lambda b: b.decode()[::-1],
    )
    feed = client.feed("orders.events", value_codec=reversed_codec)
    page = feed.records(limit=10)
    assert page[0].value == order_payload("order-1", 10)[::-1]


def test_state_store_serializes_through_codecs(stub, client):
    store = client.state_store("app.users", value_type=Order)
    store.put("u1", Order("u1", 3))
    sent = last_body(stub, "/v1/state-stores/mutate")
    op = sent["op"]
    assert base64.b64decode(op["key_base64"]) == b"u1"
    assert json.loads(base64.b64decode(op["payload_base64"])) == {"id": "u1", "total": 3}
