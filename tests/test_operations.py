"""Pipe lifecycle, Index queries, and demand observability — the
operational surfaces parity-tested against scripted wire shapes."""

import base64
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from whitewater import (
    IndexBound,
    IndexPredicate,
    WhitewaterClient,
)

PIPE_ROW = {
    "name": "orders.relay",
    "stage": "paused",
    "subscription_id": "019a0000-0000-7000-8000-000000000020",
    "operation": {"operation": "forward"},
    "output_feed_id": "019a0000-0000-7000-8000-000000000021",
    "output_store_id": None,
    "status": "active",
    "created_at_ns": 42,
}


def commands_with(data):
    return {
        "request_id": "019a0000-0000-7000-8000-000000000006",
        "revision": 9,
        "authority": "control_plane",
        "results": [{"statement": "s", "message": "ok", "data": data}],
    }


@pytest.fixture()
def stub():
    state = {"paths": [], "bodies": []}
    lock = threading.Lock()

    def route(path: str, body: str):
        if path == "/v1/admin/commands":
            payload = json.loads(body)
            command = payload["commands"][0].get("command")
            if command == "describe":
                return 200, commands_with(PIPE_ROW)
            if command == "show":
                return 200, commands_with({"rows": [PIPE_ROW]})
            return 200, commands_with({})
        if path == "/v1/admin/demand/feeds":
            return 200, {
                "feeds": [{
                    "feed": "orders.events", "domain": "orders",
                    "appended_records": 1500, "appended_bytes": 2048000,
                    "read_pages": 42,
                }],
                "domains": {"orders": {"feeds": 1, "appended_records": 1500}},
            }
        if path == "/v1/admin/demand/hot-keys":
            return 200, {
                "hot_keys": [{
                    "feed": "orders.events", "share_bps": 9000,
                    "window_records": 900, "window_bytes": 12000,
                }],
            }
        if path.startswith("/v1/admin/demand/"):
            kind = path.rsplit("/", 1)[-1]
            return 200, {kind: [], "domains": {}}
        if path == "/v1/indexes/query":
            return 200, {
                "index": "crm.byregion",
                "index_id": "019a0000-0000-7000-8000-000000000030",
                "generation": 2,
                "rows": [{
                    "key_base64": base64.b64encode(b"acct-1").decode(),
                    "payload_base64": base64.b64encode(b'{"tier":"gold"}').decode(),
                }],
                "applied_cursor": "idx-cursor",
                "target_cursor": "tip-cursor",
                "behind": False, "truncated": False,
                "served_by": "node-1",
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
    state["port"] = server.server_address[1]
    yield state
    server.shutdown()
    thread.join(timeout=5)


@pytest.fixture()
def client(stub):
    return WhitewaterClient(
        endpoints=[f"http://127.0.0.1:{stub['port']}"],
        api_key="test-key",
    )


# ---- Pipe lifecycle ----------------------------------------------------------


def test_pipe_describe_decodes_the_catalog_row(client, stub):
    info = client.pipe("orders.relay").describe()
    assert info.name == "orders.relay"
    assert info.stage == "paused"
    assert info.subscription_id == uuid.UUID(
        "019a0000-0000-7000-8000-000000000020")
    assert info.operation["operation"] == "forward"
    assert info.output_feed_id == uuid.UUID(
        "019a0000-0000-7000-8000-000000000021")
    assert info.output_store_id is None
    assert info.status == "active"
    assert info.created_at_ns == 42
    command = json.loads(stub["bodies"][0])
    assert command["commands"][0]["command"] == "describe"
    assert command["commands"][0]["kind"] == "pipe"


def test_pipe_lifecycle_posts_the_typed_verbs(client, stub):
    pipe = client.pipe("orders.relay")
    pipe.pause()
    pipe.resume()
    pipe.drop()
    verbs = [json.loads(b)["commands"][0]["command"] for b in stub["bodies"]]
    assert verbs == ["pause_pipe", "resume_pipe", "drop"]
    assert json.loads(stub["bodies"][2])["commands"][0]["kind"] == "pipe"


def test_pipes_lists_the_catalog(client, stub):
    pipes = client.pipes()
    assert len(pipes) == 1
    assert pipes[0].name == "orders.relay"
    assert pipes[0].stage == "paused"
    command = json.loads(stub["bodies"][0])
    assert command["commands"][0] == {"command": "show", "kind": "pipes"}


# ---- Demand observability ----------------------------------------------------


def test_demand_feeds_decodes_rows_and_domain_rollups(client, stub):
    report = client.demand_feeds()
    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.name == "orders.events"
    assert row.domain == "orders"
    assert row.counter("appended_records") == 1500
    assert row.counter("read_pages") == 42
    assert row.counter("nonsense") is None
    assert report.domain("orders")["feeds"] == 1
    assert stub["paths"][0] == "/v1/admin/demand/feeds"


def test_demand_endpoints_cover_every_resource_kind(client, stub):
    assert client.demand_readers().rows == []
    assert client.demand_subscriptions().rows == []
    assert client.demand_indexes().rows == []
    assert client.demand_pipes().rows == []
    assert stub["paths"][:4] == [
        "/v1/admin/demand/readers",
        "/v1/admin/demand/subscriptions",
        "/v1/admin/demand/indexes",
        "/v1/admin/demand/pipes",
    ]


def test_hot_keys_decode_dominant_key_evidence(client, stub):
    keys = client.hot_keys()
    assert len(keys) == 1
    assert keys[0].feed == "orders.events"
    assert keys[0].share_bps == 9000
    assert keys[0].window_records == 900
    assert keys[0].window_bytes == 12000
    assert stub["paths"][0] == "/v1/admin/demand/hot-keys"


# ---- Index queries ------------------------------------------------------------


def test_query_index_range_flattens_bounds_and_decodes_rows(client, stub):
    page = client.query_index(
        "crm.byregion",
        IndexPredicate.range(
            ["emea"],
            IndexBound.inclusive_bound(0),
            IndexBound.exclusive_bound(99)),
        limit=50,
        strict=True,
    )
    assert page.behind is False
    assert page.generation == 2
    assert len(page.rows) == 1
    assert page.rows[0].key == b"acct-1"
    assert page.served_by == "node-1"
    body = json.loads(stub["bodies"][0])
    assert body["mode"] == "range"
    assert body["leading"] == [{"kind": "text", "value": "emea"}]
    assert body["upper"]["inclusive"] is False
    assert body["limit"] == 50
    assert body["strict"] is True
    assert stub["paths"][0] == "/v1/indexes/query"


def test_query_index_exact_carries_every_typed_kind(client, stub):
    client.query_index(
        "crm.byregion",
        IndexPredicate.exact(["emea", 42, True, None, b"\x01\x02"]),
    )
    values = json.loads(stub["bodies"][0])["values"]
    assert values == [
        {"kind": "text", "value": "emea"},
        {"kind": "i64", "value": 42},
        {"kind": "bool", "value": True},
        {"kind": "null"},
        {"kind": "bytes", "value_base64": base64.b64encode(b"\x01\x02").decode()},
    ]


def test_query_index_prefix_omits_partial_text_when_absent(client, stub):
    client.query_index("crm.byregion", IndexPredicate.prefix(["emea"]))
    body = json.loads(stub["bodies"][0])
    assert body["mode"] == "prefix"
    assert "partial_text" not in body

    client.query_index("crm.byregion",
                       IndexPredicate.prefix(["emea"], "gold"))
    body = json.loads(stub["bodies"][1])
    assert body["partial_text"] == "gold"


def test_index_predicate_rejects_unsupported_value_types(client):
    with pytest.raises(TypeError):
        client.query_index("crm.byregion",
                           IndexPredicate.exact([object()]))
