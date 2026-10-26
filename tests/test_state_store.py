"""``client.state_store(...)`` is a thin facade over the node-side
StateStore: these tests drive every operation against a scripted HTTP
stub and assert the exact wire shapes — declare commands, typed named
values, journal mutations, lookups, iteration pages, clear, and drop.
"""

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from whitewater import Extractor, StateStore, WhitewaterClient

COMMANDS_OK = {
    "request_id": "00000000-0000-7000-8000-000000000006",
    "revision": 9,
    "authority": "control_plane",
    "results": [{"statement": "s", "message": "ok", "data": {}}],
}

MUTATION = {
    "store_id": "00000000-0000-7000-8000-000000000007",
    "mutation_id": "00000000-0000-7000-8000-000000000008",
    "sequence": 3,
    "deduplicated": False,
}

FRESHNESS = {
    "applied_seq": 7,
    "committed_seq": 7,
    "behind": False,
    "served_by": "storage-1",
}


def row(key: bytes, payload: bytes, cursor: str = "00000000000000000003") -> dict:
    return {
        "key_base64": base64.b64encode(key).decode(),
        "payload_base64": base64.b64encode(payload).decode(),
        "applied_cursor": cursor,
    }


@pytest.fixture()
def stub():
    state = {"calls": [], "pages": {}, "bodies": {}}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            with lock:
                state["calls"].append((self.path, body.decode()))
                queued = state["pages"].get(self.path)
                if queued:
                    status, response = queued.pop(0)
                else:
                    status = 200
                    response = state["bodies"].get(self.path, {})
            payload = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield state, f"http://127.0.0.1:{httpd.server_port}", lock
    httpd.shutdown()


def bodies(state, path):
    return [
        json.loads(body)
        for call_path, body in state["calls"]
        if call_path == path
    ]


def test_define_emits_named_keys_and_binds_the_facade(stub):
    state, url, _lock = stub
    state["bodies"]["/v1/admin/commands"] = COMMANDS_OK
    client = WhitewaterClient(api_key="test-key", endpoints=[url])

    spec = (
        StateStore.named("accounts.employees")
        .add_key("passport", unique=True)
        .add_key("department")
    )
    store = client.state_stores.define(spec)

    commands = bodies(state, "/v1/admin/commands")
    assert commands == [
        {
            "commands": [
                {
                    "command": "define_state_store",
                    "name": "accounts.employees",
                    "source": {"kind": "manual"},
                    "named_keys": [
                        {"name": "passport", "unique": True},
                        {"name": "department", "unique": False},
                    ],
                }
            ]
        }
    ]
    # The defined spec came back bound — data ops can run on it.
    assert store._bound() is not None


def test_define_feed_sourced_named_keys_need_extractors(stub):
    _state, url, _lock = stub
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    # A feed-sourced named key without a projection is a declaration bug.
    with pytest.raises(ValueError, match="require an extractor"):
        client.state_stores.define(
            StateStore.named("accounts.s").add_key("k"),
            source="accounts.events",
        )
    # An extractor on a manual store is refused — puts carry the values.
    with pytest.raises(ValueError, match="manual store"):
        client.state_stores.define(
            StateStore.named("accounts.m").add_key(
                "k", extractor=Extractor.metadata("pp")
            )
        )


def test_define_feed_sourced_emits_extractors(stub):
    state, url, _lock = stub
    state["bodies"]["/v1/admin/commands"] = COMMANDS_OK
    client = WhitewaterClient(api_key="test-key", endpoints=[url])

    spec = (
        StateStore.named("accounts.tagged")
        .add_key("passport", unique=True, extractor=Extractor.metadata("pp"))
        .add_key("department", extractor=Extractor.json("dept"))
    )
    store = client.state_stores.define(spec, source="accounts.events")

    commands = bodies(state, "/v1/admin/commands")
    assert commands == [
        {
            "commands": [
                {
                    "command": "define_state_store",
                    "name": "accounts.tagged",
                    "source": {"kind": "feed", "feed": "accounts.events"},
                    "named_keys": [
                        {
                            "name": "passport",
                            "unique": True,
                            "extractor": {"kind": "metadata", "name": "pp"},
                        },
                        {
                            "name": "department",
                            "unique": False,
                            "extractor": {
                                "kind": "projection",
                                "extractor": "json",
                                "version": 1,
                                "field": "dept",
                            },
                        },
                    ],
                }
            ]
        }
    ]
    assert store._bound() is not None


def test_spec_validation_and_unbound_refusal(stub):
    _state, _url, _lock = stub
    with pytest.raises(ValueError, match="duplicate named key"):
        StateStore.named("s.dup").add_key("k").add_key("k")
    with pytest.raises(ValueError, match="unbound StateStore spec"):
        StateStore.named("s.unbound").get(b"k")


def test_put_get_get_by_delete_carry_wire_shapes(stub):
    state, url, _lock = stub
    state["bodies"]["/v1/state-stores/mutate"] = MUTATION
    state["bodies"]["/v1/state-stores/lookup"] = {
        "found": True,
        "payload_base64": base64.b64encode(b"profile-1").decode(),
        "applied_cursor": "00000000000000000003",
        **FRESHNESS,
    }
    state["bodies"]["/v1/state-stores/get-by"] = {
        "rows": [row(b"user-1", b"profile-1"), row(b"user-2", b"profile-2")],
        **FRESHNESS,
    }
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    store = client.state_store("accounts.employees")

    result = store.put(
        "user-1",
        "profile-1",
        named={"passport": "P111", "department": "eng", "score": 42},
    )
    assert result.sequence == 3 and not result.deduplicated
    mutate = bodies(state, "/v1/state-stores/mutate")[0]
    op = mutate["op"]
    assert op["kind"] == "put_row"
    assert base64.b64decode(op["key_base64"]) == b"user-1"
    named = {entry["name"]: entry["values"] for entry in op["named"]}
    assert named["passport"] == [{"kind": "text", "value": "P111"}]
    assert named["score"] == [{"kind": "i64", "value": 42}]

    found = store.get("user-1", strict=True, wait_ms=250)
    assert found is not None and found.payload == "profile-1"
    assert found.freshness.applied_seq == 7 and not found.freshness.behind
    lookup = bodies(state, "/v1/state-stores/lookup")[0]
    assert lookup["strict"] is True and lookup["wait_ms"] == 250

    rows = store.get_by("department", "eng")
    assert [r.key for r in rows.rows] == ["user-1", "user-2"]
    assert rows.freshness.served_by == "storage-1"
    get_by = bodies(state, "/v1/state-stores/get-by")[0]
    assert get_by["named_key"] == "department"
    assert get_by["values"] == [{"kind": "text", "value": "eng"}]

    deleted = store.delete("user-1")
    assert deleted.sequence == 3
    op = bodies(state, "/v1/state-stores/mutate")[1]["op"]
    assert op["kind"] == "delete_row"

    missing = {"found": False, "applied_seq": 7, "committed_seq": 7}
    state["pages"]["/v1/state-stores/lookup"] = [(200, missing)]
    assert store.get("ghost") is None


def test_iterate_pages_until_exhausted_then_clear_and_drop(stub):
    state, url, _lock = stub
    state["pages"]["/v1/state-stores/iterate"] = [
        (
            200,
            {
                "rows": [row(b"a", b"1")],
                "next_key_base64": base64.b64encode(b"a").decode(),
                "pinned_seq": 9,
                "served_by": "storage-2",
            },
        ),
        (
            200,
            {
                "rows": [row(b"b", b"2")],
                "next_key_base64": None,
                "pinned_seq": 9,
                "served_by": "storage-2",
            },
        ),
    ]
    state["bodies"]["/v1/state-stores/clear"] = MUTATION
    state["bodies"]["/v1/admin/commands"] = COMMANDS_OK
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    store = client.state_store("accounts.employees")

    keys = [row.key for row in store.iterate(limit=100)]
    assert keys == ["a", "b"]
    calls = bodies(state, "/v1/state-stores/iterate")
    assert calls[0]["limit"] == 100 and "after_key_base64" not in calls[0]
    assert calls[1]["after_key_base64"] == base64.b64encode(b"a").decode()

    cleared = store.clear()
    assert cleared.store_id is not None
    drop = store.drop()
    assert drop.results[0].message == "ok"
    assert bodies(state, "/v1/admin/commands")[0]["commands"][0] == {
        "command": "drop",
        "kind": "state_store",
        "name": "accounts.employees",
    }
