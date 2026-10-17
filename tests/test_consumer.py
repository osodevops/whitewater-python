"""``feed.consumer(...)`` runs the permanent supervised consumer — the
durable-Reader equivalent of Kafka's poll loop with the plumbing
internal — and the StateStore ``find``/``find_by``/``get_or_default``
helpers strip row wrappers for value-only lookups. These drive a
scripted multi-path HTTP stub that models ack-gated redelivery.
"""

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from whitewater import Refused, WhitewaterClient, WhitewaterError

SESSION = {
    "reader_id": "00000000-0000-7000-8000-000000000004",
    "feed_id": "00000000-0000-7000-8000-000000000005",
    "session_epoch": 3,
    "capacity": 128,
    "delivered_cursor": None,
    "acknowledged_cursor": None,
}

WCL_OK = {
    "request_id": "00000000-0000-7000-8000-000000000006",
    "revision": 9,
    "authority": "control_plane",
    "results": [{"statement": "s", "message": "ok", "data": {}}],
}

FRESHNESS = {
    "applied_seq": 7,
    "committed_seq": 7,
    "behind": False,
    "served_by": "storage-1",
}


def record(cursor: str, payload: bytes) -> dict:
    return {
        "cursor": cursor,
        "message_id": "00000000-0000-7000-8000-000000000002",
        "producer_id": "00000000-0000-7000-8000-000000000003",
        "sequence": 7,
        "event_time_ns": "1697040000123456789",
        "ingest_time_ns": "1697040000223456789",
        "key_base64": base64.b64encode(f"k-{cursor}".encode()).decode(),
        "payload_base64": base64.b64encode(payload).decode(),
        "metadata_base64": {},
    }


def page(delivered: str | None, *records: dict) -> dict:
    return {
        "records": list(records),
        "delivered_cursor": delivered,
        "acknowledged_cursor": None,
        "recommended_capacity": 0,
        "retry_after_ms": 0,
    }


@pytest.fixture()
def stub():
    """The stub holds ``pending``: a page stays delivered until an ack
    lands — fetches redeliver it, exactly like the server does."""
    state = {
        "calls": [],
        "pending": None,
        "queued_pages": [],
        "open_error": None,
        "opens": 0,
        "wcl_status": 200,
        "wcl_body": WCL_OK,
        "store_bodies": {},
    }
    lock = threading.Lock()
    delivered = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            raw = self.rfile.read(int(self.headers.get("content-length", 0)))
            body = json.loads(raw) if raw else {}
            with lock:
                state["calls"].append((self.path, body))
                if self.path == "/v1/admin/wcl":
                    status = state["wcl_status"]
                    response = state["wcl_body"]
                elif self.path == "/v1/readers/open":
                    state["opens"] += 1
                    if state["open_error"] is not None:
                        status, response = state["open_error"]
                    else:
                        status, response = 200, SESSION
                elif self.path == "/v1/readers/fetch":
                    status = 200
                    if state["pending"] is not None:
                        response = state["pending"]      # unacked: redeliver
                    elif state["queued_pages"]:
                        state["pending"] = state["queued_pages"].pop(0)
                        response = state["pending"]
                    else:
                        response = page(None)
                    if response.get("records"):
                        delivered.set()
                elif self.path == "/v1/readers/ack":
                    state["pending"] = None
                    status, response = 200, SESSION
                elif self.path == "/v1/readers/close":
                    status, response = 200, SESSION
                else:
                    status = 200
                    response = state["store_bodies"].get(self.path, {})
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
    state["delivered"] = delivered
    yield state, f"http://127.0.0.1:{httpd.server_port}", lock
    httpd.shutdown()


def calls(state, path):
    return [body for call_path, body in state["calls"] if call_path == path]


def test_consumer_run_delivers_each_record_and_acks_the_page(stub):
    state, url, _lock = stub
    state["queued_pages"] = [
        page("c2", record("c1", b"alpha"), record("c2", b"beta")),
    ]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    feed = client.feed("users.update.request")
    seen = []
    consumer = feed.consumer("enricher")
    thread = consumer.run_async(seen.append)

    assert state["delivered"].wait(5)
    deadline = time.time() + 5
    while len(seen) < 2 and time.time() < deadline:
        time.sleep(0.01)
    consumer.close()
    thread.join(5)

    assert [m.value for m in seen] == [b"alpha", b"beta"]
    assert [m.cursor for m in seen] == ["c1", "c2"]
    acks = calls(state, "/v1/readers/ack")
    assert len(acks) == 1 and acks[0]["cursor"] == "c2"
    assert calls(state, "/v1/readers/close")


def test_consumer_iterates_records_and_break_closes_cleanly(stub):
    state, url, _lock = stub
    state["queued_pages"] = [
        page("c1", record("c1", b"alpha")),
        page("c2", record("c2", b"beta")),
    ]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    consumer = client.feed("users.update.request").consumer("enricher")

    seen = []
    for msg in consumer:
        seen.append(msg.value)
        if len(seen) == 2:
            break

    assert seen == [b"alpha", b"beta"]
    assert len(calls(state, "/v1/readers/ack")) == 2
    assert calls(state, "/v1/readers/close")
    assert not consumer.is_running


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_handler_failure_skips_the_ack_and_the_page_redelivers(stub):
    state, url, _lock = stub
    state["queued_pages"] = [page("c1", record("c1", b"alpha"))]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    feed = client.feed("users.update.request")
    errors = []
    attempts = [0]

    def flaky(_msg):
        attempts[0] += 1
        if attempts[0] == 1:
            raise RuntimeError("transient handler bug")
        consumer.close()

    consumer = feed.consumer("enricher").backoff(0.01, 0.05)
    consumer.on_error(errors.append).run(flaky)

    assert attempts[0] >= 2                  # redelivered after the failure
    acks = calls(state, "/v1/readers/ack")
    assert len(acks) == 1                    # only the successful replay acks
    assert state["opens"] >= 2               # supervised restart reopened
    assert any(isinstance(e, RuntimeError) for e in errors)


def test_stale_session_reopens_and_resumes(stub):
    state, url, _lock = stub
    # First open gets fenced immediately: the session dies, the
    # supervisor re-opens and the next page flows.
    stale = {"error": "fenced", "code": "stale_epoch"}
    state["open_error"] = (409, stale)
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    feed = client.feed("users.update.request")

    def flip_open():
        while state["opens"] == 0:
            time.sleep(0.01)
        state["open_error"] = None
        state["queued_pages"] = [page("c1", record("c1", b"alpha"))]

    threading.Thread(target=flip_open, daemon=True).start()
    seen = []
    consumer = feed.consumer("enricher").backoff(0.01, 0.05)
    thread = consumer.run_async(seen.append)
    assert state["delivered"].wait(5)
    consumer.close()
    thread.join(5)

    assert [m.value for m in seen] == [b"alpha"]
    assert state["opens"] >= 2


def test_startup_refusal_fails_fast(stub):
    state, url, _lock = stub
    state["wcl_status"] = 400
    state["wcl_body"] = {
        "error": "refused: denied", "code": "refused"}
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    consumer = client.feed("users.update.request").consumer("enricher")

    with pytest.raises(Refused):
        consumer.run(lambda _m: None)
    assert state["opens"] == 0               # never reached the session


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_invalid_reader_name_fails_fast_not_loops(stub):
    state, url, _lock = stub
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    consumer = client.feed("users.update.request").consumer("Bad Name!")

    with pytest.raises(WhitewaterError):
        consumer.run(lambda _m: None)
    assert state["opens"] == 0
    assert not state["calls"]                # died on local validation


def test_iteration_propagates_startup_refusal(stub):
    state, url, _lock = stub
    state["wcl_status"] = 400
    state["wcl_body"] = {
        "error": "refused: denied", "code": "refused"}
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    consumer = client.feed("users.update.request").consumer("enricher")

    with pytest.raises(Refused):
        next(iter(consumer))


def test_store_find_find_by_and_get_or_default(stub):
    state, url, _lock = stub
    state["store_bodies"]["/v1/state-stores/lookup"] = {
        "found": True,
        "payload_base64": base64.b64encode(b'"alice"').decode(),
        "applied_cursor": "00000000000000000003",
        **FRESHNESS,
    }
    state["store_bodies"]["/v1/state-stores/get-by"] = {
        "rows": [
            {
                "key_base64": base64.b64encode(b"a@x.io").decode(),
                "payload_base64": base64.b64encode(b'"alice"').decode(),
                "applied_cursor": "00000000000000000003",
            }
        ],
        **FRESHNESS,
    }
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    import whitewater.codecs as codecs

    store = client.state_store(
        "users.profiles", value_codec=codecs.json_of(str))

    assert store.find("a@x.io") == "alice"
    assert store.find_by("user_id", "u-1") == "alice"
    get_by = calls(state, "/v1/state-stores/get-by")[0]
    assert get_by["named_key"] == "user_id" and get_by["limit"] == 1

    # Misses return None / the default — no row unwrapping in app code.
    state["store_bodies"]["/v1/state-stores/lookup"] = {
        "found": False, "applied_seq": 7, "committed_seq": 7}
    state["store_bodies"]["/v1/state-stores/get-by"] = {
        "rows": [], **FRESHNESS}
    assert store.find("ghost") is None
    assert store.find_by("user_id", "ghost") is None
    assert store.get_or_default("ghost", 0) == 0
