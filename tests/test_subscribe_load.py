"""``client.subscribe(...)`` runs a managed Reader-session loop and
``client.feeds.load(...)`` pages committed history into a sink. These
tests drive both against a scripted multi-path HTTP stub.
"""

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from whitewater import ReaderStart, RecordStore, StaleSession, WhitewaterClient

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


def record(cursor: str, key: bytes) -> dict:
    return {
        "cursor": cursor,
        "message_id": "00000000-0000-7000-8000-000000000002",
        "producer_id": "00000000-0000-7000-8000-000000000003",
        "sequence": 7,
        "event_time_ns": "1697040000123456789",
        "ingest_time_ns": "1697040000223456789",
        "key_base64": base64.b64encode(key).decode(),
        "payload_base64": "dmFs",
        "metadata_base64": {},
    }


def reader_page(delivered: str | None, *cursors: str) -> dict:
    return {
        "records": [record(c, f"k-{c}".encode()) for c in cursors],
        "delivered_cursor": delivered,
        "acknowledged_cursor": None,
        "recommended_capacity": 0,
        "retry_after_ms": 0,
    }


def temporary_page(next_cursor: str | None, *cursors: str) -> dict:
    return {
        "records": [record(c, f"k-{c}".encode()) for c in cursors],
        "next_cursor": next_cursor,
    }


@pytest.fixture()
def stub():
    state = {
        "calls": [],
        "fetch_pages": [],
        "temporary_pages": [],
        "wcl_status": 200,
        "wcl_body": WCL_OK,
        "open_status": 200,
        "open_body": SESSION,
    }
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            with lock:
                state["calls"].append((self.path, body.decode()))
                if self.path == "/v1/admin/wcl":
                    status, response = state["wcl_status"], state["wcl_body"]
                elif self.path == "/v1/readers/open":
                    status, response = state["open_status"], state["open_body"]
                elif self.path in ("/v1/readers/ack", "/v1/readers/close"):
                    status, response = 200, SESSION
                elif self.path == "/v1/readers/fetch":
                    status = 200
                    response = (
                        state["fetch_pages"].pop(0)
                        if state["fetch_pages"]
                        else reader_page(None)
                    )
                elif self.path == "/v1/readers/temporary/fetch":
                    status = 200
                    response = (
                        state["temporary_pages"].pop(0)
                        if state["temporary_pages"]
                        else temporary_page(None)
                    )
                else:
                    status, response = 200, {}
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


def test_load_pages_until_empty_and_fills_the_store(stub):
    state, url, _lock = stub
    state["temporary_pages"] = [
        temporary_page("c1", "r1"),
        temporary_page("c2", "r2"),
    ]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])

    store = RecordStore.named("employee-store")
    loaded = client.feeds.load("hr.employees", ReaderStart.beginning(), store)

    assert loaded == 2
    assert len(store) == 2
    assert store.get(b"k-r1").cursor == "r1"
    calls = bodies(state, "/v1/readers/temporary/fetch")
    assert len(calls) == 3
    assert "after" not in calls[0]
    assert calls[1]["after"] == "c1"


def test_load_seeks_by_event_time_then_continues_on_cursor(stub):
    state, url, _lock = stub
    state["temporary_pages"] = [temporary_page("c1", "r1")]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])

    pages = []
    loaded = client.feeds.load(
        "hr.employees",
        ReaderStart.timestamp_ns(1697040000000000000),
        pages.append,
    )

    assert loaded == 1
    calls = bodies(state, "/v1/readers/temporary/fetch")
    assert calls[0]["after_event_time_ns"] == "1697040000000000000"
    assert "after" not in calls[0]
    assert calls[1]["after"] == "c1"


def test_subscribe_opens_delivers_acks_and_closes(stub):
    state, url, _lock = stub
    state["fetch_pages"] = [reader_page("d1", "r1")]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])

    delivered = threading.Event()
    batches = []
    sub = client.subscribe(
        "hr-view",
        "hr.employees",
        ReaderStart.beginning(),
        lambda records: (batches.append(records), delivered.set()),
    )
    assert delivered.wait(5), "handler never received the page"
    # The ack posts after the handler returns — wait for it before close.
    deadline = time.monotonic() + 5
    while not any(p == "/v1/readers/ack" for p, _ in state["calls"]):
        assert time.monotonic() < deadline, "delivered cursor never acked"
        time.sleep(0.025)
    sub.close()

    assert len(batches[0]) == 1
    wcl_calls = bodies(state, "/v1/admin/wcl")
    assert len(wcl_calls) == 1, "reader auto-provisioned once"
    assert "CREATE READER hr-view FROM hr.employees" in wcl_calls[0]["script"]
    acks = bodies(state, "/v1/readers/ack")
    assert acks[0]["cursor"] == "d1"
    assert any(p == "/v1/readers/close" for p, _ in state["calls"])
    assert not sub.running


def test_subscribe_reuses_existing_reader_and_reports_stale_session(stub):
    state, url, _lock = stub
    state["wcl_status"] = 400
    state["wcl_body"] = {"error": "Reader hr-view already exists"}
    state["open_status"] = 400
    state["open_body"] = {"error": "stale session epoch", "code": "stale_epoch"}
    client = WhitewaterClient(api_key="test-key", endpoints=[url])

    errors = []
    failed = threading.Event()
    sub = client.subscribe(
        "hr-view",
        "hr.employees",
        ReaderStart.now(),
        lambda records: None,
        lambda error: (errors.append(error), failed.set()),
    )
    assert failed.wait(5), "stale session never surfaced"
    assert isinstance(errors[0], StaleSession)
    assert isinstance(sub.failure, StaleSession)
    sub.close()


def test_named_keys_index_rows_non_uniquely_and_delete_cleans_postings(stub):
    state, url, _lock = stub
    state["temporary_pages"] = [
        temporary_page("c2", "r1", "r2"),
        temporary_page(None),
    ]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    store = RecordStore.named("employee-store").add_key("department")

    loaded = client.load_store(store).from_feed(
        "hr.employees",
        ReaderStart.beginning(),
        lambda s, record: s.upsert(record)
        .with_key(record.key)
        .with_named_key("department", "eng"),
    )

    assert loaded == 2
    assert len(store.get_by("department", "eng")) == 2
    assert store.get("k-r1").cursor == "r1"
    with pytest.raises(ValueError, match="undeclared named key"):
        store.get_by("undeclared", "x")

    assert store.delete("k-r1")
    assert len(store.get_by("department", "eng")) == 1

    store.clear()
    assert len(store) == 0
    assert store.get_by("department", "eng") == []


def test_copy_carries_named_postings_and_for_each_parallel_visits_every_row(stub):
    state, url, _lock = stub
    state["temporary_pages"] = [
        temporary_page("c2", "r1", "r2"),
        temporary_page(None),
    ]
    client = WhitewaterClient(api_key="test-key", endpoints=[url])
    store = RecordStore.named("src").add_key("team")
    client.load_store(store).from_feed(
        "hr.employees",
        ReaderStart.beginning(),
        lambda s, record: s.upsert(record)
        .with_key(record.key)
        .with_named_key("team", str(record.cursor)),
    )

    target = RecordStore.named("dst")
    store.copy(target)
    assert len(target) == 2
    assert target.named_keys == ["team"]
    assert len(target.get_by("team", "r1")) == 1

    seen = []
    store.for_each_parallel(3, lambda entry: (
        seen.append(entry),
        entry.primary_key and entry.record.cursor,
    ))
    assert len(seen) == 2
