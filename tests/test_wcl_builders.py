"""The typed resource methods on ``WhitewaterClient`` are WCL builders:
each must serialize to exactly one statement on ``POST /v1/admin/wcl``,
and invalid names must fail locally before any HTTP call.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from whitewater import Cursor, ReaderStart, WhitewaterClient


@pytest.fixture()
def server():
    captured = {"path": "", "body": ""}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            captured["path"] = self.path
            captured["body"] = self.rfile.read(
                int(self.headers.get("content-length", 0))
            ).decode()
            payload = json.dumps(
                {
                    "request_id": "00000000-0000-0000-0000-000000000000",
                    "revision": 1,
                    "authority": "control_plane",
                    "results": [],
                }
            ).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield captured, f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()


@pytest.fixture()
def client(server):
    captured, url = server
    return WhitewaterClient(api_key="test-key", endpoints=[url]), captured


def script_of(captured):
    assert captured["path"] == "/v1/admin/wcl"
    return json.loads(captured["body"])["script"]


def test_creates(client):
    c, captured = client
    c.create_domain("orders")
    assert script_of(captured) == "CREATE DOMAIN orders"
    c.create_feed("orders.events")
    assert script_of(captured) == "CREATE FEED orders.events"
    c.create_writer("checkout", "orders.events")
    assert script_of(captured) == "CREATE WRITER checkout TO orders.events"
    c.create_role("ops")
    assert script_of(captured) == "CREATE ROLE ops"


def test_readers_and_subscriptions(client):
    c, captured = client
    c.create_reader("audit", "orders.events")
    assert (
        script_of(captured)
        == "CREATE READER audit FROM orders.events START AT BEGINNING"
    )
    c.create_reader("live", "orders.events", ReaderStart.now())
    assert script_of(captured) == "CREATE READER live FROM orders.events START AT NOW"
    c.create_reader(
        "rewind", "orders.events", ReaderStart.timestamp_ns(1700000000000000000)
    )
    assert script_of(captured) == (
        "CREATE READER rewind FROM orders.events "
        "START AT TIMESTAMP 1700000000000000000"
    )
    c.create_reader("resumed", "orders.events", ReaderStart.cursor(Cursor("abc'def")))
    assert script_of(captured) == (
        "CREATE READER resumed FROM orders.events START AT CURSOR 'abc''def'"
    )
    c.create_subscription("team", "orders.events", ReaderStart.now())
    assert (
        script_of(captured)
        == "CREATE SUBSCRIPTION team FROM orders.events START AT NOW"
    )


def test_sessions_and_seek(client):
    c, captured = client
    c.open_writer_session("checkout")
    assert script_of(captured) == "OPEN WRITER SESSION checkout"
    c.seek_reader("audit", ReaderStart.now())
    assert script_of(captured) == "SEEK READER audit TO NOW"


def test_describe_rename_drop_show(client):
    c, captured = client
    c.describe_feed("orders.events")
    assert script_of(captured) == "DESCRIBE FEED orders.events"
    c.describe_subscription("team")
    assert script_of(captured) == "DESCRIBE SUBSCRIPTION team"
    c.rename_domain("orders", "billing")
    assert script_of(captured) == "RENAME DOMAIN orders TO billing"
    c.rename_feed("orders.events", "orders.accepted")
    assert script_of(captured) == "RENAME FEED orders.events TO orders.accepted"
    c.drop_writer("checkout")
    assert script_of(captured) == "DROP WRITER checkout"
    c.list_feeds()
    assert script_of(captured) == "SHOW FEEDS"
    c.list_domains()
    assert script_of(captured) == "SHOW DOMAINS"


def test_invalid_names_fail_locally(client):
    c, captured = client
    with pytest.raises(ValueError):
        c.create_feed("UPPER.case")
    with pytest.raises(ValueError):
        c.create_feed(".leading")
    with pytest.raises(ValueError):
        c.create_domain("two words")
    with pytest.raises(ValueError):
        c.create_writer("bad;name", "orders.events")
    with pytest.raises(ValueError):
        c.create_reader("a'b", "orders.events")
    with pytest.raises(ValueError):
        c.rename_domain("orders", "INJECTION; DROP")
    assert captured["path"] == ""
