"""Pipe declaration: the typed ``define_pipe`` builders serialize to the
exact command contract on ``POST /v1/admin/commands``, and bad specs —
wrong operation kind, non-positive windows, missing extractors — fail
locally before any HTTP call.
"""

import json
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from whitewater import (
    EnrichFreshness,
    EnrichMiss,
    Extractor,
    PipeOperation,
    WhitewaterClient,
    WindowAggregation,
    WindowTime,
)

COMMANDS_OK = {
    "request_id": "00000000-0000-7000-8000-000000000006",
    "revision": 9,
    "authority": "control_plane",
    "results": [{"statement": "s", "message": "ok", "data": {}}],
}


@pytest.fixture()
def stub():
    state = {"calls": []}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            with lock:
                state["calls"].append((self.path, body.decode()))
            payload = json.dumps(COMMANDS_OK).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield state, f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()


@pytest.fixture()
def client(stub):
    _state, url = stub
    return WhitewaterClient(api_key="test-key", endpoints=[url])


def commands(state):
    return [
        json.loads(body)["commands"][0]
        for path, body in state["calls"]
        if path == "/v1/admin/commands"
    ]


# ---- Extractor (IndexFieldSpec) -----------------------------------------------


def test_extractor_wire_shapes():
    assert Extractor.record_key().to_wire() == {"kind": "key"}
    assert Extractor.metadata("pp").to_wire() == {
        "kind": "metadata",
        "name": "pp",
    }
    assert Extractor.json("user.id").to_wire() == {
        "kind": "projection",
        "extractor": "json",
        "version": 1,
        "field": "user.id",
    }


def test_extractor_factories_require_arguments():
    with pytest.raises(ValueError):
        Extractor.metadata("")
    with pytest.raises(ValueError):
        Extractor.json("   ")


# ---- WindowAggregation --------------------------------------------------------


def test_window_aggregation_wire_shapes():
    assert WindowAggregation.count().to_wire() == {"aggregation": "count"}
    value = {"kind": "projection", "extractor": "json", "version": 1,
             "field": "amount"}
    assert WindowAggregation.sum(Extractor.json("amount")).to_wire() == {
        "aggregation": "sum",
        "value": value,
    }
    assert WindowAggregation.average(Extractor.json("amount")).to_wire() == {
        "aggregation": "average",
        "value": value,
    }


def test_window_aggregation_value_extractors_are_required():
    with pytest.raises(TypeError):
        WindowAggregation.sum(None)
    with pytest.raises(TypeError):
        WindowAggregation.average("amount")


# ---- PipeOperation ------------------------------------------------------------


def test_forward_operation_wire_shape():
    assert PipeOperation.forward().to_wire() == {"operation": "forward"}


def test_enrich_operation_wire_shape_with_defaults():
    operation = PipeOperation.enrich(Extractor.json("user_id"))
    assert operation.to_wire() == {
        "operation": "enrich",
        "key": {"kind": "projection", "extractor": "json", "version": 1,
                "field": "user_id"},
        "freshness": "latest",
        "on_miss": "forward",
    }
    assert "named" not in operation.to_wire()


def test_enrich_operation_wire_shape_explicit():
    operation = PipeOperation.enrich(
        Extractor.record_key(),
        freshness=EnrichFreshness.STRICT,
        named="profile",
        on_miss=EnrichMiss.QUARANTINE,
    )
    assert operation.to_wire() == {
        "operation": "enrich",
        "key": {"kind": "key"},
        "freshness": "strict",
        "named": "profile",
        "on_miss": "quarantine",
    }


def test_enrich_operation_accepts_wire_strings():
    operation = PipeOperation.enrich(
        Extractor.metadata("pp"), freshness="strict", on_miss="drop")
    assert operation.to_wire()["freshness"] == "strict"
    assert operation.to_wire()["on_miss"] == "drop"


def test_enrich_operation_requires_a_key_extractor():
    with pytest.raises(TypeError):
        PipeOperation.enrich(None)
    with pytest.raises(ValueError):
        PipeOperation.enrich(Extractor.record_key(), freshness="stale")
    with pytest.raises(ValueError):
        PipeOperation.enrich(Extractor.record_key(), on_miss="bounce")


def test_window_operation_wire_shape_minimal():
    operation = PipeOperation.window(
        timedelta(minutes=1),
        Extractor.record_key(),
        WindowAggregation.count(),
    )
    assert operation.to_wire() == {
        "operation": "window",
        "key": {"kind": "key"},
        "window_ns": 60_000_000_000,
        "aggregation": {"aggregation": "count"},
        "time": "event",
    }


def test_window_operation_wire_shape_with_every_bound():
    operation = PipeOperation.window(
        timedelta(seconds=5),
        Extractor.json("tenant"),
        WindowAggregation.sum(Extractor.json("amount")),
        time=WindowTime.INGEST,
        max_late=timedelta(seconds=1),
        expire=timedelta(seconds=4),
        key_limit=1000,
        byte_limit=1 << 20,
    )
    assert operation.to_wire() == {
        "operation": "window",
        "key": {"kind": "projection", "extractor": "json", "version": 1,
                "field": "tenant"},
        "window_ns": 5_000_000_000,
        "aggregation": {
            "aggregation": "sum",
            "value": {"kind": "projection", "extractor": "json",
                      "version": 1, "field": "amount"},
        },
        "time": "ingest",
        "max_late_ns": 1_000_000_000,
        "expire_ns": 4_000_000_000,
        "key_limit": 1000,
        "byte_limit": 1 << 20,
    }


def test_window_operation_wire_shape_watermark_and_grace():
    operation = PipeOperation.window(
        timedelta(minutes=1),
        Extractor.record_key(),
        WindowAggregation.count(),
        time=WindowTime.EVENT,
        watermark=timedelta(milliseconds=2),
        grace=timedelta(milliseconds=3),
    )
    assert operation.to_wire() == {
        "operation": "window",
        "key": {"kind": "key"},
        "window_ns": 60_000_000_000,
        "aggregation": {"aggregation": "count"},
        "time": "event",
        "watermark_ns": 2_000_000,
        "grace_ns": 3_000_000,
    }


def test_window_operation_validates_locally(client, stub):
    state, _url = stub
    with pytest.raises(ValueError):
        PipeOperation.window(timedelta(0), Extractor.record_key(),
                             WindowAggregation.count())
    with pytest.raises(ValueError):
        PipeOperation.window(timedelta(seconds=-1), Extractor.record_key(),
                             WindowAggregation.count())
    with pytest.raises(ValueError):
        PipeOperation.window(timedelta(minutes=1), Extractor.record_key(),
                             WindowAggregation.count(),
                             max_late=timedelta(0))
    with pytest.raises(TypeError):
        PipeOperation.window(1.5, Extractor.record_key(),
                             WindowAggregation.count())
    with pytest.raises(TypeError):
        PipeOperation.window(True, Extractor.record_key(),
                             WindowAggregation.count())
    with pytest.raises(TypeError):
        PipeOperation.window(60, Extractor.record_key(),
                             WindowAggregation.count())
    with pytest.raises(TypeError):
        PipeOperation.window(timedelta(seconds=60), None,
                             WindowAggregation.count())
    with pytest.raises(TypeError):
        PipeOperation.window(timedelta(seconds=60), Extractor.record_key(),
                             None)
    with pytest.raises(TypeError):
        PipeOperation.window(timedelta(seconds=60), Extractor.record_key(),
                             WindowAggregation.count(), watermark=60)
    with pytest.raises(ValueError):
        PipeOperation.window(timedelta(seconds=60), Extractor.record_key(),
                             WindowAggregation.count(), time="crayon")
    assert state["calls"] == []


def test_window_operation_validates_lateness_models():
    # MAX_LATE and WATERMARK are alternative lateness models.
    with pytest.raises(ValueError):
        PipeOperation.window(
            timedelta(minutes=1), Extractor.record_key(),
            WindowAggregation.count(), time=WindowTime.EVENT,
            max_late=timedelta(seconds=10),
            watermark=timedelta(seconds=5))
    # WATERMARK follows the event clock only.
    with pytest.raises(ValueError):
        PipeOperation.window(
            timedelta(minutes=1), Extractor.record_key(),
            WindowAggregation.count(), time=WindowTime.INGEST,
            watermark=timedelta(seconds=5))
    # GRACE requires WATERMARK — revision bounds ride the horizon.
    with pytest.raises(ValueError):
        PipeOperation.window(
            timedelta(minutes=1), Extractor.record_key(),
            WindowAggregation.count(), time=WindowTime.EVENT,
            grace=timedelta(seconds=30))


# ---- define_pipe commands ------------------------------------------------------


def test_define_forward_pipe_emits_the_copy_pipe(client, stub):
    state, _url = stub
    client.define_forward_pipe("ops.relay", "relay.sub", "ops.archive")
    assert commands(state) == [
        {
            "command": "define_pipe",
            "name": "ops.relay",
            "subscription": "relay.sub",
            "output_feed": "ops.archive",
            "operation": {"operation": "forward"},
        }
    ]


def test_define_enrich_pipe_emits_feed_store_and_operation(client, stub):
    state, _url = stub
    client.define_enrich_pipe(
        "ops.enrich",
        "enrich.sub",
        "ops.enriched",
        "ops.profiles",
        PipeOperation.enrich(
            Extractor.json("user_id"), on_miss=EnrichMiss.QUARANTINE),
        quarantine_feed="ops.misses",
    )
    assert commands(state) == [
        {
            "command": "define_pipe",
            "name": "ops.enrich",
            "subscription": "enrich.sub",
            "output_feed": "ops.enriched",
            "output_store": "ops.profiles",
            "quarantine_feed": "ops.misses",
            "operation": {
                "operation": "enrich",
                "key": {"kind": "projection", "extractor": "json",
                        "version": 1, "field": "user_id"},
                "freshness": "latest",
                "on_miss": "quarantine",
            },
        }
    ]


def test_define_enrich_pipe_omits_absent_quarantine(client, stub):
    state, _url = stub
    client.define_enrich_pipe(
        "ops.enrich", "enrich.sub", "ops.enriched", "ops.profiles",
        PipeOperation.enrich(Extractor.record_key()),
    )
    command = commands(state)[0]
    assert "quarantine_feed" not in command
    assert command["operation"]["on_miss"] == "forward"


def test_define_enrich_pipe_refuses_other_operations(client, stub):
    state, _url = stub
    with pytest.raises(ValueError):
        client.define_enrich_pipe(
            "ops.enrich", "sub", "ops.out", "ops.profiles",
            PipeOperation.window(timedelta(seconds=60),
                                 Extractor.record_key(),
                                 WindowAggregation.count()))
    with pytest.raises(ValueError):
        client.define_enrich_pipe(
            "ops.enrich", "sub", "ops.out", "ops.profiles",
            PipeOperation.forward())
    with pytest.raises(TypeError):
        client.define_enrich_pipe(
            "ops.enrich", "sub", "ops.out", "ops.profiles",
            {"operation": "enrich"})
    assert state["calls"] == []


def test_define_window_pipe_emits_store_and_operation(client, stub):
    state, _url = stub
    client.define_window_pipe(
        "ops.rollups",
        "window.sub",
        "ops.minute.totals",
        PipeOperation.window(
            timedelta(minutes=1),
            Extractor.record_key(),
            WindowAggregation.sum(Extractor.json("amount")),
            max_late=timedelta(seconds=5),
        ),
        emit_feed="ops.panes",
    )
    assert commands(state) == [
        {
            "command": "define_pipe",
            "name": "ops.rollups",
            "subscription": "window.sub",
            "output_store": "ops.minute.totals",
            "emit_feed": "ops.panes",
            "operation": {
                "operation": "window",
                "key": {"kind": "key"},
                "window_ns": 60_000_000_000,
                "aggregation": {
                    "aggregation": "sum",
                    "value": {"kind": "projection", "extractor": "json",
                              "version": 1, "field": "amount"},
                },
                "time": "event",
                "max_late_ns": 5_000_000_000,
            },
        }
    ]


def test_define_window_pipe_omits_absent_emit_feed(client, stub):
    state, _url = stub
    client.define_window_pipe(
        "ops.rollups", "window.sub", "ops.minute.totals",
        PipeOperation.window(timedelta(seconds=60),
                             Extractor.record_key(),
                             WindowAggregation.count()))
    command = commands(state)[0]
    assert "emit_feed" not in command


def test_define_window_pipe_refuses_other_operations(client, stub):
    state, _url = stub
    with pytest.raises(ValueError):
        client.define_window_pipe(
            "ops.rollups", "sub", "ops.store",
            PipeOperation.enrich(Extractor.record_key()))
    with pytest.raises(TypeError):
        client.define_window_pipe(
            "ops.rollups", "sub", "ops.store", "window")
    assert state["calls"] == []


def test_define_pipe_names_validate_before_any_call(client, stub):
    state, _url = stub
    with pytest.raises(ValueError):
        client.define_forward_pipe("Bad Name", "sub", "ops.out")
    with pytest.raises(ValueError):
        client.define_forward_pipe("ops.relay", "bad sub!", "ops.out")
    with pytest.raises(ValueError):
        client.define_forward_pipe("ops.relay", "sub", "ops.Out")
    assert state["calls"] == []
