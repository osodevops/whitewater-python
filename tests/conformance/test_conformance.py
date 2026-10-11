"""Shared-corpus conformance runner.

Executes every scenario in ``sdk/spec/conformance/*.json`` against a
real three-Node Riverbed. Enabled when both environment variables are
set::

    WHITEWATER_ENDPOINTS=http://localhost:7071,...
    WHITEWATER_SPEC_DIR=.../whitewater/sdk/spec/conformance

``WHITEWATER_API_KEY`` defaults to the local development key. A scenario
whose ``op`` this SDK cannot express fails explicitly rather than
silently skipping — that is a spec-required signal.
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from whitewater import (
    Cursor,
    FeedAppend,
    MemberPage,
    ReaderSession,
    TemporaryFetchOptions,
    WhitewaterClient,
    WhitewaterError,
    WorkLease,
    Joined,
)

ENDPOINTS = os.environ.get("WHITEWATER_ENDPOINTS")
API_KEY = os.environ.get("WHITEWATER_API_KEY", "whitewater-local-development-admin-key")
SPEC_DIR = Path(
    os.environ.get(
        "WHITEWATER_SPEC_DIR",
        str(Path(__file__).resolve().parents[1] / "spec" / "conformance"),
    )
)

FIXTURES = sorted(SPEC_DIR.glob("*.json")) if SPEC_DIR.is_dir() else []

pytestmark = pytest.mark.skipif(
    not ENDPOINTS or not FIXTURES,
    reason="WHITEWATER_ENDPOINTS and a spec/conformance checkout are required",
)

_TOKEN = re.compile(r"\{\{(.*?)\}\}")
_WHOLE = re.compile(r"^\{\{(.*?)\}\}$")


def _jsonify(value: Any) -> Any:
    """Normalize bound results to plain JSON-shaped values."""
    if dataclasses.is_dataclass(value):
        value = dataclasses.asdict(value)
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        import base64

        return base64.b64encode(value).decode("ascii")
    return value


class Runner:
    def __init__(self, client: WhitewaterClient):
        self.client = client
        self.bound: Dict[str, Any] = {}
        self.named_uuids: Dict[str, str] = {}
        self.uniq = "u%x%x" % (time.time_ns() % 36**7, int.from_bytes(os.urandom(3), "big"))
        self.last_error: Optional[WhitewaterError] = None

    # ---- substitution ----

    def resolve_node(self, token: str) -> Any:
        if token == "uniq":
            return self.uniq
        if token == "uuid":
            return str(uuid.uuid4())
        if token.startswith("uuid:"):
            return self.named_uuids.setdefault(token[5:], str(uuid.uuid4()))
        if "." in token:
            name, path = token.split(".", 1)
            bound = self.bound.get(name)
            if bound is not None:
                value = self._path_get(bound, path)
                if value is not None:
                    return value
        raise AssertionError(f"unresolved fixture token {{{{{token}}}}}")

    def substitute(self, node: Any) -> Any:
        if isinstance(node, str):
            whole = _WHOLE.match(node)
            if whole:
                return self.resolve_node(whole.group(1))
            return _TOKEN.sub(lambda m: str(self.resolve_node(m.group(1))), node)
        if isinstance(node, dict):
            return {k: self.substitute(v) for k, v in node.items()}
        if isinstance(node, list):
            return [self.substitute(v) for v in node]
        return node

    @staticmethod
    def _path_get(node: Any, path: str):
        current = node
        for segment in path.split("."):
            match = re.match(r"([^\[]+)\[(\d+)\]$", segment)
            if match:
                current = current.get(match.group(1)) if isinstance(current, dict) else None
                if not isinstance(current, list) or int(match.group(2)) >= len(current):
                    return None
                current = current[int(match.group(2))]
            elif isinstance(current, dict):
                current = current.get(segment)
            else:
                return None
            if current is None:
                return None
        return current

    # ---- decode helpers ----

    @staticmethod
    def _bytes(node: Any) -> Optional[bytes]:
        if node is None:
            return None
        if isinstance(node, dict):
            if "utf8" in node:
                return node["utf8"].encode("utf-8")
            if "hex" in node:
                return bytes.fromhex(node["hex"])
            if "base64" in node:
                import base64

                return base64.b64decode(node["base64"])
        return str(node).encode("utf-8")

    def _metadata(self, node: Any) -> Dict[str, bytes]:
        if not isinstance(node, dict):
            return {}
        return {k: self._bytes(v) for k, v in node.items()}

    # ---- operations ----

    def execute(self, op: str, args: Dict[str, Any]) -> Any:
        client = self.client
        if op == "wcl":
            request_id = uuid.UUID(args["request_id"]) if "request_id" in args else uuid.uuid4()
            return client.admin.wcl(args["script"], request_id)
        if op == "admin.commands":
            return client.admin.commands(args["commands"])
        if op == "feed.append":
            return client.feeds.append(
                args["feed"],
                FeedAppend(
                    request_id=uuid.UUID(args["request_id"]),
                    writer_session_id=uuid.UUID(args["writer_session_id"]),
                    writer_epoch=int(args["writer_epoch"]),
                    sequence=int(args["sequence"]),
                    key=self._bytes(args.get("key")),
                    payload=self._bytes(args.get("payload")),
                    event_time_ns=int(args["event_time_ns"]) if "event_time_ns" in args else None,
                    metadata=self._metadata(args.get("metadata")),
                ),
            )
        if op == "reader.temporary_fetch":
            return client.feeds.temporary_fetch(
                args["feed"],
                TemporaryFetchOptions(
                    after=Cursor(args["after"]) if args.get("after") else None,
                    limit=args.get("limit"),
                    new_only=bool(args.get("new_only")),
                    tail=bool(args.get("tail")),
                    wait_ms=args.get("wait_ms"),
                    after_event_time_ns=int(args["after_event_time_ns"])
                    if "after_event_time_ns" in args
                    else None,
                ),
            )
        if op == "writer.append":
            writer = client.writer(args["writer"], int(args["session_epoch"]))
            return writer.append(
                self._bytes(args.get("key")),
                self._bytes(args.get("payload")),
                request_id=uuid.UUID(args["request_id"]),
                event_time_ns=int(args["event_time_ns"]) if "event_time_ns" in args else None,
                metadata=self._metadata(args.get("metadata")),
            )
        if op == "writer.append_batch":
            records = args["records"]
            writer = client.writer(records[0]["writer"], int(records[0]["session_epoch"]))
            return writer.append_batch(
                [
                    {
                        "request_id": uuid.UUID(r["request_id"]),
                        "key": self._bytes(r.get("key")),
                        "payload": self._bytes(r.get("payload")),
                        "event_time_ns": int(r["event_time_ns"]) if "event_time_ns" in r else None,
                        "metadata": self._metadata(r.get("metadata")),
                    }
                    for r in records
                ]
            )
        if op == "reader.open":
            return client.reader(args["reader"]).open(
                int(args.get("capacity", 64)), uuid.UUID(args["request_id"])
            )
        if op == "reader.fetch":
            session = ReaderSession(
                reader_id=uuid.uuid4(),
                feed_id=uuid.uuid4(),
                session_epoch=int(args["session_epoch"]),
                capacity=0,
                delivered_cursor=None,
                acknowledged_cursor=None,
            )
            return client.reader(args["reader"]).fetch(
                session, args.get("limit"), uuid.UUID(args["request_id"])
            )
        if op == "reader.ack":
            session = ReaderSession(
                reader_id=uuid.uuid4(),
                feed_id=uuid.uuid4(),
                session_epoch=int(args["session_epoch"]),
                capacity=0,
                delivered_cursor=None,
                acknowledged_cursor=None,
            )
            return client.reader(args["reader"]).ack(
                session, Cursor(args["cursor"]), uuid.UUID(args["request_id"])
            )
        if op == "reader.close":
            session = ReaderSession(
                reader_id=uuid.uuid4(),
                feed_id=uuid.uuid4(),
                session_epoch=int(args["session_epoch"]),
                capacity=0,
                delivered_cursor=None,
                acknowledged_cursor=None,
            )
            return client.reader(args["reader"]).close(session)
        if op == "member.join":
            return client.subscription_member(args["subscription"]).join(uuid.UUID(args["member_id"]))
        if op == "member.claim":
            member = self._joined(args)
            lease = client.subscription_member(args["subscription"]).claim(
                member, uuid.UUID(args["work_id"]), args.get("lease_ticks")
            )
            return {"lease": lease}
        if op == "member.renew":
            member = self._joined(args)
            lease = client.subscription_member(args["subscription"]).renew(
                member, self._lease(args), args.get("lease_ticks")
            )
            return {"lease": lease}
        if op == "member.release":
            member = self._joined(args)
            client.subscription_member(args["subscription"]).release(member, self._lease(args))
            return {}
        if op == "member.fetch":
            member = self._joined(args)
            return client.subscription_member(args["subscription"]).fetch(
                member, self._lease(args), args.get("limit")
            )
        if op == "member.ack":
            member = self._joined(args)
            page = MemberPage(
                records=[],
                cursor=Cursor(args["cursor"]) if args.get("cursor") else None,
                positions={k: str(v) for k, v in (args.get("positions") or {}).items()},
                ownership_epoch=0,
            )
            client.subscription_member(args["subscription"]).ack(member, self._lease(args), page)
            return {}
        if op == "feed.records":
            path = "/v1/feeds/records?feed=" + args["feed"]
            if "limit" in args:
                path += "&limit=" + str(args["limit"])
            return {"records": self.client._transport.get(path)}
        raise AssertionError(f"unsupported operation in scenario: {op}")

    def _joined(self, args: Dict[str, Any]) -> Joined:
        return Joined(
            member_id=uuid.UUID(args["member_id"]),
            member_epoch=int(args["member_epoch"]),
            ownership_epoch=0,
            tick=0,
        )

    def _lease(self, args: Dict[str, Any]) -> WorkLease:
        member = self._joined(args)
        return WorkLease(
            work_id=uuid.UUID(args["work_id"]),
            member_id=member.member_id,
            member_epoch=member.member_epoch,
            lease_epoch=int(args["lease_epoch"]),
            expires_at_tick=0,
        )

    # ---- expectations ----

    def check_expect(self, expect: Dict[str, Any], result: Any):
        if "error_code" in expect or "error_status" in expect:
            assert self.last_error is not None, (
                f"expected error {expect} but the step succeeded"
            )
            if "error_code" in expect:
                assert self.last_error.code == expect["error_code"]
            if "error_status" in expect:
                assert self.last_error.status == expect["error_status"]
            return
        assert self.last_error is None, f"step failed: {self.last_error}"
        for path, expected in expect.items():
            actual = self._path_get(result, path)
            self._check(path, actual, expected)

    def _check(self, path: str, actual: Any, expected: Any):
        if isinstance(expected, str):
            if expected == "present":
                assert actual is not None, f"{path} expected present"
                return
            if expected == "absent":
                assert actual is None, f"{path} expected absent but was {actual}"
                return
            if expected == "!=null":
                assert actual is not None, f"{path} expected non-null"
                return
            if expected.startswith("len>="):
                minimum = int(expected[5:])
                length = len(actual) if actual is not None else -1
                assert length >= minimum, f"{path} expected len>={minimum} but was {actual}"
                return
            if expected.startswith("=="):
                reference = expected[2:]
                name, ref_path = reference.split(".", 1)
                expected = self._path_get(self.bound[name], ref_path)
            elif "{{" in expected:
                expected = self.substitute(expected)
        # i64 nanoseconds may arrive as decimal strings or JSON numbers.
        if isinstance(expected, str) and isinstance(actual, (int, float)):
            try:
                expected = int(expected)
            except ValueError:
                pass
        elif isinstance(expected, (int, float)) and isinstance(actual, str):
            try:
                actual = int(actual)
            except ValueError:
                pass
        assert actual == expected, f"{path} expected {expected!r} but was {actual!r}"

    # ---- driver ----

    def run(self, fixture_path: Path):
        scenario = json.loads(fixture_path.read_text())
        for index, step in enumerate(scenario["steps"]):
            args = self.substitute(step.get("args") or {})
            self.last_error = None
            result: Any = None
            try:
                result = self.execute(step["op"], args)
            except WhitewaterError as error:
                self.last_error = error
            if self.last_error is not None and "expect" not in step:
                # An unhandled step failure is a scenario failure —
                # surface it rather than letting downstream tokens
                # resolve against a null binding and fail confusingly.
                raise AssertionError(
                    f"step {index} ({step['op']}) failed: {self.last_error}"
                )
            if "bind" in step:
                self.bound[step["bind"]] = _jsonify(result)
            if "expect" in step:
                try:
                    self.check_expect(step["expect"], _jsonify(result))
                except AssertionError as error:
                    raise AssertionError(f"step {index} ({step['op']}): {error}")


@pytest.mark.parametrize(
    "fixture", FIXTURES, ids=[f.stem for f in FIXTURES]
)
def test_conformance(fixture: Path):
    scenario = json.loads(fixture.read_text())
    client = WhitewaterClient(
        api_key=scenario.get("api_key", API_KEY),
        endpoints=[e.strip() for e in ENDPOINTS.split(",")],
    )
    try:
        assert client.admin.healthy(), "Riverbed must be healthy for conformance"
        Runner(client).run(fixture)
    finally:
        client.close()
