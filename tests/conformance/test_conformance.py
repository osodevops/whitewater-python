"""Shared-corpus conformance runner.

Executes every scenario in ``sdk/spec/conformance/*.json`` against a
real three-Node Riverbed. Enabled when both environment variables are
set::

    WHITEWATER_ENDPOINTS=http://localhost:7071,...
    WHITEWATER_SPEC_DIR=.../whitewater/sdk/spec/conformance

``WHITEWATER_API_KEY`` defaults to the local development key.
``WHITEWATER_CHAOS_CMD`` names the environment's fault driver; without it
``chaos.*`` steps report unsupported rather than skipping. A scenario
whose ``op`` this SDK cannot express fails explicitly rather than
silently skipping — that is a spec-required signal.

Steps dispatch at the wire surface so `expect` asserts the same
(status, body) contract every language runner implements: poll windows
re-dispatch asynchronous steps until expectations settle, while a
separate transient budget rides out transport errors and 5xx
unavailability without spending poll attempts.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import httpx
import pytest

ENDPOINTS = os.environ.get("WHITEWATER_ENDPOINTS")
API_KEY = os.environ.get("WHITEWATER_API_KEY", "whitewater-local-development-admin-key")
SPEC_DIR = Path(
    os.environ.get(
        "WHITEWATER_SPEC_DIR",
        str(Path(__file__).resolve().parents[1] / "spec" / "conformance"),
    )
)
CHAOS_CMD = os.environ.get("WHITEWATER_CHAOS_CMD", "")

FIXTURES = sorted(SPEC_DIR.glob("*.json")) if SPEC_DIR.is_dir() else []

pytestmark = pytest.mark.skipif(
    not ENDPOINTS or not FIXTURES,
    reason="WHITEWATER_ENDPOINTS and a spec/conformance checkout are required",
)

_TOKEN = re.compile(r"\{\{(.*?)\}\}")
_WHOLE = re.compile(r"^\{\{(.*?)\}\}$")

# The shared op vocabulary onto the wire surface; fixtures assert wire
# names directly.
_ENDPOINTS: Dict[str, Tuple[str, str]] = {
    "wcl": ("POST", "/v1/admin/wcl"),
    "admin.wcl": ("POST", "/v1/admin/wcl"),
    "admin.commands": ("POST", "/v1/admin/commands"),
    "writer.append": ("POST", "/v1/writers/append"),
    "writer.append_batch": ("POST", "/v1/writers/append-batch"),
    "reader.open": ("POST", "/v1/readers/open"),
    "reader.fetch": ("POST", "/v1/readers/fetch"),
    "reader.ack": ("POST", "/v1/readers/ack"),
    "reader.close": ("POST", "/v1/readers/close"),
    "reader.temporary_fetch": ("POST", "/v1/readers/temporary/fetch"),
    "feed.append": ("POST", "/v1/feeds/append"),
    "member.join": ("POST", "/v1/subscriptions/members/join"),
    "member.claim": ("POST", "/v1/subscriptions/members/claim"),
    "member.renew": ("POST", "/v1/subscriptions/members/renew"),
    "member.release": ("POST", "/v1/subscriptions/members/release"),
    "member.transfer": ("POST", "/v1/subscriptions/members/transfer"),
    "member.fetch": ("POST", "/v1/subscriptions/members/fetch"),
    "member.ack": ("POST", "/v1/subscriptions/members/ack"),
    "subscription.seek": ("POST", "/v1/subscriptions/seek"),
    "statestore.lookup": ("POST", "/v1/state-stores/lookup"),
    "statestore.get_by": ("POST", "/v1/state-stores/get-by"),
    "statestore.iterate": ("POST", "/v1/state-stores/iterate"),
    "statestore.clear": ("POST", "/v1/state-stores/clear"),
}

TRANSIENT_ATTEMPTS = 120
TRANSIENT_INTERVAL = 0.5


class TransportError(Exception):
    """Connection/transport failure — covered by the transient budget."""


class Wire:
    """Status-preserving JSON-over-HTTP calls; non-2xx replies return the
    body so `expect` asserts error_status/error_code against it."""

    def __init__(self, endpoints, api_key: str):
        self._endpoints = [e.rstrip("/") for e in endpoints]
        self._api_key = api_key
        self._next = 0
        self._http = httpx.Client(timeout=30.0)

    def close(self):
        self._http.close()

    def request(self, method: str, path: str, body: Optional[dict]) -> Tuple[int, Any]:
        url = self._endpoints[self._next] + path
        self._next = (self._next + 1) % len(self._endpoints)
        try:
            response = self._http.request(
                method,
                url,
                json=body,
                headers={
                    "authorization": f"Bearer {self._api_key}",
                    "accept": "application/json",
                    "content-type": "application/json" if body is not None else "application/json",
                },
            )
        except httpx.HTTPError as exc:
            raise TransportError(f"Node unreachable: {exc}") from exc
        content = response.content
        try:
            parsed = json.loads(content) if content else None
        except ValueError:
            parsed = None
        return response.status_code, parsed

    def get(self, path: str) -> Tuple[int, Any]:
        return self.request("GET", path, None)

    def post(self, path: str, body: dict) -> Tuple[int, Any]:
        return self.request("POST", path, body)


def _raw_bytes(node: Any) -> bytes:
    """Decode {"utf8": ..} / {"hex": ..} / {"base64": ..} wrappers; a plain
    string is already utf8 bytes."""
    if node is None:
        return b""
    if isinstance(node, dict):
        if "utf8" in node:
            return node["utf8"].encode("utf-8")
        if "hex" in node:
            return bytes.fromhex(node["hex"])
        if "base64" in node:
            return base64.b64decode(node["base64"])
    if isinstance(node, str):
        return node.encode("utf-8")
    return b""


def _wire_fields(row: dict) -> dict:
    for raw, encoded in (("key", "key_base64"), ("payload", "payload_base64"), ("after_key", "after_key_base64")):
        if raw in row:
            row[encoded] = base64.b64encode(_raw_bytes(row.pop(raw))).decode("ascii")
    if "metadata" in row:
        metadata = row.pop("metadata")
        row["metadata_base64"] = {
            name: base64.b64encode(_raw_bytes(value)).decode("ascii")
            for name, value in (metadata or {}).items()
        }
    for field in ("event_time_ns", "after_event_time_ns", "timestamp_ns"):
        if isinstance(row.get(field), int):
            row[field] = str(row[field])
    return row


def _wire_args(args: Any) -> Any:
    """Lower fixture vocabulary onto the wire: raw objects become
    *_base64 and nanosecond numbers become decimal strings."""
    if not isinstance(args, dict):
        return args
    out = dict(args)
    if isinstance(out.get("records"), list):
        out["records"] = [
            _wire_fields(dict(record)) if isinstance(record, dict) else record
            for record in out["records"]
        ]
    return _wire_fields(out)


class Runner:
    def __init__(self, wire: Wire, chaos_cmd: str):
        self.wire = wire
        self.chaos_cmd = chaos_cmd
        self.bound: Dict[str, Any] = {}
        self.named_uuids: Dict[str, str] = {}
        self.uniq = "u%x%x" % (time.time_ns() % 36**7, int.from_bytes(os.urandom(3), "big"))

    # ---- substitution ----

    def resolve_node(self, token: str) -> Any:
        token = token.strip()
        if token == "uniq":
            return self.uniq
        if token == "uuid":
            return str(uuid.uuid4())
        if token.startswith("uuid:"):
            return self.named_uuids.setdefault(token[5:], str(uuid.uuid4()))
        if token == "now_ns" or token.startswith("now_ns:"):
            # Wall-clock epoch nanoseconds as a decimal string; an
            # optional signed offset shifts the instant.
            shift = 0
            suffix = token[len("now_ns"):]
            if suffix.startswith(":"):
                shift = int(suffix[1:])
            return str(time.time_ns() + shift)
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
            if whole and "{{" not in whole.group(1):
                try:
                    return self.resolve_node(whole.group(1))
                except AssertionError:
                    return "null"
            return _TOKEN.sub(lambda m: str(self._resolve_or_null(m.group(1))), node)
        if isinstance(node, dict):
            return {k: self.substitute(v) for k, v in node.items()}
        if isinstance(node, list):
            return [self.substitute(v) for v in node]
        return node

    def _resolve_or_null(self, token: str) -> Any:
        try:
            return self.resolve_node(token)
        except AssertionError:
            return "null"

    @staticmethod
    def _split_path(path: str) -> list:
        segments = []
        depth = 0
        start = 0
        for index, char in enumerate(path):
            if char == "[":
                depth += 1
            elif char == "]":
                depth = max(0, depth - 1)
            elif char == "." and depth == 0:
                segments.append(path[start:index])
                start = index + 1
        segments.append(path[start:])
        return segments

    @classmethod
    def _path_get(cls, node: Any, path: str) -> Any:
        current = node
        for segment in cls._split_path(path):
            if current is None:
                return None
            name = segment
            indices = []
            bracket = segment.find("[")
            if bracket >= 0:
                name = segment[:bracket]
                rest = segment[bracket:]
                while rest.startswith("["):
                    close = rest.find("]")
                    if close < 0:
                        return None
                    indices.append(rest[1:close])
                    rest = rest[close + 1:]
            if name:
                if not isinstance(current, dict):
                    return None
                current = current.get(name)
            for index in indices:
                if len(index) >= 2 and index.startswith('"') and index.endswith('"'):
                    if not isinstance(current, dict):
                        return None
                    current = current.get(index[1:-1])
                    continue
                if not isinstance(current, list) or not index.isdigit() or int(index) >= len(current):
                    return None
                current = current[int(index)]
        return current

    # ---- dispatch ----

    def dispatch(self, op: str, args: Any) -> Tuple[int, Any]:
        if op == "health":
            return self.wire.get("/health")
        if op == "feed.records":
            assert isinstance(args, dict)
            path = "/v1/feeds/records?feed=" + quote(str(args.get("feed", "")), safe="")
            if args.get("limit"):
                path += "&limit=" + str(args["limit"])
            if args.get("after"):
                path += "&after=" + quote(str(args["after"]), safe="")
            status, body = self.wire.get(path)
            if isinstance(body, list):
                body = {"records": body}
            return status, body
        if op == "statestore.mutate":
            # Flat {store, mutation_id, kind, key, payload, named} lowers
            # to {store, mutation_id, op: {kind, key_base64?, ...}}.
            body = _wire_args(args)
            assert isinstance(body, dict), "statestore.mutate args must be an object"
            kind = body.pop("kind", None)
            assert kind, "statestore.mutate needs a `kind`"
            operation = {"kind": kind}
            for name in ("key_base64", "payload_base64", "named"):
                if name in body:
                    operation[name] = body.pop(name)
            body["op"] = operation
            return self.wire.post("/v1/state-stores/mutate", body)
        target = _ENDPOINTS.get(op)
        if target is None:
            raise AssertionError(f"unsupported op {op}")
        method, path = target
        return self.wire.request(method, path, _wire_args(args))

    def run_chaos(self, op: str, args: Any):
        if not self.chaos_cmd:
            raise AssertionError(f"unsupported: {op} needs a WHITEWATER_CHAOS_CMD hook")
        rendered = self.substitute(args if isinstance(args, dict) else {})
        action = op
        for key in sorted(rendered):
            action += " " + str(rendered[key])
        run = subprocess.run(
            f"{self.chaos_cmd} {action}",
            shell=True,
            capture_output=True,
            text=True,
        )
        if run.returncode != 0:
            raise AssertionError(
                f"chaos hook failed: {run.returncode}: {run.stdout}{run.stderr}"
            )

    # ---- expectations ----

    def check_expect(self, status: int, body: Any, expect: Dict[str, Any]):
        for path, expected in expect.items():
            if path == "error_status":
                assert status == int(expected), f"expected status {expected}, got {status}: {body}"
                continue
            if path == "error_code":
                code = body.get("code") if isinstance(body, dict) else None
                assert code == expected, f"expected code {expected}, got body {body}"
                continue
            actual = self._path_get(body, path)
            self._check(path, actual, expected, body)

    def _check(self, path: str, actual: Any, expected: Any, body: Any):
        if isinstance(expected, str):
            if expected in ("present", "!=null"):
                assert actual is not None, f"{path} absent in {body}"
                return
            if expected == "absent":
                assert actual is None, f"{path} present: {actual}"
                return
            if expected.startswith("len>="):
                minimum = int(expected[5:])
                try:
                    length = len(actual)
                except TypeError:
                    length = -1
                assert length >= minimum, f"{path} len {length} < {minimum} in {body}"
                return
            if expected.startswith("=="):
                reference = expected[2:]
                name, _, ref_path = reference.partition(".")
                bound = self.bound.get(name)
                expected = self._path_get(bound, ref_path) if ref_path else bound
                assert actual == expected, f"{path} = {actual!r}, expected {expected!r}"
                return
        if isinstance(expected, dict) and "base64_json" in expected:
            # Decode the actual base64 string as JSON, then apply each
            # named expectation to the decoded document.
            decoded = None
            if isinstance(actual, str):
                try:
                    decoded = json.loads(base64.b64decode(actual))
                except (ValueError, TypeError):
                    decoded = None
            assert decoded is not None, f"{path} is not base64 JSON: {actual!r} in {body}"
            try:
                self.check_expect(0, decoded, expected["base64_json"])
            except AssertionError as inner:
                raise AssertionError(f"{path} decoded: {inner}")
            return
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
        assert actual == expected, f"{path} = {actual!r}, expected {expected!r} in {body}"

    # ---- anonymous-uuid rebinding ----

    @staticmethod
    def _bind_anonymous_uuids(node: Any, step: int, ordinal: list) -> Any:
        if isinstance(node, str):
            while "{{uuid}}" in node:
                node = node.replace("{{uuid}}", "{{uuid:anon-%d-%d}}" % (step, ordinal[0]), 1)
                ordinal[0] += 1
            return node
        if isinstance(node, dict):
            return {k: Runner._bind_anonymous_uuids(v, step, ordinal) for k, v in node.items()}
        if isinstance(node, list):
            return [Runner._bind_anonymous_uuids(v, step, ordinal) for v in node]
        return node

    # ---- driver ----

    def run(self, fixture_path: Path):
        scenario = json.loads(fixture_path.read_text())
        for index, step in enumerate(scenario["steps"]):
            op = step["op"]
            expect = step.get("expect")

            if op.startswith("chaos."):
                try:
                    self.run_chaos(op, step.get("args") or {})
                except AssertionError as error:
                    raise AssertionError(f"step {index} ({op}): {error}")
                continue

            if op not in _ENDPOINTS and op not in ("feed.records", "health", "statestore.mutate"):
                raise AssertionError(f"step {index}: unsupported op {op}")

            args_template = self._bind_anonymous_uuids(step.get("args") or {}, index, [0])
            poll = step.get("poll") or {}
            poll_times = min(max(int(poll.get("times", 1)), 1), 30)
            poll_interval_ms = min(int(poll.get("interval_ms", 500)) or 500, 10_000)

            attempt = 0
            transient = 0
            status, body = 0, None
            while True:
                args = self.substitute(args_template)
                try:
                    status, body = self.dispatch(op, args)
                except TransportError:
                    if transient < TRANSIENT_ATTEMPTS:
                        transient += 1
                        time.sleep(TRANSIENT_INTERVAL)
                        continue
                    attempt += 1
                    if attempt < poll_times:
                        time.sleep(poll_interval_ms / 1000)
                        continue
                    raise AssertionError(f"step {index} ({op}): transport exhausted")
                if status >= 500 and transient < TRANSIENT_ATTEMPTS:
                    transient += 1
                    time.sleep(TRANSIENT_INTERVAL)
                    continue
                attempt += 1
                if expect is not None:
                    substituted = self.substitute(expect)
                    try:
                        self.check_expect(status, body, substituted)
                    except AssertionError as error:
                        if attempt < poll_times:
                            time.sleep(poll_interval_ms / 1000)
                            continue
                        raise AssertionError(
                            f"step {index} ({op}) expectation failed (status {status}): {error}"
                        )
                break
            if "bind" in step:
                self.bound[step["bind"]] = body


@pytest.mark.parametrize(
    "fixture", FIXTURES, ids=[f.stem for f in FIXTURES]
)
def test_conformance(fixture: Path):
    scenario = json.loads(fixture.read_text())
    wire = Wire(
        endpoints=[e.strip() for e in ENDPOINTS.split(",")],
        api_key=scenario.get("api_key", API_KEY),
    )
    try:
        Runner(wire, CHAOS_CMD).run(fixture)
    finally:
        wire.close()
