"""Whitewater client facades: resource management, Writer session,
Reader session, Subscription member, and Feed operations.

All share one :class:`_transport.Transport`, so endpoint
round-robining and error classification behave identically. Method
shapes mirror the conformance fixture operation vocabulary.
"""

from __future__ import annotations

import re
import uuid
from typing import Any, Dict, List, Optional, Union

from . import _wire
from ._transport import Transport
from .errors import Conflict
from .models import (
    AppendBatchResult,
    AppendResult,
    ControlExecution,
    Cursor,
    FeedAppend,
    Joined,
    MemberPage,
    ReaderPage,
    ReaderSession,
    ReaderStart,
    Record,
    TemporaryFetchOptions,
    TemporaryFetchPage,
    WorkLease,
)

UuidLike = Union[uuid.UUID, str]

_DOTTED_RE = re.compile(r"^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$")
_RESOURCE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")


def _dotted(name: str) -> str:
    """Lowercase dotted names (domains, feeds), ≤512 bytes."""
    if not name or len(name) > 512 or not _DOTTED_RE.match(name):
        raise ValueError(f"invalid dotted name: {name!r}")
    return name


def _resource(name: str) -> str:
    """Single-segment resource names (writers, readers, subscriptions, roles)."""
    if not name or not _RESOURCE_RE.match(name):
        raise ValueError(f"invalid resource name: {name!r}")
    return name


class WriterSessionClient:
    """One named-Writer session: idempotent appends fenced by epoch."""

    def __init__(self, transport: Transport, writer: str, session_epoch: int):
        self._transport = transport
        self._writer = writer
        self._session_epoch = session_epoch

    def append(
        self,
        key: Optional[bytes],
        payload: bytes,
        *,
        request_id: Optional[uuid.UUID] = None,
        event_time_ns: Optional[int] = None,
        metadata: Optional[Dict[str, bytes]] = None,
    ) -> AppendResult:
        body = _wire.append_request(
            request_id or uuid.uuid4(),
            self._writer,
            self._session_epoch,
            key,
            payload,
            event_time_ns,
            metadata or {},
        )
        return _wire.append_result(self._transport.post("/v1/writers/append", body))

    def append_batch(self, records: List[dict]) -> AppendBatchResult:
        """Append many records; each entry takes request_id/key/payload/
        event_time_ns/metadata as in :meth:`append`."""
        body = {
            "records": [
                _wire.append_request(
                    r.get("request_id") or uuid.uuid4(),
                    self._writer,
                    self._session_epoch,
                    r.get("key"),
                    r["payload"],
                    r.get("event_time_ns"),
                    r.get("metadata") or {},
                )
                for r in records
            ]
        }
        response = self._transport.post("/v1/writers/append-batch", body)
        return AppendBatchResult(
            results=[_wire.append_result(item) for item in response.get("results") or []],
            feedback=_wire.feedback(response.get("feedback")),
        )


class ReaderSessionClient:
    """One named-Reader session: open/fetch/ack/close with durable resume."""

    def __init__(self, transport: Transport, reader: str):
        self._transport = transport
        self._reader = reader

    def open(self, capacity: int = 64, request_id: Optional[uuid.UUID] = None) -> ReaderSession:
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "reader": self._reader,
            "capacity": capacity,
        }
        return _wire.reader_session(self._transport.post("/v1/readers/open", body))

    def fetch(
        self,
        session: ReaderSession,
        limit: Optional[int] = None,
        request_id: Optional[uuid.UUID] = None,
    ) -> ReaderPage:
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "reader": self._reader,
            "session_epoch": session.session_epoch,
        }
        if limit is not None:
            body["limit"] = limit
        return _wire.reader_page(self._transport.post("/v1/readers/fetch", body))

    def ack(
        self,
        session: ReaderSession,
        cursor,
        request_id: Optional[uuid.UUID] = None,
    ) -> ReaderSession:
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "reader": self._reader,
            "session_epoch": session.session_epoch,
            "cursor": str(cursor),
        }
        return _wire.reader_session(self._transport.post("/v1/readers/ack", body))

    def close(self, session: ReaderSession) -> ReaderSession:
        body = {
            "request_id": str(uuid.uuid4()),
            "reader": self._reader,
            "session_epoch": session.session_epoch,
        }
        return _wire.reader_session(self._transport.post("/v1/readers/close", body))


class SubscriptionMemberClient:
    """One cooperating member of a named Subscription.

    Delivery is at-least-once and fenced at commit: fetch under a held
    lease, then ack the page's ``cursor``/``positions`` verbatim.
    """

    def __init__(self, transport: Transport, subscription: str):
        self._transport = transport
        self._subscription = subscription

    def _lease_body(
        self,
        member: Joined,
        work_id: UuidLike,
        lease_epoch: Optional[int],
    ) -> dict:
        body = {
            "subscription": self._subscription,
            "request_id": str(uuid.uuid4()),
            "member_id": str(member.member_id),
            "member_epoch": member.member_epoch,
            "work_id": str(work_id),
        }
        if lease_epoch is not None:
            body["lease_epoch"] = lease_epoch
        return body

    def join(self, member_id: UuidLike) -> Joined:
        body = {
            "subscription": self._subscription,
            "request_id": str(uuid.uuid4()),
            "member_id": str(member_id),
        }
        response = self._transport.post("/v1/subscriptions/members/join", body)
        return Joined(
            member_id=uuid.UUID(str(member_id)),
            member_epoch=response["member_epoch"],
            ownership_epoch=response.get("ownership_epoch", 0),
            tick=response.get("tick", 0),
        )

    def claim(
        self,
        member: Joined,
        work_id: UuidLike,
        lease_ticks: Optional[int] = None,
    ) -> WorkLease:
        return self._lease("/v1/subscriptions/members/claim", member, work_id, None, lease_ticks)

    def renew(
        self,
        member: Joined,
        lease: WorkLease,
        lease_ticks: Optional[int] = None,
    ) -> WorkLease:
        return self._lease(
            "/v1/subscriptions/members/renew",
            member,
            lease.work_id,
            lease.lease_epoch,
            lease_ticks,
        )

    def _lease(
        self,
        path: str,
        member: Joined,
        work_id: UuidLike,
        lease_epoch: Optional[int],
        lease_ticks: Optional[int],
    ) -> WorkLease:
        body = self._lease_body(member, work_id, lease_epoch)
        if lease_ticks is not None:
            body["lease_ticks"] = lease_ticks
        response = self._transport.post(path, body)
        lease = response.get("lease")
        if not lease:
            raise Conflict(409, "lease.refused", "work claim was not granted")
        return WorkLease(
            work_id=uuid.UUID(lease["work_id"]),
            member_id=uuid.UUID(lease["member_id"]),
            member_epoch=lease["member_epoch"],
            lease_epoch=lease["lease_epoch"],
            expires_at_tick=lease.get("expires_at_tick", 0),
        )

    def release(self, member: Joined, lease: WorkLease) -> None:
        self._transport.post(
            "/v1/subscriptions/members/release",
            self._lease_body(member, lease.work_id, lease.lease_epoch),
        )

    def fetch(
        self,
        member: Joined,
        lease: WorkLease,
        limit: Optional[int] = None,
    ) -> MemberPage:
        body = self._lease_body(member, lease.work_id, lease.lease_epoch)
        if limit is not None:
            body["limit"] = limit
        return _wire.member_page(self._transport.post("/v1/subscriptions/members/fetch", body))

    def ack(self, member: Joined, lease: WorkLease, page: MemberPage) -> None:
        body = self._lease_body(member, lease.work_id, lease.lease_epoch)
        if page.cursor is not None:
            body["cursor"] = str(page.cursor)
        body["positions"] = dict(page.positions)
        self._transport.post("/v1/subscriptions/members/ack", body)


class FeedClient:
    """Ad-hoc Feed operations: committed record reads, anonymous
    temporary fetches, and single-shot appends with a caller-supplied
    writer identity — no named session required."""

    def __init__(self, transport: Transport):
        self._transport = transport

    def records(
        self,
        feed: str,
        limit: Optional[int] = None,
        after: Optional[Cursor] = None,
    ) -> List[Record]:
        """Committed record history via ``GET /v1/feeds/records``."""
        path = "/v1/feeds/records?feed=" + feed
        if limit is not None:
            path += "&limit=" + str(limit)
        if after is not None:
            path += "&after=" + str(after)
        return _wire.records(self._transport.get(path))

    def temporary_fetch(
        self,
        feed: str,
        options: Optional[TemporaryFetchOptions] = None,
    ) -> TemporaryFetchPage:
        """Anonymous temporary Reader fetch: bounded page after a Cursor,
        ``new_only`` positioning, or ``tail`` + ``wait_ms`` long-poll."""
        options = options or TemporaryFetchOptions()
        body: Dict[str, Any] = {"feed": feed}
        if options.after is not None:
            body["after"] = str(options.after)
        if options.limit is not None:
            body["limit"] = options.limit
        if options.new_only:
            body["new_only"] = True
        if options.tail:
            body["tail"] = True
        if options.wait_ms is not None:
            body["wait_ms"] = options.wait_ms
        if options.after_event_time_ns is not None:
            body["after_event_time_ns"] = str(options.after_event_time_ns)
        return _wire.temporary_fetch_page(
            self._transport.post("/v1/readers/temporary/fetch", body)
        )

    def append(self, feed: str, record: FeedAppend) -> AppendResult:
        """Ad-hoc append; identical ``request_id`` retries dedupe."""
        return _wire.append_result(
            self._transport.post(
                "/v1/feeds/append", _wire.feed_append_request(feed, record)
            )
        )


class WhitewaterClient:
    """Entry point: resource management (WCL, typed commands, health) plus
    the Writer, Reader, Subscription-member, and Feed facades."""

    def __init__(
        self,
        api_key: str,
        endpoints: List[str],
        timeout: float = 60.0,
    ):
        self._transport = Transport(endpoints, api_key, timeout)
        self.feeds = FeedClient(self._transport)

    def wcl(self, script: str, request_id: Optional[uuid.UUID] = None) -> ControlExecution:
        """Execute a WCL script; identical ``request_id`` retries return the
        original committed execution."""
        body = {
            "request_id": str(request_id or uuid.uuid4()),
            "script": script,
        }
        return _wire.control_execution(self._transport.post("/v1/admin/wcl", body))

    def commands(
        self,
        commands: List[dict],
        request_id: Optional[uuid.UUID] = None,
    ) -> ControlExecution:
        """Execute typed control commands, e.g.
        ``[{"command": "show", "kind": "feeds"}]``."""
        body: Dict[str, Any] = {"commands": commands}
        if request_id is not None:
            body["request_id"] = str(request_id)
        return _wire.control_execution(self._transport.post("/v1/admin/commands", body))

    def healthy(self) -> bool:
        """Liveness of the contacted Node."""
        health = self._transport.get("/health")
        return isinstance(health, dict) and health.get("status") == "ok"

    # ---- Resource management --------------------------------------------------
    # Typed convenience over :meth:`wcl`: each method builds one WCL statement
    # and executes it. Names are validated before interpolation, so a bad value
    # fails locally rather than reaching the Control Plane.

    def create_domain(self, name: str) -> ControlExecution:
        """``CREATE DOMAIN <name>``"""
        return self.wcl("CREATE DOMAIN " + _dotted(name))

    def create_feed(self, name: str) -> ControlExecution:
        """``CREATE FEED <dotted.name>``"""
        return self.wcl("CREATE FEED " + _dotted(name))

    def create_writer(self, name: str, feed: str) -> ControlExecution:
        """``CREATE WRITER <name> TO <feed>``"""
        return self.wcl(f"CREATE WRITER {_resource(name)} TO {_dotted(feed)}")

    def create_reader(
        self, name: str, feed: str, start: Optional["ReaderStart"] = None
    ) -> ControlExecution:
        """``CREATE READER <name> FROM <feed> [START AT <start>]`` — BEGINNING
        when ``start`` is omitted."""
        return self._create_from("READER", name, feed, start or ReaderStart.beginning())

    def create_subscription(
        self, name: str, feed: str, start: Optional["ReaderStart"] = None
    ) -> ControlExecution:
        """``CREATE SUBSCRIPTION <name> FROM <feed> [START AT <start>]``"""
        return self._create_from(
            "SUBSCRIPTION", name, feed, start or ReaderStart.beginning()
        )

    def _create_from(
        self, kind: str, name: str, feed: str, start: "ReaderStart"
    ) -> ControlExecution:
        return self.wcl(
            f"CREATE {kind} {_resource(name)} FROM {_dotted(feed)} START AT {start.wcl}"
        )

    def create_role(self, name: str) -> ControlExecution:
        """``CREATE ROLE <name>``"""
        return self.wcl("CREATE ROLE " + _resource(name))

    def open_writer_session(self, writer: str) -> ControlExecution:
        """``OPEN WRITER SESSION <name>``; the returned epoch feeds
        :meth:`writer`."""
        return self.wcl("OPEN WRITER SESSION " + _resource(writer))

    def seek_reader(self, reader: str, start: "ReaderStart") -> ControlExecution:
        """``SEEK READER <name> TO <start>`` — repositions a durable Reader."""
        return self.wcl(f"SEEK READER {_resource(reader)} TO {start.wcl}")

    def describe_domain(self, name: str) -> ControlExecution:
        """``DESCRIBE DOMAIN <name>``"""
        return self._describe("DOMAIN", name, True)

    def describe_feed(self, name: str) -> ControlExecution:
        """``DESCRIBE FEED <dotted.name>``"""
        return self._describe("FEED", name, True)

    def describe_writer(self, name: str) -> ControlExecution:
        """``DESCRIBE WRITER <name>``"""
        return self._describe("WRITER", name, False)

    def describe_reader(self, name: str) -> ControlExecution:
        """``DESCRIBE READER <name>``"""
        return self._describe("READER", name, False)

    def describe_subscription(self, name: str) -> ControlExecution:
        """``DESCRIBE SUBSCRIPTION <name>``"""
        return self._describe("SUBSCRIPTION", name, False)

    def describe_role(self, name: str) -> ControlExecution:
        """``DESCRIBE ROLE <name>``"""
        return self._describe("ROLE", name, False)

    def rename_domain(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME DOMAIN <current> TO <renamed>``"""
        return self._rename("DOMAIN", current, renamed, True)

    def rename_feed(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME FEED <current> TO <renamed>``"""
        return self._rename("FEED", current, renamed, True)

    def rename_writer(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME WRITER <current> TO <renamed>``"""
        return self._rename("WRITER", current, renamed, False)

    def rename_reader(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME READER <current> TO <renamed>``"""
        return self._rename("READER", current, renamed, False)

    def rename_subscription(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME SUBSCRIPTION <current> TO <renamed>``"""
        return self._rename("SUBSCRIPTION", current, renamed, False)

    def rename_role(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME ROLE <current> TO <renamed>``"""
        return self._rename("ROLE", current, renamed, False)

    def drop_domain(self, name: str) -> ControlExecution:
        """``DROP DOMAIN <name>``"""
        return self._drop("DOMAIN", name, True)

    def drop_feed(self, name: str) -> ControlExecution:
        """``DROP FEED <dotted.name>``"""
        return self._drop("FEED", name, True)

    def drop_writer(self, name: str) -> ControlExecution:
        """``DROP WRITER <name>``"""
        return self._drop("WRITER", name, False)

    def drop_reader(self, name: str) -> ControlExecution:
        """``DROP READER <name>``"""
        return self._drop("READER", name, False)

    def drop_subscription(self, name: str) -> ControlExecution:
        """``DROP SUBSCRIPTION <name>``"""
        return self._drop("SUBSCRIPTION", name, False)

    def drop_role(self, name: str) -> ControlExecution:
        """``DROP ROLE <name>``"""
        return self._drop("ROLE", name, False)

    def list_domains(self) -> ControlExecution:
        """``SHOW DOMAINS`` — rows live in ``results[0].data``."""
        return self.wcl("SHOW DOMAINS")

    def list_feeds(self) -> ControlExecution:
        """``SHOW FEEDS``"""
        return self.wcl("SHOW FEEDS")

    def list_writers(self) -> ControlExecution:
        """``SHOW WRITERS``"""
        return self.wcl("SHOW WRITERS")

    def list_readers(self) -> ControlExecution:
        """``SHOW READERS``"""
        return self.wcl("SHOW READERS")

    def list_subscriptions(self) -> ControlExecution:
        """``SHOW SUBSCRIPTIONS``"""
        return self.wcl("SHOW SUBSCRIPTIONS")

    def list_roles(self) -> ControlExecution:
        """``SHOW ROLES``"""
        return self.wcl("SHOW ROLES")

    def list_grants(self) -> ControlExecution:
        """``SHOW GRANTS``"""
        return self.wcl("SHOW GRANTS")

    def _describe(self, kind: str, name: str, dotted: bool) -> ControlExecution:
        return self.wcl(f"DESCRIBE {kind} {self._name(name, dotted)}")

    def _rename(
        self, kind: str, current: str, renamed: str, dotted: bool
    ) -> ControlExecution:
        return self.wcl(
            f"RENAME {kind} {self._name(current, dotted)} TO {self._name(renamed, dotted)}"
        )

    def _drop(self, kind: str, name: str, dotted: bool) -> ControlExecution:
        return self.wcl(f"DROP {kind} {self._name(name, dotted)}")

    @staticmethod
    def _name(name: str, dotted: bool) -> str:
        return _dotted(name) if dotted else _resource(name)

    def writer(self, name: str, session_epoch: int) -> WriterSessionClient:
        return WriterSessionClient(self._transport, name, session_epoch)

    def reader(self, name: str) -> ReaderSessionClient:
        return ReaderSessionClient(self._transport, name)

    def subscription_member(self, subscription: str) -> SubscriptionMemberClient:
        return SubscriptionMemberClient(self._transport, subscription)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "WhitewaterClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
