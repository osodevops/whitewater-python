"""Whitewater client facades: Admin, Writer session, Reader session, and
Subscription member.

All four share one :class:`_transport.Transport`, so endpoint
round-robining and error classification behave identically. Method
shapes mirror the conformance fixture operation vocabulary.
"""

from __future__ import annotations

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
    Record,
    TemporaryFetchOptions,
    TemporaryFetchPage,
    WorkLease,
)

UuidLike = Union[uuid.UUID, str]


class AdminClient:
    """WCL script execution and Node health."""

    def __init__(self, transport: Transport):
        self._transport = transport

    def wcl(self, script: str, request_id: Optional[uuid.UUID] = None) -> ControlExecution:
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
        health = self._transport.get("/health")
        return isinstance(health, dict) and health.get("status") == "ok"


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
    """Entry point: one transport, five capability facades."""

    def __init__(
        self,
        api_key: str,
        endpoints: List[str],
        timeout: float = 60.0,
    ):
        self._transport = Transport(endpoints, api_key, timeout)
        self.admin = AdminClient(self._transport)
        self.feeds = FeedClient(self._transport)

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
