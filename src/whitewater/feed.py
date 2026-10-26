"""Typed Feed facades — the codec seam applied to Feeds end to end.

Bind ``Codec``s once on :meth:`WhitewaterClient.feed` and keys/payloads
serialize automatically in both directions::

    orders = client.feed("orders.events", value_codec=codecs.json_of(Order))

    orders.writer("ingest").append("order-1", Order("order-1", 42))

    with orders.reader("audit", capacity=64) as reader:
        for message in reader.fetch().records:
            handle(message.value)               # decoded — never .encode()
        reader.ack()

``client.feed(name)`` binds ``str`` keys with raw ``bytes`` payloads;
``view`` rebinds the same feed under different codecs —
``feed.view(utf8(), raw())`` is the escape hatch to wire bytes.
"""

from __future__ import annotations

import queue
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import Callable, Dict, Generic, List, Optional, TypeVar, Union

from . import codecs as codecs_module
from ._transport import Transport
from .codecs import Codec
from .errors import Refused, WhitewaterError
from .models import (
    AppendBatchResult,
    AppendResult,
    Cursor,
    FeedAppend,
    ReaderSession,
    ReaderStart,
    Record,
    TemporaryFetchOptions,
)
from .models import (
    ReaderPage,
    TemporaryFetchPage,
)

K = TypeVar("K")
V = TypeVar("V")
K2 = TypeVar("K2")
V2 = TypeVar("V2")


@dataclass(frozen=True)
class FeedMessage(Generic[K, V]):
    """One committed Feed record with its key and payload already
    decoded through the bound codecs — the :class:`Record` fields are
    identical; only the payloads are typed. Times are signed 64-bit
    Unix epoch nanoseconds; ``cursor`` is opaque — pass it back
    verbatim, never parse it."""

    cursor: Optional[Cursor]
    message_id: Optional[uuid.UUID]
    producer_id: Optional[uuid.UUID]
    sequence: int
    event_time_ns: int
    ingest_time_ns: int
    key: Optional[K]
    value: Optional[V]
    metadata: Dict[str, bytes] = field(default_factory=dict)

    @staticmethod
    def decode(
        record: Record,
        key_codec: Codec,
        value_codec: Codec,
    ) -> "FeedMessage":
        return FeedMessage(
            cursor=record.cursor,
            message_id=record.message_id,
            producer_id=record.producer_id,
            sequence=record.sequence,
            event_time_ns=record.event_time_ns,
            ingest_time_ns=record.ingest_time_ns,
            key=key_codec.decode(record.key) if record.key is not None else None,
            value=value_codec.decode(record.payload)
            if record.payload is not None
            else None,
            metadata=record.metadata,
        )

    def metadata_text(self, name: str) -> Optional[str]:
        """A Metadata entry decoded as UTF-8, or None when absent."""
        value = self.metadata.get(name)
        return value.decode("utf-8") if value is not None else None


@dataclass(frozen=True)
class FeedEvent(Generic[K, V]):
    """A typed record to append: key, payload, optional request identity,
    event time and Metadata. The bound codecs serialize ``key``/``value``
    at write time — application code never touches bytes::

        writer.append(FeedEvent("order-1", order))
        writer.append(FeedEvent("order-2", order2,
                                request_id=request_id,
                                metadata=FeedEvent.text_metadata({"tenant": "acme"})))
    """

    key: Optional[K]
    value: Optional[V]
    request_id: Optional[uuid.UUID] = None
    event_time_ns: Optional[int] = None
    metadata: Dict[str, bytes] = field(default_factory=dict)

    def with_request_id(self, request_id: uuid.UUID) -> "FeedEvent[K, V]":
        """Pin a request identity so caller-driven retries dedupe."""
        return replace(self, request_id=request_id)

    def with_event_time_ns(self, nanos: int) -> "FeedEvent[K, V]":
        return replace(self, event_time_ns=nanos)

    def with_metadata(self, metadata: Dict[str, bytes]) -> "FeedEvent[K, V]":
        return replace(self, metadata=metadata)

    def with_text_metadata(self, metadata: Dict[str, str]) -> "FeedEvent[K, V]":
        return replace(self, metadata=self.text_metadata(metadata))

    @staticmethod
    def text_metadata(metadata: Dict[str, str]) -> Dict[str, bytes]:
        """Encode a text Metadata map to wire bytes."""
        return {
            name: value.encode("utf-8") if value is not None else None
            for name, value in metadata.items()
        }


@dataclass(frozen=True)
class FeedPage(Generic[K, V]):
    """A decoded temporary-fetch page: messages plus the opaque
    continuation frontier."""

    records: List[FeedMessage[K, V]]
    next_cursor: Optional[Cursor]


@dataclass(frozen=True)
class FeedReaderPage(Generic[K, V]):
    """One bounded page of committed records, decoded. Acknowledge
    ``delivered_cursor`` — not the last record's cursor — to durably
    commit progress."""

    records: List[FeedMessage[K, V]]
    delivered_cursor: Optional[Cursor]
    acknowledged_cursor: Optional[Cursor]
    recommended_capacity: int
    retry_after_ms: int


class FeedWriter(Generic[K, V]):
    """A session-bound Writer with the feed's codecs applied — the
    session epoch and request identities stay inside the handle::

        writer.append("order-1", order)
        writer.append(FeedEvent("order-2", order2).with_request_id(rid))
        writer.append_all([FeedEvent("o3", o3), FeedEvent("o4", o4)])
    """

    def __init__(self, session_client, key_codec: Codec[K], value_codec: Codec[V]):
        self._session = session_client
        self._key_codec = key_codec
        self._value_codec = value_codec

    @property
    def writer(self) -> str:
        return self._session._writer

    @property
    def session_epoch(self) -> int:
        return self._session._session_epoch

    def use_compression(self, enabled: bool = True) -> "FeedWriter[K, V]":
        return FeedWriter(
            self._session.use_compression(enabled),
            self._key_codec,
            self._value_codec,
        )

    def append(
        self,
        key: Optional[K],
        value: Optional[V],
        *,
        request_id: Optional[uuid.UUID] = None,
        event_time_ns: Optional[int] = None,
        metadata: Optional[Dict[str, bytes]] = None,
    ) -> AppendResult:
        """Append one record; codecs serialize key/payload, identity
        auto-generates. Pass ``request_id`` for caller-driven retries."""
        return self._session.append(
            self._key_codec.encode(key) if key is not None else None,
            self._value_codec.encode(value) if value is not None else None,
            request_id=request_id,
            event_time_ns=event_time_ns,
            metadata=metadata,
        )

    def append_event(self, event: FeedEvent[K, V]) -> AppendResult:
        return self.append(
            event.key,
            event.value,
            request_id=event.request_id,
            event_time_ns=event.event_time_ns,
            metadata=event.metadata or None,
        )

    def append_all(self, events: List[FeedEvent[K, V]]) -> AppendBatchResult:
        """Append a batch under one request; per-record results plus
        server batching guidance."""
        return self._session.append_batch(
            [
                {
                    "request_id": e.request_id,
                    "key": self._key_codec.encode(e.key) if e.key is not None else None,
                    "payload": self._value_codec.encode(e.value)
                    if e.value is not None
                    else None,
                    "event_time_ns": e.event_time_ns,
                    "metadata": e.metadata,
                }
                for e in events
            ]
        )


class FeedReader(Generic[K, V]):
    """A session-bound named Reader with the feed's codecs applied —
    ``fetch`` returns a bounded page of decoded messages; ``ack`` commits
    the page's delivered Cursor durably::

        with feed.reader("audit", capacity=64) as reader:
            for message in reader.fetch().records:
                handle(message.value)
            reader.ack()
    """

    def __init__(self, session_client, session: ReaderSession, key_codec: Codec[K], value_codec: Codec[V]):
        self._session_client = session_client
        self._session = session
        self._key_codec = key_codec
        self._value_codec = value_codec
        self._last_page: Optional[FeedReaderPage[K, V]] = None
        self._closed = False

    @property
    def session_epoch(self) -> int:
        return self._session.session_epoch

    @property
    def acknowledged_cursor(self) -> Optional[Cursor]:
        return self._session.acknowledged_cursor

    def fetch(self, limit: Optional[int] = None) -> FeedReaderPage[K, V]:
        """Fetch up to ``limit`` committed records after the frontier,
        decoded through the bound codecs."""
        page = self._session_client.fetch(self._session, limit)
        self._last_page = FeedReaderPage(
            records=[
                FeedMessage.decode(r, self._key_codec, self._value_codec)
                for r in page.records
            ],
            delivered_cursor=page.delivered_cursor,
            acknowledged_cursor=page.acknowledged_cursor,
            recommended_capacity=page.recommended_capacity,
            retry_after_ms=page.retry_after_ms,
        )
        return self._last_page

    def ack(self, cursor: Optional[Union[Cursor, str]] = None) -> ReaderSession:
        """Durably commit the last fetched page's delivered Cursor (or an
        explicit one). Call after processing a page."""
        target = cursor
        if target is None and self._last_page is not None:
            target = self._last_page.delivered_cursor
        if target is None:
            return self._session
        self._session = self._session_client.ack(self._session, target)
        return self._session

    def close(self) -> None:
        """Close the session; durable acknowledged progress survives."""
        if self._closed:
            return
        self._closed = True
        self._session = self._session_client.close(self._session)

    def __enter__(self) -> "FeedReader[K, V]":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


class Feed(Generic[K, V]):
    """A typed facade over one Feed — the immutable temporal history on
    the Nodes. The bound codecs serialize on write and deserialize on
    read, so application code stays in its own types::

        orders = client.feed("orders.events", value_type=Order)
        orders.writer("ingest").append("order-1", Order("order-1", 42))

        with orders.reader("audit", capacity=64) as reader:
            page = reader.fetch()             # FeedMessage[str, Order]

    ``feed.view(...)`` rebinds the same feed under different codecs —
    ``feed.view(utf8(), raw())`` reads raw wire bytes.
    """

    def __init__(
        self,
        client,
        name: str,
        key_codec: Codec[K],
        value_codec: Codec[V],
        *,
        feed_client=None,
    ):
        self._client = client
        self._feed_client = feed_client or client.feeds
        self.name = name
        self.key_codec = key_codec
        self.value_codec = value_codec

    # ---- reading ----------------------------------------------------------

    def records(
        self,
        limit: Optional[int] = None,
        after: Optional[Cursor] = None,
    ) -> List[FeedMessage[K, V]]:
        """Committed records from the beginning or after an opaque
        ``after`` Cursor, decoded through the bound codecs."""
        return [
            self._decode(record)
            for record in self._feed_client.records(self.name, limit=limit, after=after)
        ]

    def fetch(
        self, options: Optional[TemporaryFetchOptions] = None
    ) -> FeedPage[K, V]:
        """Anonymous temporary Reader fetch: decoded records plus the
        opaque continuation frontier. Retains no server-side progress."""
        page = self._feed_client.temporary_fetch(self.name, options)
        return FeedPage(
            records=[self._decode(record) for record in page.records],
            next_cursor=page.next_cursor,
        )

    def load(
        self,
        start: ReaderStart,
        sink: Callable[[List[FeedMessage[K, V]]], None],
    ) -> int:
        """Bulk-read committed history once, from ``start``, delivering
        each decoded page to ``sink`` until the frontier is drained."""
        return self._feed_client.load(
            self.name,
            start,
            lambda records: sink([self._decode(r) for r in records]),
        )

    # ---- writing ----------------------------------------------------------

    def writer(self, writer_name: str) -> FeedWriter[K, V]:
        """A session-bound Writer on this Feed: declares the Writer when
        missing (reusing an existing one), opens a session, and binds the
        feed's codecs so ``writer.append(key, value)`` just works."""
        self._ensure_writer(writer_name)
        session_epoch = self._open_writer_session(writer_name)
        session = self._client.writer(writer_name, session_epoch)
        return FeedWriter(session, self.key_codec, self.value_codec)

    def append(
        self,
        writer_session_id: uuid.UUID,
        writer_epoch: int,
        sequence: int,
        key: Optional[K],
        value: Optional[V],
        *,
        request_id: Optional[uuid.UUID] = None,
        event_time_ns: Optional[int] = None,
        metadata: Optional[Dict[str, bytes]] = None,
    ) -> AppendResult:
        """Ad-hoc single append with a caller-owned writer identity —
        codecs serialize key/payload. For durable named producers prefer
        :meth:`writer`."""
        return self._feed_client.append(
            self.name,
            FeedAppend(
                request_id=request_id or uuid.uuid4(),
                writer_session_id=writer_session_id,
                writer_epoch=writer_epoch,
                sequence=sequence,
                key=self.key_codec.encode(key) if key is not None else None,
                payload=self.value_codec.encode(value) if value is not None else None,
                event_time_ns=event_time_ns,
                metadata=metadata or {},
            ),
        )

    # ---- readers / subscriptions -------------------------------------------

    def reader(self, reader_name: str, capacity: int = 64) -> FeedReader[K, V]:
        """A session-bound named Reader on this Feed: declares the Reader
        (at BEGINNING) when missing, opens a session, and returns decoded
        messages."""
        self._ensure_reader(reader_name)
        session_client = self._client.reader(reader_name)
        session = session_client.open(capacity)
        return FeedReader(session_client, session, self.key_codec, self.value_codec)

    def subscribe(
        self,
        reader: str,
        start: ReaderStart,
        on_records: Callable[[List[FeedMessage[K, V]]], None],
        on_error: Optional[Callable[[WhitewaterError], None]] = None,
        capacity: Optional[int] = None,
    ):
        """A running subscription on this Feed: a named Reader polled on
        a background thread, delivering decoded batches and acknowledging
        durable progress after each."""
        return self._subscribe_decoded(
            reader, start, capacity, on_records, on_error)

    def _subscribe_decoded(
        self,
        reader: str,
        start: ReaderStart,
        capacity: Optional[int],
        on_records: Callable[[List[FeedMessage[K, V]]], None],
        on_error: Optional[Callable[[WhitewaterError], None]],
    ):
        return self._client.subscribe(
            reader,
            self.name,
            start,
            lambda records: on_records([self._decode(r) for r in records]),
            on_error,
            capacity=capacity,
        )

    def consumer(self, reader: str) -> "FeedConsumer[K, V]":
        """A permanent, supervised consumer bound to this Feed's named
        Reader — per-record delivery, internal restart/backoff, durable
        progress, graceful close. Iterate it or :meth:`run` a handler::

            for msg in feed.consumer("enricher"):
                process(msg)

            feed.consumer("enricher").on_record(process).run()

        See :class:`FeedConsumer`.
        """
        return FeedConsumer(self, reader)

    # ---- views -----------------------------------------------------------

    def view(self, key_codec: Codec[K2], value_codec: Codec[V2]) -> "Feed[K2, V2]":
        """Rebind the same Feed under different codecs — no wire call."""
        return Feed(
            self._client,
            self.name,
            key_codec,
            value_codec,
            feed_client=self._feed_client,
        )

    def view_types(self, key_type, value_type) -> "Feed":
        """``view`` by Python types (``str``/``bytes``/``dict``/classes)."""
        return self.view(
            codecs_module.for_type(key_type),
            codecs_module.for_type(value_type),
        )

    def use_compression(self, enabled: bool = True) -> "Feed[K, V]":
        """Wire compression on fetches and ad-hoc appends through this
        facade."""
        return Feed(
            self._client,
            self.name,
            self.key_codec,
            self.value_codec,
            feed_client=self._feed_client.use_compression(enabled),
        )

    # ---- internals --------------------------------------------------------

    def _decode(self, record: Record) -> FeedMessage[K, V]:
        return FeedMessage.decode(record, self.key_codec, self.value_codec)

    def _ensure_writer(self, writer_name: str) -> None:
        try:
            self._client.create_writer(writer_name, self.name)
        except Refused as e:
            if "exists" not in str(e).lower():
                raise

    def _ensure_reader(self, reader_name: str, start=None) -> None:
        try:
            self._client.create_reader(reader_name, self.name, start)
        except Refused as e:
            if "exists" not in str(e).lower():
                raise

    def _open_writer_session(self, writer_name: str) -> int:
        execution = self._client.commands(
            [{"command": "open_writer_session", "writer": writer_name}]
        )
        epoch = execution.results[0].data.get("session_epoch")
        if not isinstance(epoch, int):
            raise ValueError(
                f"open_writer_session returned no session_epoch for {writer_name}"
            )
        return epoch


_SENTINEL = object()


class FeedConsumer(Generic[K, V]):
    """A permanent, supervised consumer: the durable-Reader equivalent
    of Kafka's ``while True: consumer.poll(...)`` loop, with the
    plumbing removed::

        for msg in feed.consumer("enricher"):
            process(msg)

        # or a handler:
        consumer = feed.consumer("enricher").on_record(process)
        consumer.run()          # blocks until close()

    Semantics:

    - The named Reader's acked cursor is durable server-side — a restart
      (app or session) resumes where it stopped; no offset management.
    - Each fetched page is delivered record-by-record; the page's
      delivered cursor is acknowledged only after the handler returns —
      a handler exception leaves the page unacked so it redelivers
      (at-least-once). In ``for`` form, an exception in the loop body
      has the same effect: the page was not acked.
    - Transient failures retry with exponential backoff inside the
      supervisor; a dead session re-opens and resumes. :meth:`run`
      raises only for startup refusals (misconfiguration, denied auth)
      — everything after a successful first open is supervised forever
      until :meth:`close`.
    - :meth:`close` finishes the in-flight page, closes the session,
      and lets ``run()``/iteration return — the SIGTERM path.
    """

    _POLL_S = 0.05

    def __init__(self, feed: Feed[K, V], reader: str):
        self._feed = feed
        self._reader = reader
        self._start = ReaderStart.beginning()
        self._capacity: Optional[int] = None
        self._initial_backoff = 0.25
        self._max_backoff = 30.0
        self._on_record: Optional[Callable[[FeedMessage[K, V]], None]] = None
        self._on_error: Callable[[BaseException], None] = lambda _err: None
        self._stop = threading.Event()
        self._current = None
        self._thread: Optional[threading.Thread] = None
        self._last_error: Optional[BaseException] = None
        self._running = False

    @property
    def reader(self) -> str:
        """The declared Reader's name — its durable identity."""
        return self._reader

    def start_at(self, start: ReaderStart) -> "FeedConsumer[K, V]":
        """Where the Reader starts on first declaration (default
        BEGINNING); ignored once the Reader exists — it resumes from
        durable progress."""
        self._start = start
        return self

    def capacity(self, capacity: int) -> "FeedConsumer[K, V]":
        """Records fetched per page."""
        self._capacity = capacity
        return self

    def on_record(
        self, handler: Callable[[FeedMessage[K, V]], None]
    ) -> "FeedConsumer[K, V]":
        """The per-record handler — required for :meth:`run`. A thrown
        exception leaves the page unacked so it redelivers; poison
        pills are the app's job (route to a DLQ feed)."""
        self._on_record = handler
        return self

    def on_error(
        self, handler: Callable[[BaseException], None]
    ) -> "FeedConsumer[K, V]":
        """Error sink for visibility — plumbing failures and handler
        exceptions alike. The loop continues regardless; stopping is
        only via :meth:`close`."""
        self._on_error = handler
        return self

    def backoff(
        self, initial_s: float, max_s: float
    ) -> "FeedConsumer[K, V]":
        """Restart backoff bounds in seconds (default 0.25 -> 30)."""
        self._initial_backoff = initial_s
        self._max_backoff = max_s
        return self

    @property
    def last_error(self) -> Optional[BaseException]:
        """The last supervised failure (plumbing or handler)."""
        return self._last_error

    @property
    def is_running(self) -> bool:
        """True while the supervise loop is active."""
        return self._running

    def run(
        self, handler: Optional[Callable[[FeedMessage[K, V]], None]] = None
    ) -> None:
        """Run the consumer on the calling thread until :meth:`close`.
        Raises only for startup refusals — Kafka-style fail-fast."""
        handler = handler or self._on_record
        if handler is None:
            raise ValueError("on_record handler is required")
        self._stop.clear()
        self._thread = threading.current_thread()
        self._running = True
        backoff = self._initial_backoff
        try:
            while not self._stop.is_set():
                iter_error: List[Optional[BaseException]] = [None]

                def deliver(records: List[FeedMessage[K, V]]) -> None:
                    for record in records:
                        try:
                            handler(record)
                        except Exception as error:
                            self._last_error = error
                            iter_error[0] = error
                            raise

                sub = self._feed._subscribe_decoded(
                    self._reader, self._start, self._capacity,
                    deliver, None)
                self._current = sub
                try:
                    while sub.running and not self._stop.is_set():
                        time.sleep(self._POLL_S)
                finally:
                    sub.close()
                    self._current = None
                if self._stop.is_set():
                    break
                failure = sub.failure
                if isinstance(failure, Refused):
                    raise failure        # refused = fix the request — fail fast
                error = failure or iter_error[0]
                if error is None:
                    # Died without a wire error or handler exception —
                    # e.g. local name validation. Supervising a silent
                    # death loops forever; surface it instead.
                    raise WhitewaterError(
                        0, None,
                        f"subscription '{self._reader}' terminated "
                        "without an error to supervise")
                if error is not None:
                    try:
                        self._on_error(error)
                    except Exception:
                        pass             # error sinks never break supervision
                self._stop.wait(backoff)
                backoff = min(backoff * 2, self._max_backoff)
        finally:
            self._running = False
            current, self._current = self._current, None
            if current is not None:
                current.close()

    def run_async(
        self, handler: Optional[Callable[[FeedMessage[K, V]], None]] = None
    ) -> threading.Thread:
        """The same supervised consumer on its own non-daemon thread —
        for several consumers or non-blocking startup."""
        thread = threading.Thread(
            target=self.run, args=(handler,),
            name=f"whitewater-consumer-{self._reader}", daemon=False)
        thread.start()
        return thread

    def close(self) -> None:
        """Graceful stop: the in-flight page finishes and acks, the
        session closes, ``run()``/iteration return. Safe from a
        signal/atexit handler; joins the supervise thread so the
        consumer is fully stopped when this returns (unless called
        from inside a handler, where it only signals)."""
        self._stop.set()
        current = self._current
        me = threading.current_thread()
        if current is not None and current._thread is me:
            return            # handler-initiated close: signal only — the
                              # run loop's finally releases the session
        if current is not None:
            current.close()
        thread = self._thread
        if thread is not None and thread is not me:
            thread.join(timeout=30)
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=30)

    def __iter__(self):
        """Iterate decoded records forever — supervised like
        :meth:`run`. Breaking out (or a loop-body exception) closes the
        consumer; a :class:`Refused` startup error propagates."""
        messages: "queue.Queue" = queue.Queue()

        def supervise() -> None:
            try:
                self.run(messages.put)
            except BaseException as error:
                self._last_error = error
                messages.put(error)
            finally:
                messages.put(_SENTINEL)

        thread = threading.Thread(
            target=supervise,
            name=f"whitewater-consumer-{self._reader}", daemon=False)
        thread.start()
        try:
            while True:
                item = messages.get()
                if item is _SENTINEL:
                    return
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            self.close()
