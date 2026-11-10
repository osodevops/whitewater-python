"""The strongly-typed Domain facade.

A Domain owns every Feed it prefixes (``shop`` owns ``shop.orders``), so
the friendliest surface navigates domain → feed → reader/writer instead
of composing dotted names by hand::

    from whitewater import ReaderStart

    shop = client.domain("shop")          # lazy handle — no I/O yet
    shop = client.get_domain("shop")      # verifies + materializes now

    shop.feed_names()                     # ['shop.orders', 'shop.returns']
    shop.feeds("order")                   # prefix-filtered Feed handles
    shop.readers(); shop.writers()

    reader = (shop.create_reader("orders")
              .reader_id("fulfillment")          # the Kafka groupId analogue
              .reader_start_point(ReaderStart.BEGINNING)
              .max_page_size(256)
              .open())                          # FeedReader[str, str]

The first catalog accessor issues ONE batched control read
(``describe domain`` + every ``show`` listing), joins rows by
``space_id``/``feed_id``, and caches the snapshot — ``refresh()``
re-pulls. ``domain.feed("orders")`` is pure name composition: zero I/O.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, TypeVar

from . import codecs as _codecs
from .client import _dotted
from .errors import Refused
from .feed import Feed, FeedReader, FeedWriter
from .models import ControlExecution, ReaderStart

if TYPE_CHECKING:  # pragma: no cover
    from .client import WhitewaterClient
    from .codecs import Codec

K = TypeVar("K")
V = TypeVar("V")


class SchemaDecl:
    """One side of a Feed's schema metadata — format plus optional
    subject/definition text. Formats: ``json``, ``jsonschema``,
    ``avro``, ``protobuf``, ``text``, ``bytes``, or ``custom.<name>``. Records stay schema-neutral bytes on
    the wire; declarations are catalog metadata for tooling and
    compatibility — not yet an enforced registry."""

    __slots__ = ("format", "subject", "definition")

    def __init__(self, format: str, subject: Optional[str] = None,
                 definition: Optional[str] = None):
        self.format = format
        self.subject = subject
        self.definition = definition

    def wire(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"format": self.format}
        if self.subject is not None:
            out["subject"] = self.subject
        if self.definition is not None:
            out["definition"] = self.definition
        return out

    @staticmethod
    def _from(row) -> Optional["SchemaDecl"]:
        if not isinstance(row, dict):
            return None
        return SchemaDecl(row.get("format"), row.get("subject"),
                          row.get("definition"))

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"SchemaDecl(format={self.format!r}, subject={self.subject!r})"


class FeedSchema:
    """The declared key/value schema pair — either side may be None.
    A Feed's declaration overrides the named sides of its Domain's
    default."""

    __slots__ = ("key", "value")

    def __init__(self, key: Optional[SchemaDecl] = None,
                 value: Optional[SchemaDecl] = None):
        self.key = key
        self.value = value

    def wire(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.key is not None:
            out["key"] = self.key.wire()
        if self.value is not None:
            out["value"] = self.value.wire()
        return out

    @staticmethod
    def _from(row) -> Optional["FeedSchema"]:
        if not isinstance(row, dict):
            return None
        key, value = SchemaDecl._from(row.get("key")), SchemaDecl._from(row.get("value"))
        if key is None and value is None:
            return None
        return FeedSchema(key, value)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"FeedSchema(key={self.key!r}, value={self.value!r})"


class Domain:
    """A strongly-typed handle on one Domain — see module docs.

    ``client.domain(name)`` returns a lazy handle; ``client.get_domain(name)``
    verifies the Domain exists and materializes its catalog view
    immediately. Catalog accessors share one cached snapshot; ``feed()``
    name composition never performs I/O.
    """

    def __init__(self, client: "WhitewaterClient", name: str):
        self._client = client
        self._name = _dotted(name)
        self._lock = threading.Lock()
        self._snapshot: Optional["_Snapshot"] = None

    @property
    def name(self) -> str:
        """The Domain's dotted name."""
        return self._name

    @property
    def domain_id(self) -> str:
        """The Domain's immutable id — survives renames."""
        return self._snap().domain_id

    @property
    def revision(self) -> int:
        """The catalog revision the snapshot was taken at."""
        return self._snap().revision

    def describe(self) -> Dict[str, Any]:
        """The raw ``DESCRIBE DOMAIN`` row — status, created_at_ns,
        history_policy."""
        return self._snap().describe

    @property
    def history_policy(self) -> Optional[Dict[str, Any]]:
        """The declared History Policy row, or None when unset."""
        return self._snap().describe.get("history_policy")

    # ---- scoped resources -------------------------------------------------

    def schema(self) -> Optional[FeedSchema]:
        """The Domain's declared schema default — None when undeclared.
        Every Feed without its own declaration inherits it."""
        return FeedSchema._from(self._snap().describe.get("schema"))

    def feed_schema(self, feed: str) -> Optional[FeedSchema]:
        """The effective schema for a Feed — its declared sides layered
        over the Domain default. None when nothing is declared anywhere
        (schema-neutral bytes). Resolves ``feed`` inside this Domain."""
        return self._snap().feed_schemas.get(self._resolve(feed))

    def declared_feed_schema(self, feed: str) -> Optional[FeedSchema]:
        """Only the sides the Feed itself declares — None when it
        inherits the Domain default wholesale. Resolves ``feed`` inside
        this Domain."""
        return self._snap().declared_schemas.get(self._resolve(feed))

    def feeds(self, prefix: Optional[str] = None) -> List["Feed[str, str]"]:
        """Every Feed in this Domain as typed :class:`Feed` handles,
        sorted by name. ``prefix`` filters on the relative name
        (``shop.orders`` → ``orders``)."""
        return [
            self.feed(self._relative(full))
            for full in self._snap().feeds
            if prefix is None or self._relative(full).startswith(prefix)
        ]

    def feed_names(self) -> List[str]:
        """The Feed names in this Domain, sorted."""
        return list(self._snap().feeds)

    def readers(self) -> List[str]:
        """Declared Readers on this Domain's Feeds, sorted."""
        return list(self._snap().readers)

    def writers(self) -> List[str]:
        """Declared Writers on this Domain's Feeds, sorted."""
        return list(self._snap().writers)

    def subscriptions(self) -> List[str]:
        """Declared Subscriptions in this Domain, sorted."""
        return list(self._snap().subscriptions)

    def pipes(self) -> List[str]:
        """Declared Pipes in this Domain, sorted."""
        return list(self._snap().pipes)

    def indexes(self) -> List[str]:
        """Declared Indexes in this Domain, sorted."""
        return list(self._snap().indexes)

    def state_stores(self) -> List[str]:
        """Declared StateStores in this Domain, sorted."""
        return list(self._snap().state_stores)

    def refresh(self) -> "Domain":
        """Re-pull the catalog snapshot — the next accessor sees fresh
        rows. Returns ``self`` for chaining."""
        with self._lock:
            self._snapshot = self._load()
        return self

    # ---- Feed handles ------------------------------------------------------

    def feed(
        self,
        relative: str,
        *,
        key_codec=None,
        value_codec=None,
        key_type=None,
        value_type=None,
    ) -> "Feed":
        """A typed :class:`Feed` handle on ``relative`` inside this
        Domain: ``shop.feed("orders")`` binds ``shop.orders``. An
        already-qualified name (``"shop.orders"``) is accepted as-is.
        Pure name composition — no I/O. Codecs bind exactly like
        :meth:`WhitewaterClient.feed`.
        """
        key_codec = key_codec or (
            _codecs.for_type(key_type) if key_type is not None else _codecs.utf8()
        )
        value_codec = value_codec or (
            _codecs.for_type(value_type)
            if value_type is not None
            else _codecs.utf8()
        )
        return Feed(self._client, self._resolve(relative), key_codec, value_codec)

    def get_feed(self, relative: str, **kwargs) -> "Feed":
        """Alias of :meth:`feed`."""
        return self.feed(relative, **kwargs)

    def create_feed(self, relative: str) -> ControlExecution:
        """Execute ``CREATE FEED`` inside this Domain —
        ``shop.create_feed("orders")`` declares ``shop.orders``.
        :meth:`refresh` (or a lazy accessor's first load) surfaces it
        through :meth:`feeds`/:meth:`feed_names`.
        """
        return self._client._create_feed(self._resolve(relative))

    def create_reader(self, name_or_feed, feed=None,
                      start: Optional[ReaderStart] = None):
        """``shop.create_reader("orders")`` returns a fluent
        :class:`ReaderBuilder` bound to ``shop.orders``::

            reader = (shop.create_reader("orders")
                      .reader_id("fulfillment")      # Kafka groupId analogue
                      .reader_start_point(ReaderStart.BEGINNING)
                      .open())

        ``shop.create_reader("audit", "orders")`` executes the raw
        ``CREATE READER shop.audit FROM shop.orders`` statement instead —
        the feed argument follows :func:`_scoped`, so a ``Feed`` handle
        or ``Scoped.absolute`` binds a Reader across Domains.
        """
        if feed is None:
            return ReaderBuilder(self._client, self, _as_feed(self, name_or_feed))
        return self._client._create_reader(
            self._resolve(name_or_feed), _scoped(self, feed),
            start or ReaderStart.beginning())

    def create_writer(self, name_or_feed, feed=None):
        """``shop.create_writer("orders")`` returns a fluent
        :class:`WriterBuilder`; ``shop.create_writer("ingest", "orders")``
        executes ``CREATE WRITER shop.ingest TO shop.orders``. The feed
        argument follows :func:`_scoped` like :meth:`create_reader`."""
        if feed is None:
            return WriterBuilder(self._client, self, _as_feed(self, name_or_feed))
        return self._client._create_writer(
            self._resolve(name_or_feed), _scoped(self, feed))

    def create_subscription(
        self, name: str, feed: str, start: Optional[ReaderStart] = None
    ) -> ControlExecution:
        """``CREATE SUBSCRIPTION <name> FROM <feed>`` — the feed follows
        :func:`_scoped`: relative in-domain, a handle or
        ``Scoped.absolute`` across Domains."""
        return self._client._create_subscription(
            self._resolve(name), _scoped(self, feed),
            start or ReaderStart.beginning())

    # ---- session-bound resources -------------------------------------------

    def writer(self, name: str, session_epoch: int) -> "WriterSessionClient":
        """The raw writer-session client bound to ``name`` inside this
        Domain (epoch from :meth:`open_writer_session`)."""
        return self._client._writer(self._resolve(name), session_epoch)

    def writer_session(self, name: str, session_epoch: int) -> "WriterSessionClient":
        """Alias of :meth:`writer`."""
        return self.writer(name, session_epoch)

    def reader(self, name: str) -> "ReaderSessionClient":
        """The raw reader-session client bound to ``name`` inside this
        Domain — ``open``/``fetch``/``ack``/``close`` with explicit
        session state."""
        return self._client._reader(self._resolve(name))

    def reader_session(self, name: str) -> "ReaderSessionClient":
        """Alias of :meth:`reader`."""
        return self.reader(name)

    def member_client(self, subscription: str) -> "SubscriptionMemberClient":
        """The raw subscription-member client bound to ``subscription``
        inside this Domain — explicit member/lease epoch plumbing."""
        return self._client._subscription_member(self._resolve(subscription))

    def subscription(self, relative: str) -> "SubscriptionRef":
        """A :class:`SubscriptionRef` handle naming a Subscription inside
        this Domain — pure name composition, no I/O."""
        return SubscriptionRef(self._client, self._resolve(relative))

    def subscribe(
        self,
        reader: str,
        feed: str,
        start: ReaderStart,
        on_records,
        on_error=None,
        max_page_size: Optional[int] = None,
    ) -> "Subscription":
        """Managed durable consume inside this Domain: ensures the Reader
        exists, runs open → fetch → deliver → ack on a background thread."""
        return self._client._subscribe(
            self._resolve(reader), _scoped(self, feed), start,
            on_records, on_error, max_page_size)

    def open_writer_session(self, writer: str) -> ControlExecution:
        """``OPEN WRITER SESSION <writer>`` inside this Domain."""
        return self._client._open_writer_session(self._resolve(writer))

    def seek_reader(self, reader: str, start: "ReaderStart") -> ControlExecution:
        """``SEEK READER <reader> TO <start>`` inside this Domain."""
        return self._client._seek_reader(self._resolve(reader), start)

    # ---- catalog lifecycle --------------------------------------------------

    def describe_feed(self, relative: str) -> ControlExecution:
        """``DESCRIBE FEED <feed>`` inside this Domain."""
        return self._client._describe_feed(self._resolve(relative))

    def describe_writer(self, relative: str) -> ControlExecution:
        """``DESCRIBE WRITER <writer>`` inside this Domain."""
        return self._client._describe_writer(self._resolve(relative))

    def describe_reader(self, relative: str) -> ControlExecution:
        """``DESCRIBE READER <reader>`` inside this Domain."""
        return self._client._describe_reader(self._resolve(relative))

    def describe_subscription(self, relative: str) -> ControlExecution:
        """``DESCRIBE SUBSCRIPTION <subscription>`` inside this Domain."""
        return self._client._describe_subscription(self._resolve(relative))

    def rename_feed(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME FEED`` — both names resolve inside this Domain."""
        return self._client._rename_feed(
            self._resolve(current), self._resolve(renamed))

    def rename_writer(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME WRITER`` — both names resolve inside this Domain."""
        return self._client._rename_writer(
            self._resolve(current), self._resolve(renamed))

    def rename_reader(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME READER`` — both names resolve inside this Domain."""
        return self._client._rename_reader(
            self._resolve(current), self._resolve(renamed))

    def rename_subscription(self, current: str, renamed: str) -> ControlExecution:
        """``RENAME SUBSCRIPTION`` — both names resolve inside this Domain."""
        return self._client._rename_subscription(
            self._resolve(current), self._resolve(renamed))

    def drop_feed(self, relative: str) -> ControlExecution:
        """``DROP FEED <feed>`` inside this Domain."""
        return self._client._drop_feed(self._resolve(relative))

    def drop_writer(self, relative: str) -> ControlExecution:
        """``DROP WRITER <writer>`` inside this Domain."""
        return self._client._drop_writer(self._resolve(relative))

    def drop_reader(self, relative: str) -> ControlExecution:
        """``DROP READER <reader>`` inside this Domain."""
        return self._client._drop_reader(self._resolve(relative))

    def drop_subscription(self, relative: str) -> ControlExecution:
        """``DROP SUBSCRIPTION <subscription>`` inside this Domain."""
        return self._client._drop_subscription(self._resolve(relative))

    # ---- schemas --------------------------------------------------------------

    def set_schema(self, schema) -> ControlExecution:
        """``SET SCHEMA ON DOMAIN <domain>`` — the inherited default."""
        return self._client.set_domain_schema(self._name, schema)

    def clear_schema(self) -> ControlExecution:
        """``DROP SCHEMA ON DOMAIN <domain>``."""
        return self._client.clear_domain_schema(self._name)

    def set_feed_schema(self, relative: str, schema) -> ControlExecution:
        """``SET SCHEMA ON FEED <feed>`` inside this Domain."""
        return self._client._set_feed_schema(self._resolve(relative), schema)

    def clear_feed_schema(self, relative: str) -> ControlExecution:
        """``DROP SCHEMA ON FEED <feed>`` inside this Domain."""
        return self._client._clear_feed_schema(self._resolve(relative))

    # ---- pipes ------------------------------------------------------------------

    def pipe(self, relative: str) -> "Pipe":
        """A declared Pipe's lifecycle handle — ``describe``/``pause``/
        ``resume``/``drop`` — inside this Domain."""
        return self._client._pipe(self._resolve(relative))

    def pipe_infos(self) -> List["PipeInfo"]:
        """``SHOW PIPES`` rows scoped to this Domain."""
        prefix = self._name + "."
        return [info for info in self._client._pipes()
                if info.name.startswith(prefix)]

    def define_forward_pipe(
        self, name: str, subscription, output_feed
    ) -> ControlExecution:
        """``DEFINE PIPE <name> FROM <subscription> TO FEED <feed>`` —
        strings resolve inside this Domain; a ``Feed``/
        ``SubscriptionRef`` handle or :meth:`Scoped.absolute` points the
        endpoint across Domains."""
        return self._client._define_forward_pipe(
            self._resolve(name), _scoped(self, subscription),
            _scoped(self, output_feed))

    def define_enrich_pipe(
        self, name: str, subscription, output_feed, store,
        operation, quarantine_feed=None,
    ) -> ControlExecution:
        """``DEFINE PIPE ... ENRICH FROM <store>`` — every endpoint
        follows :func:`_scoped`: strings in-domain, handles/absolute
        cross-Domain."""
        return self._client._define_enrich_pipe(
            self._resolve(name), _scoped(self, subscription),
            _scoped(self, output_feed), _scoped(self, store), operation,
            _scoped(self, quarantine_feed) if quarantine_feed is not None else None)

    def define_window_pipe(
        self, name: str, subscription, output_store, operation,
        emit_feed=None,
    ) -> ControlExecution:
        """``DEFINE PIPE ... TO STATESTORE <store>`` — every endpoint
        follows :func:`_scoped`."""
        return self._client._define_window_pipe(
            self._resolve(name), _scoped(self, subscription),
            _scoped(self, output_store), operation,
            _scoped(self, emit_feed) if emit_feed is not None else None)

    # ---- state stores -----------------------------------------------------------

    def state_store(self, relative: str, key_codec=None, value_codec=None,
                    *, key_type=None, value_type=None) -> "StateStore":
        """A typed ``StateStore`` facade inside this Domain —
        ``shop.state_store("customers")`` binds ``shop.customers``."""
        return self._client._state_store(
            self._resolve(relative), key_codec, value_codec,
            key_type=key_type, value_type=value_type)

    def define_state_store(self, spec, source=None) -> "StateStore":
        """Declare ``spec`` inside this Domain — the spec's name
        resolves in-domain. ``source`` names the Feed the store
        materializes from (None = manual); a ``Feed`` handle or
        :meth:`Scoped.absolute` sources it across Domains. Returns the
        bound store facade."""
        # Rebind the spec's name into this Domain without touching keys.
        from .client import StateStore as _SS
        scoped_spec = _SS.named(self._resolve(spec.name))
        for key in spec.named_keys:
            scoped_spec.add_key(key.name, key.unique, key.extractor)
        return self._client._state_stores.define(
            scoped_spec,
            _scoped(self, source) if source is not None else None)

    def drop_state_store(self, relative: str) -> ControlExecution:
        """``DROP STATESTORE <name>`` inside this Domain."""
        return self._client._state_stores.drop(self._resolve(relative))

    # ---- indexes -----------------------------------------------------------------

    def query_index(self, relative: str, predicate, limit: Optional[int] = None,
                    strict: bool = False, wait_ms: Optional[int] = None) -> "IndexPage":
        """Query the replicated Index ``relative`` inside this Domain."""
        return self._client._query_index(
            self._resolve(relative), predicate, limit, strict, wait_ms)

    @property
    def feed_ops(self) -> "FeedClient":
        """The raw byte-level feed client — committed records, temporary
        fetches, ad-hoc appends by absolute name. The escape hatch under
        the Domain door; prefer bound :class:`Feed` handles."""
        return self._client._feeds

    def scoped(self, relative: str) -> "Scoped":
        """A :class:`Scoped` reference naming a resource inside this
        Domain — pass to Pipe definitions to make resolution explicit."""
        return Scoped.relative(relative)

    def resolve(self, relative: str) -> str:
        """The full wire name of ``relative`` inside this Domain —
        ``shop.resolve("orders.hourly")`` → ``shop.orders.hourly``."""
        return self._resolve(relative)

    def _resolve(self, relative: str) -> str:
        if relative is None:
            raise TypeError("feed name is required")
        full = relative if relative.startswith(self._name + ".") else f"{self._name}.{relative}"
        return _dotted(full)

    def _relative(self, full: str) -> str:
        return full[len(self._name) + 1 :] if full.startswith(self._name + ".") else full

    # ---- snapshot loading --------------------------------------------------

    def _snap(self) -> "_Snapshot":
        snap = self._snapshot
        if snap is None:
            with self._lock:
                snap = self._snapshot
                if snap is None:
                    snap = self._load()
                    self._snapshot = snap
        return snap

    def _load(self) -> "_Snapshot":
        """One batched control read: ``describe domain`` plus every
        ``show`` listing. Rows join on ``space_id`` — readers/writers
        join through their Feed's ``feed_id``. A missing Domain fails
        the whole batch server-side."""
        execution = self._client.commands(
            [
                {"command": "describe", "kind": "domain", "name": self._name},
                {"command": "show", "kind": "feeds"},
                {"command": "show", "kind": "readers"},
                {"command": "show", "kind": "writers"},
                {"command": "show", "kind": "subscriptions"},
                {"command": "show", "kind": "pipes"},
                {"command": "show", "kind": "indexes"},
                {"command": "show", "kind": "state_stores"},
            ]
        )
        results = execution.results
        describe = results[0].data or {}
        domain_id = describe.get("space_id")
        if not isinstance(domain_id, str) or not domain_id:
            raise Refused(404, "domain.unknown", f"Domain '{self._name}' is not declared")

        feeds: List[str] = []
        feed_ids = set()
        feed_schemas: Dict[str, Optional[FeedSchema]] = {}
        declared_schemas: Dict[str, Optional[FeedSchema]] = {}
        for row in results[1].data or []:
            if row.get("space_id") == domain_id:
                feeds.append(row["name"])
                feed_ids.add(row["feed_id"])
                feed_schemas[row["name"]] = FeedSchema._from(row.get("effective_schema"))
                declared_schemas[row["name"]] = FeedSchema._from(row.get("schema"))
        feeds.sort()

        def scoped(index: int, field: str, ids) -> List[str]:
            names = [
                row["name"]
                for row in (results[index].data or [])
                if row.get(field) in ids and row.get("name")
            ]
            names.sort()
            return names

        return _Snapshot(
            domain_id=domain_id,
            describe=describe,
            revision=execution.revision,
            feeds=feeds,
            feed_schemas=feed_schemas,
            declared_schemas=declared_schemas,
            readers=scoped(2, "feed_id", feed_ids),
            writers=scoped(3, "feed_id", feed_ids),
            subscriptions=scoped(4, "space_id", {domain_id}),
            pipes=scoped(5, "space_id", {domain_id}),
            indexes=scoped(6, "space_id", {domain_id}),
            state_stores=scoped(7, "space_id", {domain_id}),
        )


class _Snapshot:
    __slots__ = (
        "domain_id",
        "describe",
        "revision",
        "feeds",
        "feed_schemas",
        "declared_schemas",
        "readers",
        "writers",
        "subscriptions",
        "pipes",
        "indexes",
        "state_stores",
    )

    def __init__(self, **fields):
        for name in self.__slots__:
            setattr(self, name, fields[name])


class ReaderBuilder(Generic[K, V]):
    """Fluent declaration + session-open of a durable named Reader —
    returned by ``domain.create_reader(feed)``.

    ``reader_id`` is required: it is the durable consume identity (the
    Kafka ``groupId`` analogue). ``reader_start_point`` applies only
    when the Reader is first declared — an existing Reader resumes its
    acknowledged frontier (the ``auto.offset.reset`` mapping).
    """

    def __init__(self, client: "WhitewaterClient", domain: Domain, feed: "Feed[K, V]"):
        self._client = client
        self._domain = domain
        self._feed = feed
        self._reader_id: Optional[str] = None
        self._start = ReaderStart.beginning()
        self._max_page_size = 128

    def reader_id(self, reader_id: str) -> "ReaderBuilder[K, V]":
        """The durable Reader identity. Required before open/declare."""
        self._reader_id = reader_id
        return self

    def reader_start_point(self, start: ReaderStart) -> "ReaderBuilder[K, V]":
        """First-time start position — ``ReaderStart.BEGINNING``,
        ``.END``, ``cursor(c)``, or ``timestamp(instant)``."""
        if start is None:
            raise TypeError("start is required")
        self._start = start
        return self

    def max_page_size(self, max_page_size: int) -> "ReaderBuilder[K, V]":
        """Max records delivered per fetch page — the default is 128."""
        if max_page_size <= 0:
            raise ValueError("max_page_size must be positive")
        self._max_page_size = max_page_size
        return self

    def declare(self) -> ControlExecution:
        """Declare the Reader in the catalog without opening a session —
        fails when the name is already declared."""
        return self._client._create_reader(
            self._require_id(), self._feed.name, self._start)

    def open(self) -> "FeedReader[K, V]":
        """Declare the Reader when missing (an existing declaration keeps
        its start and durable progress), open a session, and bind the
        Feed's codecs — ``fetch`` returns decoded ``FeedMessage``s."""
        reader_id = self._require_id()
        self._feed._ensure_reader(reader_id, self._start)
        session_client = self._client._reader(reader_id)
        session = session_client.open(self._max_page_size)
        return FeedReader(
            session_client, session, self._feed.key_codec, self._feed.value_codec
        )

    def _require_id(self) -> str:
        if self._reader_id is None:
            raise ValueError(
                "reader_id(...) is required — the durable consume identity"
                " (the Kafka groupId analogue)"
            )
        # Reader identities are Domain resources — "fulfillment" inside
        # "shop" declares shop.fulfillment, never a global name.
        return self._domain._resolve(self._reader_id)


class WriterBuilder(Generic[K, V]):
    """Fluent declaration + session-open of a durable named Writer —
    returned by ``domain.create_writer(feed)``."""

    def __init__(self, client: "WhitewaterClient", domain: Domain, feed: "Feed[K, V]"):
        self._client = client
        self._domain = domain
        self._feed = feed
        self._writer_id: Optional[str] = None

    def writer_id(self, writer_id: str) -> "WriterBuilder[K, V]":
        """The durable Writer identity. Required before open/declare."""
        self._writer_id = writer_id
        return self

    def declare(self) -> ControlExecution:
        """Declare the Writer in the catalog without opening a session —
        fails when the name is already declared."""
        return self._client._create_writer(
            self._require_id(), self._feed.name)

    def open(self) -> "FeedWriter[K, V]":
        """Declare the Writer when missing, open a session (the fencing
        epoch stays inside the handle), and bind the Feed's codecs."""
        writer_id = self._require_id()
        self._feed._ensure_writer(writer_id)
        session_epoch = self._feed._open_writer_session(writer_id)
        session = self._client._writer(writer_id, session_epoch)
        return FeedWriter(session, self._feed.key_codec, self._feed.value_codec)

    def _require_id(self) -> str:
        if self._writer_id is None:
            raise ValueError("writer_id(...) is required")
        return self._domain._resolve(self._writer_id)


def _as_feed(domain: Domain, feed) -> "Feed":
    """Coerce a ``Feed`` handle or relative name into a Feed inside
    ``domain`` — a Feed bound to another Domain fails locally."""
    bound = feed if isinstance(feed, Feed) else domain.feed(feed)
    if not bound.name.startswith(domain.name + "."):
        raise ValueError(
            f"feed '{bound.name}' is outside Domain '{domain.name}'"
        )
    return bound


class Scoped:
    """An explicit reference to a resource that may live outside the
    declaring Domain — the Pipe-endpoint argument type.

    ``Scoped.relative("orders.sub")`` resolves inside the Domain (same as
    a plain string); ``Scoped.absolute("warehouse.inbound")`` carries a
    fully-qualified cross-Domain name verbatim. ``Feed``,
    ``StateStore``, and :class:`SubscriptionRef` handles count as
    absolute references — they carry their full dotted name.
    """

    __slots__ = ("_name", "_absolute")

    def __init__(self, name: str, absolute: bool):
        self._name = name
        self._absolute = absolute

    @staticmethod
    def relative(name: str) -> "Scoped":
        """Resolve ``name`` inside the declaring Domain."""
        return Scoped(name, False)

    @staticmethod
    def absolute(name: str) -> "Scoped":
        """Carry the fully-qualified ``name`` verbatim — cross-Domain."""
        return Scoped(name, True)

    def resolve(self, domain: "Domain") -> str:
        return _dotted(self._name) if self._absolute else domain._resolve(self._name)


class SubscriptionRef:
    """A handle naming a Subscription — the pipe-endpoint reference that
    carries an absolute name; also exposes the raw member client."""

    __slots__ = ("_client", "_name")

    def __init__(self, client: "WhitewaterClient", name: str):
        self._client = client
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def member_client(self) -> "SubscriptionMemberClient":
        """The raw member client — explicit member/lease epoch plumbing."""
        return self._client._subscription_member(self._name)


def _scoped(domain: Domain, arg) -> str:
    """Resolve a Pipe endpoint: ``str``/``Scoped.relative`` inside the
    Domain; ``Scoped.absolute``/a ``name``-carrying handle verbatim."""
    if isinstance(arg, Scoped):
        return arg.resolve(domain)
    if isinstance(arg, str):
        return domain._resolve(arg)
    name = getattr(arg, "name", None)
    if isinstance(name, str) and name:
        return name
    raise TypeError(
        "a Pipe endpoint is a name (resolves in-domain), a "
        "Feed/StateStore/SubscriptionRef handle, or Scoped.absolute")
