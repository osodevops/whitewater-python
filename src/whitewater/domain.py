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

    reader = (client.create_reader(shop, shop.get_feed("orders"))
              .reader_id("fulfillment")          # the Kafka groupId analogue
              .reader_start_point(ReaderStart.BEGINNING)
              .capacity(256)
              .open())                          # FeedReader[str, bytes]

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
        (schema-neutral bytes)."""
        return self._snap().feed_schemas.get(feed)

    def declared_feed_schema(self, feed: str) -> Optional[FeedSchema]:
        """Only the sides the Feed itself declares — None when it
        inherits the Domain default wholesale."""
        return self._snap().declared_schemas.get(feed)

    def feeds(self, prefix: Optional[str] = None) -> List["Feed[str, bytes]"]:
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
            else _codecs.raw()
        )
        return Feed(self._client, self._resolve(relative), key_codec, value_codec)

    def get_feed(self, relative: str, **kwargs) -> "Feed":
        """Alias of :meth:`feed`."""
        return self.feed(relative, **kwargs)

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
    returned by ``client.create_reader(domain, feed)``.

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
        self._capacity = 128

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

    def capacity(self, capacity: int) -> "ReaderBuilder[K, V]":
        """Bounded fetch page capacity — the default is 128."""
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        return self

    def declare(self) -> ControlExecution:
        """Declare the Reader in the catalog without opening a session —
        fails when the name is already declared."""
        return self._client.create_reader(self._require_id(), self._feed.name, self._start)

    def open(self) -> "FeedReader[K, V]":
        """Declare the Reader when missing (an existing declaration keeps
        its start and durable progress), open a session, and bind the
        Feed's codecs — ``fetch`` returns decoded ``FeedMessage``s."""
        reader_id = self._require_id()
        self._feed._ensure_reader(reader_id, self._start)
        session_client = self._client.reader(reader_id)
        session = session_client.open(self._capacity)
        return FeedReader(
            session_client, session, self._feed.key_codec, self._feed.value_codec
        )

    def _require_id(self) -> str:
        if self._reader_id is None:
            raise ValueError(
                "reader_id(...) is required — the durable consume identity"
                " (the Kafka groupId analogue)"
            )
        return self._reader_id


class WriterBuilder(Generic[K, V]):
    """Fluent declaration + session-open of a durable named Writer —
    returned by ``client.create_writer(domain, feed)``."""

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
        return self._client.create_writer(self._require_id(), self._feed.name)

    def open(self) -> "FeedWriter[K, V]":
        """Declare the Writer when missing, open a session (the fencing
        epoch stays inside the handle), and bind the Feed's codecs."""
        writer_id = self._require_id()
        self._feed._ensure_writer(writer_id)
        session_epoch = self._feed._open_writer_session(writer_id)
        session = self._client.writer(writer_id, session_epoch)
        return FeedWriter(session, self._feed.key_codec, self._feed.value_codec)

    def _require_id(self) -> str:
        if self._writer_id is None:
            raise ValueError("writer_id(...) is required")
        return self._writer_id


def _as_feed(domain: Domain, feed) -> "Feed":
    """Coerce a ``Feed`` handle or relative name into a Feed inside
    ``domain`` — a Feed bound to another Domain fails locally."""
    bound = feed if isinstance(feed, Feed) else domain.feed(feed)
    if not bound.name.startswith(domain.name + "."):
        raise ValueError(
            f"feed '{bound.name}' is outside Domain '{domain.name}'"
        )
    return bound
