"""Operational surfaces: Pipe lifecycle, Index queries, and demand
observability — the same topology-free model as Feeds (names, never
partitions or Nodes)."""

from __future__ import annotations

import base64
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

from .models import Extractor

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .client import WhitewaterClient
    from .models import ControlExecution


# ---------------------------------------------------------------------------
# Pipes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipeInfo:
    """The catalog row ``DESCRIBE PIPE``/``SHOW PIPES`` returns for a
    declared Pipe. ``operation`` stays an opaque dict — Forward, Window,
    and Enrich variants evolve server-side without an SDK bump."""

    name: str
    #: ``declared`` while driven, ``paused`` under ``PAUSE PIPE``.
    stage: str
    subscription_id: uuid.UUID
    operation: Dict[str, Any]
    output_feed_id: Optional[uuid.UUID]
    output_store_id: Optional[uuid.UUID]
    status: str
    created_at_ns: int


def _pipe_uuid(row: Dict[str, Any], key: str) -> Optional[uuid.UUID]:
    raw = row.get(key)
    if not isinstance(raw, str):
        return None
    try:
        return uuid.UUID(raw)
    except ValueError:
        return None


def _decode_pipe(row: Dict[str, Any]) -> Optional[PipeInfo]:
    name = row.get("name")
    stage = row.get("stage")
    subscription = _pipe_uuid(row, "subscription_id")
    status = row.get("status")
    created = row.get("created_at_ns")
    if not isinstance(name, str) or subscription is None:
        return None
    if not isinstance(stage, str) or not isinstance(status, str):
        return None
    return PipeInfo(
        name=name,
        stage=stage,
        subscription_id=subscription,
        operation=row.get("operation") or {},
        output_feed_id=_pipe_uuid(row, "output_feed_id"),
        output_store_id=_pipe_uuid(row, "output_store_id"),
        status=status,
        created_at_ns=int(created) if isinstance(created, (int, float)) else 0,
    )


def decode_pipes(rows: Any) -> List[PipeInfo]:
    if not isinstance(rows, list):
        rows = [rows]
    return [info for row in rows if isinstance(row, dict)
            for info in [_decode_pipe(row)] if info is not None]


class Pipe:
    """A declared Pipe's lifecycle surface.

    Pausing is cooperative — an in-flight effect finishes and the
    frontier stays put so redelivery stays replay-safe — and resume
    restarts driving from the same durable frontier.
    """

    def __init__(self, client: "WhitewaterClient", name: str):
        self._client = client
        self.name = name

    def describe(self) -> PipeInfo:
        """``DESCRIBE PIPE``: the live catalog row including stage and
        the opaque operation definition."""
        execution = self._client.commands([
            {"command": "describe", "kind": "pipe", "name": self.name},
        ])
        data = execution.results[0].data if execution.results else {}
        pipes = decode_pipes([data])
        if not pipes:
            from .errors import WhitewaterError
            raise WhitewaterError("describe pipe returned no definition")
        return pipes[0]

    def pause(self) -> None:
        """``PAUSE PIPE``: halts every drive cooperatively; idempotent."""
        self._lifecycle("pause_pipe")

    def resume(self) -> None:
        """``RESUME PIPE``: restarts driving from the durable frontier."""
        self._lifecycle("resume_pipe")

    def drop(self) -> None:
        """``DROP PIPE``: retires the declaration and its compute lease."""
        self._client.commands([
            {"command": "drop", "kind": "pipe", "name": self.name},
        ])

    def _lifecycle(self, command: str) -> None:
        self._client.commands([{"command": command, "name": self.name}])


# ---- Pipe declaration ------------------------------------------------------
#
# ``define_pipe`` declares the managed processing step a Pipe applies to
# every record its Subscription delivers — Forward copies, Enrich
# point-looks-up a StateStore row, Window accumulates fixed panes. The
# types below build the tagged ``operation`` object verbatim; the server
# owns the drive, so a declaration carries names only — never partitions
# or Nodes.


class WindowTime(str, Enum):
    """Which record clock a ``window`` Pipe buckets into panes."""

    #: ``event_time_ns`` — the application-stamped occurrence clock (default).
    EVENT = "event"
    #: ``ingest_time_ns`` — the durable-arrival clock.
    INGEST = "ingest"


class EnrichFreshness(str, Enum):
    """How fresh an ``enrich`` Pipe lookup row must be before it is
    journaled."""

    #: Serve the replica's applied state (default; the resolution is
    #: journaled either way, so replayed outputs are byte-identical).
    LATEST = "latest"
    #: Require applied to cover committed at read entry; lag yields a
    #: retryable drive failure.
    STRICT = "strict"


class EnrichMiss(str, Enum):
    """What an ``enrich`` Pipe lookup miss does to the output record."""

    #: Forward the record with ``enrich.matched = false`` (default).
    FORWARD = "forward"
    #: Consume without emitting; the input frontier still commits.
    DROP = "drop"
    #: Route the miss to the declared quarantine Feed.
    QUARANTINE = "quarantine"


def _require_extractor(value: Any, where: str) -> Extractor:
    if not isinstance(value, Extractor):
        raise TypeError(
            f"{where} requires an Extractor — "
            "Extractor.record_key()/metadata()/json()"
        )
    return value


@dataclass(frozen=True)
class WindowAggregation:
    """What a ``window`` Pipe adds to each pane's accumulator: ``count``
    records, ``sum`` an extracted integer, or ``average`` tracking
    count+sum (readers divide). Build with the class factories."""

    aggregation: str
    value: Optional[Extractor] = None

    @staticmethod
    def count() -> "WindowAggregation":
        """Count records per (group Key, pane)."""
        return WindowAggregation("count")

    @staticmethod
    def sum(value: Extractor) -> "WindowAggregation":
        """Sum one extracted integer value per pane."""
        return WindowAggregation("sum", _require_extractor(value, "sum value"))

    @staticmethod
    def average(value: Extractor) -> "WindowAggregation":
        """Track count and integer sum per pane; readers divide."""
        return WindowAggregation(
            "average", _require_extractor(value, "average value"))

    def to_wire(self) -> Dict[str, Any]:
        """The wire form: ``{"aggregation": "sum", "value": {...}}``."""
        node: Dict[str, Any] = {"aggregation": self.aggregation}
        if self.value is not None:
            node["value"] = self.value.to_wire()
        return node


@dataclass(frozen=True)
class PipeOperation:
    """The processing step a Pipe applies to each delivered input record.
    Build with :meth:`forward`, :meth:`enrich`, or :meth:`window`; the
    wire form is internally tagged on ``operation``."""

    operation: str
    key: Optional[Extractor] = None
    freshness: Union[EnrichFreshness, str] = EnrichFreshness.LATEST
    named: Optional[str] = None
    on_miss: Union[EnrichMiss, str] = EnrichMiss.FORWARD
    window_ns: Optional[int] = None
    aggregation: Optional[WindowAggregation] = None
    time: Union[WindowTime, str] = WindowTime.EVENT
    max_late_ns: Optional[int] = None
    watermark_ns: Optional[int] = None
    grace_ns: Optional[int] = None
    expire_ns: Optional[int] = None
    key_limit: Optional[int] = None
    byte_limit: Optional[int] = None

    @staticmethod
    def forward() -> "PipeOperation":
        """Copy each delivered record to the output Feed unchanged."""
        return PipeOperation("forward")

    @staticmethod
    def enrich(
        key: Extractor,
        freshness: Union[EnrichFreshness, str] = EnrichFreshness.LATEST,
        named: Optional[str] = None,
        on_miss: Union[EnrichMiss, str] = EnrichMiss.FORWARD,
    ) -> "PipeOperation":
        """Point-lookup enrichment against the output StateStore: resolve
        one row by the projected ``key`` and copy its payload into output
        Metadata (``enrich.payload``, ``enrich.matched``). The lookup
        resolution rides inside the effect journal, so replayed outputs
        are byte-identical."""
        return PipeOperation(
            "enrich",
            key=_require_extractor(key, "enrich key"),
            freshness=EnrichFreshness(freshness),
            named=named,
            on_miss=EnrichMiss(on_miss),
        )

    @staticmethod
    def window(
        window_ns: int,
        key: Extractor,
        aggregation: WindowAggregation,
        *,
        time: Union[WindowTime, str] = WindowTime.EVENT,
        max_late_ns: Optional[int] = None,
        watermark_ns: Optional[int] = None,
        grace_ns: Optional[int] = None,
        expire_ns: Optional[int] = None,
        key_limit: Optional[int] = None,
        byte_limit: Optional[int] = None,
    ) -> "PipeOperation":
        """Accumulate each delivered record into fixed ``window_ns``-wide
        panes of a managed StateStore view. Panes are immutable once
        written; ``max_late_ns`` bounds late-event policy (later arrivals
        quarantine into a countable ``late`` row) and ``expire_ns`` bounds
        pane retention once the pane provably closes."""
        if isinstance(window_ns, bool) or not isinstance(window_ns, int):
            raise TypeError("window_ns must be an int of nanoseconds")
        if window_ns <= 0:
            raise ValueError("window_ns must be positive")
        if not isinstance(aggregation, WindowAggregation):
            raise TypeError(
                "window aggregation is required — "
                "WindowAggregation.count()/sum()/average()"
            )
        return PipeOperation(
            "window",
            key=_require_extractor(key, "window key"),
            window_ns=window_ns,
            aggregation=aggregation,
            time=WindowTime(time),
            max_late_ns=max_late_ns,
            watermark_ns=watermark_ns,
            grace_ns=grace_ns,
            expire_ns=expire_ns,
            key_limit=key_limit,
            byte_limit=byte_limit,
        )

    def to_wire(self) -> Dict[str, Any]:
        """The tagged ``operation`` object ``define_pipe`` carries."""
        node: Dict[str, Any] = {"operation": self.operation}
        if self.key is not None:
            node["key"] = self.key.to_wire()
        if self.window_ns is not None:
            node["window_ns"] = self.window_ns
        if self.aggregation is not None:
            node["aggregation"] = self.aggregation.to_wire()
        if self.operation == "window":
            node["time"] = WindowTime(self.time).value
        if self.max_late_ns is not None:
            node["max_late_ns"] = self.max_late_ns
        if self.watermark_ns is not None:
            node["watermark_ns"] = self.watermark_ns
        if self.grace_ns is not None:
            node["grace_ns"] = self.grace_ns
        if self.expire_ns is not None:
            node["expire_ns"] = self.expire_ns
        if self.key_limit is not None:
            node["key_limit"] = self.key_limit
        if self.byte_limit is not None:
            node["byte_limit"] = self.byte_limit
        if self.operation == "enrich":
            node["freshness"] = EnrichFreshness(self.freshness).value
            if self.named is not None:
                node["named"] = self.named
            node["on_miss"] = EnrichMiss(self.on_miss).value
        return node


# ---------------------------------------------------------------------------
# Index queries
# ---------------------------------------------------------------------------


def wire_typed(value: Any) -> Dict[str, Any]:
    """Lower a Python value onto the index wire's typed-value shape."""
    if value is None:
        return {"kind": "null"}
    if isinstance(value, bool):
        return {"kind": "bool", "value": value}
    if isinstance(value, int):
        return {"kind": "i64", "value": value}
    if isinstance(value, str):
        return {"kind": "text", "value": value}
    if isinstance(value, (bytes, bytearray)):
        return {"kind": "bytes",
                "value_base64": base64.b64encode(bytes(value)).decode("ascii")}
    raise TypeError(f"unsupported index value type: {type(value).__name__}")


@dataclass(frozen=True)
class IndexBound:
    """Inclusive/exclusive bound for a range query."""

    value: Any
    inclusive: bool = True

    @staticmethod
    def inclusive_bound(value: Any) -> "IndexBound":
        return IndexBound(value, True)

    @staticmethod
    def exclusive_bound(value: Any) -> "IndexBound":
        return IndexBound(value, False)


@dataclass(frozen=True)
class IndexPredicate:
    """The query shape ``WhitewaterClient.query_index`` lowers onto the
    wire: every declared field exact, a prefix over leading fields, or a
    range over the next field after exact leaders."""

    mode: str
    leading: Tuple[Any, ...] = ()
    partial_text: Optional[str] = None
    lower: Optional[IndexBound] = None
    upper: Optional[IndexBound] = None

    @staticmethod
    def exact(values: List[Any]) -> "IndexPredicate":
        return IndexPredicate("exact", tuple(values))

    @staticmethod
    def prefix(leading: List[Any], partial_text: Optional[str] = None
               ) -> "IndexPredicate":
        return IndexPredicate("prefix", tuple(leading), partial_text)

    @staticmethod
    def range(leading: List[Any], lower: IndexBound, upper: IndexBound
              ) -> "IndexPredicate":
        return IndexPredicate("range", tuple(leading), None, lower, upper)

    def wire_fields(self) -> Dict[str, Any]:
        if self.mode == "exact":
            return {"mode": "exact",
                    "values": [wire_typed(v) for v in self.leading]}
        if self.mode == "prefix":
            body: Dict[str, Any] = {
                "mode": "prefix",
                "leading": [wire_typed(v) for v in self.leading],
            }
            if self.partial_text is not None:
                body["partial_text"] = self.partial_text
            return body
        lower = self.lower or IndexBound(None)
        upper = self.upper or IndexBound(None)
        return {
            "mode": "range",
            "leading": [wire_typed(v) for v in self.leading],
            "lower": {"value": wire_typed(lower.value),
                      "inclusive": lower.inclusive},
            "upper": {"value": wire_typed(upper.value),
                      "inclusive": upper.inclusive},
        }


@dataclass(frozen=True)
class IndexRow:
    """One decoded index posting row."""

    key: bytes
    payload: bytes


@dataclass(frozen=True)
class IndexPage:
    """A page returned by ``query_index``."""

    rows: List[IndexRow]
    #: Active generation that served — changes on rebuild.
    generation: int
    applied_cursor: Optional[str]
    target_cursor: Optional[str]
    #: Serving Node still materializing the activation frontier.
    behind: bool
    #: The page hit a server bound; narrow the query to continue.
    truncated: bool
    #: Node identity that served — diagnostics, never routing input.
    served_by: str


def decode_index_page(body: Dict[str, Any]) -> IndexPage:
    def decode(row: Dict[str, Any], field_name: str) -> bytes:
        raw = row.get(field_name)
        if not isinstance(raw, str):
            from .errors import WhitewaterError
            raise WhitewaterError(f"index row missing {field_name}")
        return base64.b64decode(raw)

    rows = [
        IndexRow(key=decode(row, "key_base64"),
                 payload=decode(row, "payload_base64"))
        for row in body.get("rows") or []
        if isinstance(row, dict)
    ]
    return IndexPage(
        rows=rows,
        generation=int(body.get("generation") or 0),
        applied_cursor=body.get("applied_cursor"),
        target_cursor=body.get("target_cursor"),
        behind=bool(body.get("behind")),
        truncated=bool(body.get("truncated")),
        served_by=str(body.get("served_by") or ""),
    )


# ---------------------------------------------------------------------------
# Demand observability
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DemandRow:
    """One demand row for a declared resource. ``fields`` carries the
    server's counters verbatim — new counters arrive without an SDK
    update, the same contract as :attr:`PipeInfo.operation`."""

    name: str
    #: Owning Domain when the server attributes one.
    domain: Optional[str]
    fields: Dict[str, Any] = field(default_factory=dict)

    def counter(self, name: str) -> Optional[int]:
        """One counter as ``int`` when present."""
        value = self.fields.get(name)
        return int(value) if isinstance(value, (int, float)) else None


@dataclass(frozen=True)
class DemandReport:
    """A demand snapshot for one resource kind: per-resource rows plus
    the per-domain rollup map."""

    rows: List[DemandRow]
    #: ``{"<domain>": {<counters>}}`` — opaque rollup rows keyed by Domain.
    domains: Dict[str, Any] = field(default_factory=dict)

    def domain(self, name: str) -> Dict[str, Any]:
        """One domain's aggregated counters."""
        value = self.domains.get(name)
        return value if isinstance(value, dict) else {}


@dataclass(frozen=True)
class HotKey:
    """Evidence that one Feed Key dominates its traffic window. A
    strictly ordered hot Key is never parallelized — the honest
    operator signal is to spread or shed load for the Key, not add
    capacity."""

    #: The evidence object verbatim — includes any range-placement
    #: fields the server reports; those are diagnostics, not a public
    #: placement contract.
    evidence: Dict[str, Any] = field(default_factory=dict)

    @property
    def feed(self) -> Optional[str]:
        value = self.evidence.get("feed")
        return value if isinstance(value, str) else None

    @property
    def share_bps(self) -> Optional[int]:
        """Dominant-Key share of the window in basis points."""
        value = self.evidence.get("share_bps")
        return int(value) if isinstance(value, (int, float)) else None

    @property
    def window_records(self) -> Optional[int]:
        value = self.evidence.get("window_records")
        return int(value) if isinstance(value, (int, float)) else None

    @property
    def window_bytes(self) -> Optional[int]:
        value = self.evidence.get("window_bytes")
        return int(value) if isinstance(value, (int, float)) else None


def decode_demand_report(body: Dict[str, Any], resource_key: str,
                         name_field: str) -> DemandReport:
    rows = [
        DemandRow(
            name=str(row.get(name_field) or ""),
            domain=row.get("domain") if isinstance(row.get("domain"), str)
            else None,
            fields=dict(row),
        )
        for row in body.get(resource_key) or []
        if isinstance(row, dict)
    ]
    domains = body.get("domains")
    return DemandReport(
        rows=rows,
        domains=dict(domains) if isinstance(domains, dict) else {},
    )


def decode_hot_keys(body: Dict[str, Any]) -> List[HotKey]:
    return [
        HotKey(evidence=dict(row))
        for row in body.get("hot_keys") or []
        if isinstance(row, dict)
    ]
