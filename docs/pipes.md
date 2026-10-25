# Pipes

A **Pipe** is a managed stream job: it drives one declared **Subscription**
and applies an operation to every record the Subscription delivers, writing
results to a Feed or a StateStore. Pipes run server-side — your process
does not need to stay alive for a Pipe to keep going.

| Operation | Reads from | Writes to | Purpose |
|---|---|---|---|
| `forward` | Subscription | Feed | copy records verbatim |
| `enrich` | Subscription + StateStore | Feed (+ optional quarantine Feed) | point-lookup enrichment |
| `window` | Subscription | StateStore (+ optional emit Feed) | fixed-window aggregation |

```python
from whitewater import WhitewaterClient

client = WhitewaterClient("localhost:7371", api_key)
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe:

```python
client.create_subscription("orders.sub", "ops.orders")
# start defaults to ReaderStart.beginning(); now()/cursor()/timestamp_ns()
# select other starts.
```

`enrich` and `window` pipes need a **StateStore** (see `docs/stores.md`):

```python
from whitewater import Extractor

store = (client.state_store("ops.customers")
         .add_key("email", unique=True, extractor=Extractor.json("$.email"))
         .add_key("tier", extractor=Extractor.json("$.tier")))
client.state_stores.define(store, source="ops.customer.changes")
```

## 2. Forward pipes

```python
client.define_forward_pipe(
    "ops.orders.relay",    # pipe name (dotted)
    "orders.sub",          # source subscription
    "ops.orders.relayed",  # output feed
)
```

## 3. Enrich pipes

Point-lookup enrichment: project a key with an `Extractor`, resolve it
against the store, and copy the matched row into output metadata
(`enrich.payload`, `enrich.matched`). Resolution rides inside the effect
journal — replayed outputs are byte-identical.

```python
from whitewater import PipeOperation, Extractor, EnrichFreshness, EnrichMiss

client.define_enrich_pipe(
    "ops.orders.enrich",
    "orders.sub",
    "ops.enriched",
    "ops.customers",                    # lookup store
    PipeOperation.enrich(
        Extractor.json("$.customerId"),
        freshness=EnrichFreshness.LATEST,     # or EnrichFreshness.STRICT
        named="email",                        # named key; None = primary
        on_miss=EnrichMiss.QUARANTINE,        # FORWARD | DROP | QUARANTINE
    ),
    quarantine_feed="ops.enrich.misses",      # required for QUARANTINE
)
```

## 4. Window pipes

A window pipe's output store must be declared before the pipe — and it
must carry non-unique named keys `"key"` and `"pane"` so pane rows stay
enumerable without composite row keys:

```python
from whitewater import WindowAggregation, WindowTime

client.state_stores.define(
    client.state_store("ops.order.counts").add_key("key").add_key("pane")
)

client.define_window_pipe(
    "ops.orders.hourly",
    "orders.sub",
    "ops.order.counts",                 # pane store (declared above)
    PipeOperation.window(
        timedelta(hours=1),             # 1h panes
        Extractor.json("$.region"),
        WindowAggregation.sum(Extractor.json("$.total")),
        time=WindowTime.EVENT,          # or WindowTime.INGEST
        watermark=timedelta(minutes=5), # event-time close horizon
        grace=timedelta(minutes=1),     # revise emitted panes up to 1min late
        expire=timedelta(hours=24),     # keep panes 24h
        key_limit=10_000,
    ),
    emit_feed="ops.hourly.emits",       # optional closed-pane output
)
```

Every window duration is a `datetime.timedelta` — the SDK extracts
nanoseconds internally for the wire; callers never spell raw nanosecond
counts.

### Lateness models — pick exactly one

- `max_late` — ingest-clock bound; later arrivals quarantine into a
  countable `late` row.
- `watermark` — event-time horizon `max(event_time) − watermark`
  closes a pane once it passes the pane end (refused under
  `WindowTime.INGEST`); `grace` lets late events revise an emitted pane
  before counting into `late`.

An emit feed requires a declared lateness model. `None` leaves a bound
unset; the `*_ns` fields on the wire carry the nanoseconds the SDK
extracts.

### Aggregations

```python
WindowAggregation.count()
WindowAggregation.sum(Extractor.json("$.total"))
WindowAggregation.average(Extractor.json("$.rtt"))
```

## 5. Lifecycle

```python
pipe = client.pipe("ops.orders.relay")

info = pipe.describe()   # stage, operation, outputs, status
pipe.pause()             # cooperative; the frontier stays put
pipe.resume()            # restarts from the durable frontier
pipe.drop()

all_pipes = client.pipes()
```

`pause` is durable and idempotent — nothing is dropped while paused.
`describe()` returns `stage` (`declared`/`paused`), the opaque `operation`
node, and output Feed/Store IDs.

## 6. Watching demand

```python
demand = client.demand_pipes()
for row in demand.rows:
    print(row.name, row.stage)
```

## 7. Failure semantics

- Declarations commit atomically inside `client.commands([...])` — submit
  subscription + `define_pipe` in one batch to apply both or neither.
- Pipe delivery is at-least-once; effects journal lookups/accumulation so
  replayed outputs are byte-identical.
