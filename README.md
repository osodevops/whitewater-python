# whitewater-python

Python SDK for Whitewater Riverbeds. Implements the versioned
`whitewater-protocol` wire contract defined in the main repository under
`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Python 3.10+. Runtime dependency: `httpx`.

```bash
pip install whitewater-client   # once published; pip install -e . for development
```

## Clients

```python
from whitewater import WhitewaterClient

client = WhitewaterClient(
    api_key=os.environ["WHITEWATER_API_KEY"],
    endpoints=[
        "https://node1:7070",
        "https://node2:7070",
        "https://node3:7070",
    ],
)
```

| Client | Covers |
|---|---|
| `client` (direct) | WCL + typed command execution (`/v1/admin/wcl`, `/v1/admin/commands`), node health — plus typed resource methods (`create_domain`, `create_feed`, `create_writer`, `create_reader`, `create_subscription`, `describe_feed`, `rename_feed`, `drop_feed`, `list_feeds`, `seek_reader`, `open_writer_session`, …) that build the same WCL for you. `ReaderStart` (`beginning()`, `now()`, `cursor(c)`, `timestamp_ns(ns)`) supplies `START AT` positions |
| `client.writer(name, epoch)` | Idempotent single and batch appends (`request_id` dedup) |
| `client.reader(name)` | Named-Reader sessions: open / fetch / ack / close / resume |
| `client.subscription_member(sub)` | Join, claim, renew, fetch, ack, release under fenced leases |
| `client.feed(name[, value_type/value_codec])` | **Typed feed facade**: binds `codecs` once so keys/payloads serialize automatically in both directions — `feed.writer(name)` (declares + opens a session), `feed.reader(name, capacity)` (decoded `fetch`/`ack`, context-managed), `feed.fetch`/`records`/`load` (decoded ad-hoc reads), `feed.subscribe` (decoded batches). Same codec vocabulary as `state_store`: `str`/`bytes`/JSON dataclasses out of the box, protobuf/msgpack/custom via `codecs.of` |
| `client.feeds` | Raw-byte ad-hoc Feed operations: committed record reads, anonymous temporary fetches (`tail`/`wait_ms`/`new_only`), ad-hoc appends, `load(feed, start, sink)` — the escape hatch under `client.feed(...)` |
| `client.subscribe(reader, feed, start, on_records)` | Managed durable consume: provisions the Reader, runs open → fetch → deliver → ack on a background thread; `sub.close()` (or `with`) to stop |
| `client.state_store(name[, value_type/value_codec])` | Typed facade over the node-side StateStore: `put`/`get`/`get_by`/`delete`/`iterate`/`clear`/`drop` with typed named keys and freshness evidence. `value_type=` maps a class/dataclass to JSON; `value_codec=` binds any `codecs.Codec` |

```python
store = (client.state_stores.define(
        StateStore.named("accounts.employees")
        .add_key("passport", unique=True).add_key("department")))  # up to 10 named keys

store.put(b"user-1", b"profile-1",
          named={"passport": "P111", "department": "eng"})        # typed values
rows = store.get_by("department", "eng").rows                    # every match

store.get("emp-1")                       # by primary key
store.get_by("passport", "P123")         # by named key — every match
for row in store.iterate(): export(row)   # primary-key order, bounded pages
```

`client.feeds.load(feed, start, sink)` remains the lighter one-shot
read when you don't need named-key projections. Codecs are shared
across feeds and stores — bind once, never `.encode()`/`.decode()` in
application code:

```python
orders = client.feed("orders.events", value_type=Order)
orders.writer("ingest").append("order-1", Order("order-1", 42))
with orders.reader("audit", capacity=64) as reader:
    order = reader.fetch().records[0].value          # decoded Order

latest = client.state_store("demo.latest", value_type=Order)
latest.put("order-1", order)                          # same codec seam
```

See [docs/stores.md](docs/stores.md) for the full store guide.

Errors surface as `WhitewaterError` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, ...). `Unavailable` covers transient refusals;
`AmbiguousResult` marks operations that may have committed — retry
through the same `request_id`, never blindly re-issue.

Cursors are opaque strings: never parsed, ordered, or fabricated.
`event_time_ns`/`ingest_time_ns` are Python `int`s (arbitrary precision)
and travel as decimal strings on the wire — the full signed 64-bit
nanosecond range round-trips exactly.

## Pipes, indexes, and operations

Managed Pipes and replicated Indexes share the same topology-free
model � names, never partitions or Nodes:

```python
# Pipe lifecycle: describe, pause, resume, drop.
pipe = client.pipe("orders.relay")
info = pipe.describe()        # stage, operation, outputs, status
pipe.pause(); pipe.resume(); pipe.drop()
pipes = client.pipes()        # the declared catalog

# Index queries: exact, prefix, or range predicates over typed values.
page = client.query_index(
    "crm.byregion",
    IndexPredicate.range(
        ["emea"],
        IndexBound.inclusive_bound(0),
        IndexBound.exclusive_bound(99)),
    limit=50, strict=True)
for row in page.rows:         # row.key / row.payload bytes
    ...

# Demand observability: typed rows + per-domain rollups.
report = client.demand_feeds()
appended = report.rows[0].counter("appended_records")
client.demand_readers(); client.demand_subscriptions()
client.demand_indexes();  client.demand_pipes()
hot = client.hot_keys()       # dominant-key evidence per feed
```

Pipe operation payloads and demand counter fields stay opaque dicts so
server-side variants evolve without an SDK bump; `query_index` typed
values lower automatically (`str`/`int`/`bool`/`None`/`bytes`).


## Test

```bash
pip install -e .[dev]
pytest
```

## Conformance

`tests/conformance/` executes the shared fixture corpus against a real
three-Node Riverbed:

```powershell
$env:WHITEWATER_ENDPOINTS = "http://localhost:7071,http://localhost:7072,http://localhost:7073"
$env:WHITEWATER_SPEC_DIR  = "C:\Code\Stackapps\whitewater\sdk\spec\conformance"
pytest tests/conformance
```

A scenario whose `op` the SDK cannot express fails explicitly rather
than silently skipping — a spec-required signal, not a bug.
