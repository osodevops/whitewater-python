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

| `client` (direct) | WCL + typed command execution (`/v1/admin/wcl`, `/v1/admin/commands`), node health â€” plus typed resource methods (`create_domain`, `create_feed`, `create_writer`, `create_reader`, `create_subscription`, `describe_feed`, `rename_feed`, `drop_feed`, `list_feeds`, `seek_reader`, `open_writer_session`, â€¦) that build the same WCL for you. `ReaderStart` (`beginning()`, `now()`, `cursor(c)`, `timestamp_ns(ns)`) supplies `START AT` positions |

| `client.writer(name, epoch)` | Idempotent single and batch appends (`request_id` dedup) |

| `client.reader(name)` | Named-Reader sessions: open / fetch / ack / close / resume |

| `client.subscription_member(sub)` | Join, claim, renew, fetch, ack, release under fenced leases |

| `client.feed(name[, value_type/value_codec])` | **Typed feed facade**: binds `codecs` once so keys/payloads serialize automatically in both directions â€” `feed.writer(name)` (declares + opens a session), `feed.reader(name, capacity)` (decoded `fetch`/`ack`, context-managed), `feed.fetch`/`records`/`load` (decoded ad-hoc reads), `feed.subscribe` (decoded batches). Same codec vocabulary as `state_store`: `str`/`bytes`/JSON dataclasses out of the box, protobuf/msgpack/custom via `codecs.of` |
See [docs/stores.md](docs/stores.md) for the full store guide.

### Permanent consumers — the Kafka loop without the plumbing

`feed.consumer(name)` binds the durable named Reader (the
consumer-group equivalent) and owns the fetch → deliver → ack cycle:
transient failures retry with backoff, a fenced session re-opens,
progress resumes from the server-side cursor. Your code is a loop over
records — nothing else:

```python
for msg in in_feed.consumer("enricher"):
    process(msg)          # decoded value; page acks after each yield

# or a handler, blocking forever until close():
consumer = in_feed.consumer("enricher").on_record(process)
consumer.run()
```

- Delivery is per-record; the page's delivered cursor acks only after
  every handler returns — a raised exception leaves it unacked, so the
  page redelivers (at-least-once: make effects idempotent via
  `request_id`, and route poison pills to a DLQ yourself).
- `run()` raises only for startup refusals (bad names, denied auth) —
  everything after the first open is supervised forever until
  `consumer.close()` (safe from signals/atexit; the in-flight page
  finishes and acks first).
- `run_async(handler)` is the same supervised consumer on its own
  thread; `.backoff(initial_s, max_s)`, `.capacity(n)`,
  `.start_at(...)`, `.on_error(fn)`, `.last_error`, `.is_running` tune
  and observe it.
- `feed.subscribe(...)` remains for page-oriented loops that want raw
  control.

Value-only store lookups skip the row wrapper when freshness evidence
isn't needed:

```python
user  = users.find(email)                    # value or None
user  = users.find_by("user_id", user_id)    # first named-key match
count = counters.get_or_default(bucket, 0)
```

`get`/`get_by` still return `StoreRow`/`StoreRows` with applied-cursor
freshness evidence. A complete enrichment service — durable consumer,
named-key fallback, DLQ, idempotent outputs, counters — is
[examples/enrich_users.py](examples/enrich_users.py).
| `client.feeds` | Raw-byte ad-hoc Feed operations: committed record reads, anonymous temporary fetches (`tail`/`wait_ms`/`new_only`), ad-hoc appends, `load(feed, start, sink)` â€” the escape hatch under `client.feed(...)` |

| `client.subscribe(reader, feed, start, on_records)` | Managed durable consume: provisions the Reader, runs open â†’ fetch â†’ deliver â†’ ack on a background thread; `sub.close()` (or `with`) to stop |

| `client.state_store(name[, value_type/value_codec])` | Typed facade over the node-side StateStore: `put`/`get`/`get_by`/`delete`/`iterate`/`clear`/`drop` with typed named keys and freshness evidence. `value_type=` maps a class/dataclass to JSON; `value_codec=` binds any `codecs.Codec` |



```python

store = (client.state_stores.define(

        StateStore.named("accounts.employees")

        .add_key("passport", unique=True).add_key("department")))  # up to 10 named keys



store.put(b"user-1", b"profile-1",

          named={"passport": "P111", "department": "eng"})        # typed values

rows = store.get_by("department", "eng").rows                    # every match



store.get("emp-1")                       # by primary key

store.get_by("passport", "P123")         # by named key â€” every match

for row in store.iterate(): export(row)   # primary-key order, bounded pages

```



`client.feeds.load(feed, start, sink)` remains the lighter one-shot

read when you don't need named-key projections. Codecs are shared

across feeds and stores â€” bind once, never `.encode()`/`.decode()` in

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

`AmbiguousResult` marks operations that may have committed â€” retry

through the same `request_id`, never blindly re-issue.



Cursors are opaque strings: never parsed, ordered, or fabricated.

`event_time_ns`/`ingest_time_ns` are Python `int`s (arbitrary precision)

and travel as decimal strings on the wire â€” the full signed 64-bit

nanosecond range round-trips exactly.



## Pipes, indexes, and operations



Managed Pipes and replicated Indexes share the same topology-free

model — names, never partitions or Nodes:



```python

# Pipe declaration: the client names the business key, source and
# outputs — the server owns panes, leases and the effect journal.
client.define_forward_pipe("orders.relay", "orders-sub", "orders.relayed")
client.define_enrich_pipe("orders.enrich", "orders-sub",
    "orders.enriched", "crm.customers",
    PipeOperation.enrich(Extractor.record_key(),
        freshness=EnrichFreshness.LATEST,
        on_miss=EnrichMiss.QUARANTINE),
    quarantine_feed="orders.enrich-dlq")
client.define_window_pipe("orders.hourly", "orders-sub",
    "stats.orders-hourly",
    PipeOperation.window(3_600_000_000_000,  # window_ns: 1h
        Extractor.record_key(), WindowAggregation.count(),
        time=WindowTime.EVENT,
        watermark_ns=300_000_000_000,  # 5m horizon
        grace_ns=30_000_000_000,       # 30s late revisions
        expire_ns=7_200_000_000_000),
    emit_feed="orders.hourly-emitted")

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

than silently skipping â€” a spec-required signal, not a bug.

