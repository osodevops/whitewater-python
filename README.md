# whitewater-python



Python SDK for Whitewater Riverbeds. Implements the versioned

`whitewater-protocol` wire contract defined in the main repository under

`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).



Requires Python 3.10+. Runtime dependency: `httpx`.



```bash

pip install whitewater-client   # once published; pip install -e . for development

```



A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

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

| `client.domain(name)` / `client.get_domain(name)` | **Strongly-typed Domain facade**: `domain()` is a lazy handle (free name composition, no I/O); `get_domain()` verifies the Domain exists and materializes its catalog view in one batched read — `domain.feeds("order")`, `feed_names()`, `readers()`, `writers()`, `subscriptions()`, `pipes()`, `indexes()`, `state_stores()`, `describe()`, `history_policy`, `refresh()`. `domain.feed("orders")` binds a typed `Feed` under the Domain's prefix; `domain.create_feed("orders")` declares it |

| `client.writer(name, epoch)` | Idempotent single and batch appends (`request_id` dedup) |

| `client.reader(name)` | Named-Reader sessions: open / fetch / ack / close / resume |

| `client.subscription_member(sub)` | Join, claim, renew, fetch, ack, release under fenced leases |

| `client.feed(name[, value_type/value_codec])` | **Typed feed facade**: binds `codecs` once so keys/payloads serialize automatically in both directions â€” `feed.writer(name)` (declares + opens a session), `feed.reader(name, max_page_size)` (decoded `fetch`/`ack`, context-managed), `feed.fetch`/`records`/`load` (decoded ad-hoc reads), `feed.subscribe` (decoded batches). Same codec vocabulary as `state_store`: `str`/`bytes`/JSON dataclasses out of the box, protobuf/msgpack/custom via `codecs.of` |
See [docs/stores.md](docs/stores.md) for the full store guide.

### Typed Domains — navigate, don't concatenate

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`), so the
friendliest surface navigates domain → feed → reader/writer instead
of composing dotted names by hand:

```python
from whitewater import ReaderStart

shop = client.get_domain("shop")          # verifies + materializes now
# client.domain("shop") is the lazy form — zero I/O until needed
shop.create_feed("orders")              # CREATE FEED shop.orders;

shop.feed_names()                         # ['shop.orders', 'shop.returns']
[f.name for f in shop.feeds("order")]     # prefix-filtered Feed handles
shop.readers(); shop.writers()            # scoped catalog views
shop.describe(); shop.history_policy

orders = shop.feed("orders", value_type=Order)   # typed Feed handle

# Everything a service needs hangs off the Domain — relative names
# resolve inside it:
writer = (shop.create_writer("orders")
          .writer_id("orders.ingest")
          .open())
writer.append("order-1", Order("order-1", 42))

with (shop.create_reader("orders")
      .reader_id("fulfillment")                    # durable id — the Kafka groupId analogue
      .reader_start_point(ReaderStart.BEGINNING)   # first-declare only
      .max_page_size(256)
      .open()) as reader:
    pump = reader.listen(lambda m: handle(m.value))  # SDK-owned thread — returns now
    # pump.stop() releases thread + session; pump.failure surfaces a raised handler
    # or iterate: `for m in reader:` / `reader.for_each(h)` — same ack-per-page semantics
    # or stay manual: fetch() → handle records → ack()
```

`reader_id` is durable application state: `reader_start_point` applies
only when the Reader is first declared — an existing Reader resumes its
acknowledged frontier (the `auto.offset.reset` mapping). A `Feed` bound
to another Domain is refused locally, before any network call. The
first catalog accessor issues ONE batched control read (`describe
domain` + every `show` listing) and caches the snapshot — `refresh()`
re-pulls.

### Record schemas

Records stay schema-neutral bytes on the wire; a Feed can still declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```python
from whitewater import FeedSchema, SchemaDecl

client.set_domain_schema("shop", FeedSchema(value=SchemaDecl("json", subject="order-v1")))
client.set_feed_schema("shop.orders", FeedSchema(key=SchemaDecl("text")))

shop.schema()                            # the Domain default
shop.feed_schema("shop.orders")          # effective: key=text, value=json/order-v1
shop.declared_feed_schema("shop.orders") # the Feed's own sides only
client.clear_feed_schema("shop.orders")  # reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`,
or `custom.<name>` — compatibility enforcement is still a no-op.

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
  thread; `.backoff(initial_s, max_s)`, `.max_page_size(n)`,
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



store.put("user-1", profile,

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

with orders.reader("audit", max_page_size=64) as reader:

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

model — names, never partitions or Nodes. See

[docs/pipes.md](docs/pipes.md) for the full pipe guide:



```python
from datetime import timedelta

# Pipe declaration: the client names the business key, source and
# outputs — the server owns panes, leases and the effect journal.
client.define_forward_pipe("orders.relay", "orders.sub", "orders.relayed")
client.define_enrich_pipe("orders.enrich", "orders.sub",
    "orders.enriched", "crm.customers",
    PipeOperation.enrich(Extractor.record_key(),
        freshness=EnrichFreshness.LATEST,
        on_miss=EnrichMiss.QUARANTINE),
    quarantine_feed="orders.enrich.dlq")
client.define_window_pipe("orders.hourly", "orders.sub",
    "stats.orders.hourly",
    PipeOperation.window(timedelta(hours=1),  # 1h panes
        Extractor.record_key(), WindowAggregation.count(),
        time=WindowTime.EVENT,
        watermark=timedelta(minutes=5),  # 5m horizon
        grace=timedelta(seconds=30),     # 30s late revisions
        expire=timedelta(hours=2)),
    emit_feed="orders.hourly.emitted")

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





## API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`commands`, `wcl`, and the `*SessionClient`/`SubscriptionMemberClient`
classes are the explicit escape hatches for the same wire contract.

| Area | API |
|---|---|
| Client & transport | `WhitewaterClient(api_key, endpoints, timeout)` · `healthy()` · `use_compression` on feeds/writers · `close()` |
| Typed Domains | `domain(name)` lazy / `get_domain(name)` eager · `name` `domain_id` `revision` `describe()` `history_policy` `refresh()` · `feeds(prefix)` `feed_names()` `readers()` `writers()` `subscriptions()` `pipes()` `indexes()` `state_stores()` · `feed(relative, codecs/types)` `get_feed` · `create_feed` `create_reader` `create_writer` (relative/Feed) |
| Schemas | `set_domain_schema` `set_feed_schema` `clear_domain_schema` `clear_feed_schema` · `Domain.schema()` `feed_schema()` `declared_feed_schema()` · `SchemaDecl`/`FeedSchema` — `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` |
| Feeds | `feed(name, key_codec=, value_codec=)` → `Feed` · `use_compression` · `records()` `fetch()` `load()` · `writer` `append` `reader` `subscribe` `consumer` `view` `view_types` |
| Writing | `FeedWriter.append(key, value)` `append_event` `append_all` · `use_compression` `session_epoch` · idempotent `request_id` · `FeedEvent.with_request_id/.with_event_time_ns/.with_metadata/.text_metadata` |
| Reading | `FeedReader.listen(handler)` SDK-owned pump → `ReaderPump` (`stop()` `failure` `running`) · `for_each(h)` blocking pump · `for m in reader` iterator · `fetch(limit)` `ack()` `close()` · `session_epoch` `acknowledged_cursor` · durable `reader_id` · `ReaderStart` — `BEGINNING`, `END`, `now()`, `cursor`, `timestamp(datetime)` |
| Temporary fetches | `Feed.fetch(after=, limit=)` / `FeedClient.temporary_fetch` — `after_event_time_ns` seeks, `new_only`, `tail` + `wait_ms` long-poll · `Feed.records` `Feed.load` |
| Consumers | `feed.consumer(reader)` → `FeedConsumer`: `start_at` `max_page_size` `on_record` `on_error` `backoff` `last_error` `is_running` · `run` / `run_async` / `close` |
| Subscription members | `subscription_member(name)` → `join` `claim` `renew` `release` `fetch` `ack` — shared progress + RF3 work leases · `client.subscribe(...)` managed loop |
| StateStores | `state_store(name)` / `StateStore.named(name)` → `view` `view_types` `add_key` `named_keys` `put` `get` `get_by` `find` `find_by` `get_or_default` `delete` `page` `iterate` `clear` `drop` · `state_stores.define` `state_stores.drop` |
| Indexes | `query_index(name, IndexPredicate)` → `IndexPage` · `IndexPredicate.exact/prefix/range` + `IndexBound` |
| Pipes | `define_forward_pipe` `define_enrich_pipe` `define_window_pipe` · `pipe(name)` → `describe` `pause` `resume` `drop` · `pipes()` · `PipeOperation` `WindowAggregation` `WindowTime` · durations are `datetime.timedelta` |
| Catalog lifecycle | `create_*` `describe_*` `rename_*` `drop_*` `list_*` for Domain, Feed, Writer, Reader, Subscription, Role · `list_grants` · `open_writer_session` `seek_reader` |
| Observability | `demand_feeds` `demand_readers` `demand_subscriptions` `demand_indexes` `demand_pipes` → `DemandReport`/`DemandRow` · `hot_keys()` → `HotKey` |
| Escape hatches | `wcl(script)` · `commands([...])` raw command batch · `WriterSessionClient` `ReaderSessionClient` `SubscriptionMemberClient` byte-level sessions · `FeedClient` raw feed ops |
| Errors | `WhitewaterError` — `Refused` `StaleSession` `AmbiguousResult` `Conflict` `Unavailable` `LeaseConflict` · `.code` carries the wire `code` |
| Testing | `FakeRiverbed` — in-process loopback Riverbed implementing the wire contract (dedup, epochs, at-least-once reads, stores, WCL/commands, schemas) |

## Test



```bash

pip install -e .[dev]

pytest

```



## Unit-test doubles

`whitewater.testing.FakeRiverbed` serves the wire contract on loopback —
appends dedupe by request id, unacked pages redeliver, stale epochs
fence, and StateStores model named keys — so application tests exercise
the real client without a Riverbed.

```python
from whitewater.testing import FakeRiverbed

with FakeRiverbed() as fake:
    client = fake.client()          # configured WhitewaterClient
    # ...
```

See `tests/test_testing_double.py` and the
[testing guide](../whitewater/docs/guides/testing-your-app.md).

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

