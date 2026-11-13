# whitewater-python

Python SDK for Whitewater. Implements the versioned `whitewater-protocol`
wire contract defined in the main repository under `sdk/spec/`
(`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Python 3.10+. Runtime dependency: `httpx`.

```bash
pip install whitewater-client   # once published; pip install -e . for development
```

A complete microservice walkthrough — provision, produce, pump-process,
dead-letter, shutdown — lives in
[`docs/demo-service.md`](docs/demo-service.md).

## Creating a client

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

`client.healthy()` checks liveness. The client also carries the flat
administration surface — `wcl`/`commands`, node health, Domain and role
lifecycle — while every Feed, Writer, Reader, Subscription, Member,
Pipe, StateStore, and Index is reached **only** through a `Domain`.

Whitewater's public model is `domain -> feed -> key -> cursor ->
subscription`: partitions, ranges, replicas, and physical positions are
internal implementation details — cursors are opaque tokens, never
parsed.

## Domains — the strongly-typed front door

A Domain is the administrative namespace every business resource lives
inside: a Feed named `shop.orders` belongs to the Domain `shop`, and
nothing outside a Domain can reach it. Writers, Readers, Subscriptions,
StateStores, Indexes, and Pipes resolve their relative names inside the
owning Domain, so a `"fulfillment"` Reader declared on `shop` is
`shop.fulfillment` everywhere it matters: progress, leases, WCL, demand
reports, and access control all scope to it. Cross-Domain references are
possible but always explicit (a handle or `Scoped.absolute`), which
makes accidental cross-wiring of two tenants' resources a deliberate
act rather than a silent mistake.

```python
from whitewater import ReaderStart

shop = client.create_domain("shop")       # CREATE DOMAIN shop;
shop = client.get_domain("shop")          # verifies + materializes now
# client.domain("shop") is the lazy form — zero I/O until needed
shop.create_feed("orders")                # CREATE FEED shop.orders;

shop.feed_names()                         # ['shop.orders', 'shop.returns']
[f.name for f in shop.feeds("order")]     # prefix-filtered Feed handles
shop.readers(); shop.writers()            # scoped catalog views
shop.subscriptions(); shop.pipes()
shop.indexes();     shop.state_stores()
shop.describe(); shop.history_policy
shop.refresh()                            # re-pull the cached snapshot

orders = shop.feed("orders", value_type=Order)   # typed Feed handle

# Everything a service needs hangs off the Domain — relative names
# resolve inside it:
writer = (shop.create_writer("orders")
          .writer_id("ingest")                     # durable producer identity — required
          .open())

with (shop.create_reader("orders")
      .reader_id("fulfillment")                    # durable consume identity — Kafka groupId
      .reader_start_point(ReaderStart.BEGINNING)   # first-declare only — auto.offset.reset
      .max_page_size(256)                          # max records per fetch
      .open()) as reader:
    ...
```

`reader_id` is durable application state: `reader_start_point` applies
only when the Reader is first declared — an existing Reader resumes its
acknowledged frontier. `writer_id` is required because it's the Writer's
transactional identity: the fencing epoch binds to it and the
retry-dedup ledger lives under it. A `Feed` bound to another Domain is
refused locally, before any network call. The first catalog accessor
issues ONE batched control read (`describe domain` + every `show`
listing) and caches the snapshot — `refresh()` re-pulls.

### Record schemas

Records stay schema-neutral bytes on the wire; a Feed can still declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```python
from whitewater import FeedSchema, SchemaDecl

client.set_domain_schema("shop", FeedSchema(value=SchemaDecl("json", subject="order-v1")))
shop.set_feed_schema("orders", FeedSchema(key=SchemaDecl("text")))

shop.schema()                            # the Domain default
shop.feed_schema("orders")               # effective: key=text, value=json/order-v1
shop.declared_feed_schema("orders")      # the Feed's own sides only
shop.clear_feed_schema("orders")         # reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`,
`text`, `bytes`, or `custom.<name>` — compatibility enforcement is
still a no-op.

## Working with Feeds

`shop.feed(name, value_type=Order)` binds a feed to key and payload
codecs once; every write encodes and every read decodes through them —
application code never calls `.encode()`/`.decode()`. `value_type=`
maps a class/dataclass to JSON; `value_codec=` binds any
`codecs.Codec` (`str`/`bytes`/JSON out of the box,
protobuf/msgpack/custom via `codecs.of`).

```python
orders = client.domain("orders").feed("events", value_type=Order)

orders.writer("ingest").append("order-1", Order("order-1", 42))

with orders.reader("audit", max_page_size=64) as reader:
    order = reader.fetch().records[0].value          # decoded Order
```

### Reading — durable Readers and ad-hoc fetches

A **Reader** owns a durable, server-side cursor — the progress identity
that survives restarts, exactly like a committed Kafka group offset but
without the group protocol:

```python
with (shop.create_reader("orders")
      .reader_id("fulfillment")                    # durable consume identity
      .reader_start_point(ReaderStart.BEGINNING)   # first-declare start position
      .max_page_size(256)                          # max records per fetch
      .open()) as reader:
    page = reader.fetch()                          # up to max_page_size records
    for m in page.records:
        process(m.value)
    reader.ack()                                   # commits the delivered frontier
```

Three SDK-owned loops sit on top of the same fetch→ack cycle —
`reader.listen(handler)` pumps on a background thread and returns a
`ReaderPump` (`stop()`, `failure`, `running`), `reader.for_each(h)` is
the same pump blocking, and `for m in reader` iterates:

```python
pump = reader.listen(lambda m: handle(m.value))    # returns immediately
# pump.stop() releases thread + session; pump.failure surfaces a raised handler
```

For ad-hoc reads without a durable identity, `feed.records(limit)`,
`feed.fetch(after=, limit=)` (`tail`/`wait_ms` long-poll,
`after_event_time_ns` seeks), and `feed.load(start, sink)` one-shot
replays cover committed history.

### Shared progress — subscriptions and members

A named **Subscription** is the durable shared frontier cooperating
readers claim work against — the Kafka consumer-group model, minus the
rebalance storm. `domain.member_client(name)` joins a member and runs
the work loop:

```python
member = shop.member_client("orders-sub")
member.join("member-1")                            # register this member
work = member.claim()                              # RF3-fenced work lease
page = member.fetch(work, 256)                     # records for this member
member.ack(work, page)                             # commits shared progress
member.release(work)                               # or transfer to another member
```

Leases are fenced — a lost or expired claim is refused, and work
transfers cleanly to another member.

### The read loop — consumers

`feed.consumer(name)` binds the durable named Reader (the
consumer-group equivalent) and owns the fetch → deliver → ack cycle:
transient failures retry with backoff, a fenced session re-opens,
progress resumes from the server-side cursor. Your code is a loop over
records — nothing else:

```python
for msg in in_feed.consumer("enricher"):
    process(msg.value)    # decoded value; page acks after each yield

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
  control; `domain.subscribe(reader, feed, start, on_records)` runs the
  managed version on a background thread.

A complete enrichment service — durable consumer, named-key fallback,
DLQ, idempotent outputs, counters — is
[examples/enrich_users.py](examples/enrich_users.py).

### Writing

```python
# Declares shop.ingest and blocks until the record is
# majority-committed (at least 2 of 3 Nodes):
writer = orders.writer("ingest")
appended = writer.append("order-42", Order("order-42", 99))
# appended: message_id, cursor, deduplicated, durability — the ack payload

# Idempotent events: pin request_id for caller-driven retries —
# the same identity replays its committed result instead of writing twice.
writer.append_event(FeedEvent.of("order-42", Order("order-42", 99))
                    .with_request_id("a-stable-request-uuid")
                    .with_event_time_ns(event_time_ns)
                    .with_metadata({"source": b"api"}))

# One commit for a group of records:
writer.append_all([e1, e2])
```

Every append carries an idempotent `request_id` — retrying a timed-out
append with the same identity deduplicates; reusing it with different
contents is refused. Writers are epoch-fenced sessions: a second writer
claiming `ingest` fences the first, so an ambiguous timeout can never
double-commit under retry. `use_compression` opts into LZ4; record
`event_time_ns`/`ingest_time_ns` are Python `int`s (arbitrary
precision) traveling as decimal strings on the wire — the full signed
64-bit nanosecond range round-trips exactly.

## Working with StateStores

StateStores are durable, replicated key/value views that live on the
Nodes — the SDK holds no local copy. Declare named keys up front and
each becomes a maintained secondary index.

### Declaring a store — named keys

```python
accounts = client.domain("accounts")
store = accounts.define_state_store(
    accounts.state_store("employees")
    .add_unique_key("passport")              # one row per value
    .add_non_unique_key("department"))       # value → many rows; up to 10 keys
```

**Unique** (`passport`, `email`, `order_number`) — one value maps to at
most one row; `get_by` returns the single match and a second claim on
the same value arbitrates first-committed-wins. **Non-unique**
(`department`, `status`, `region`) — one value maps to many rows;
`get_by` returns every match as a bounded page.

Two store kinds: **manual** (your app puts rows — no source Feed) and
**feed-sourced** (the Nodes replay a source Feed and project rows +
postings via `Extractor.record_key()` / `Extractor.metadata(k)` /
`Extractor.json(path)` — required on feed-sourced keys, refused on
manual ones).

### Putting and deleting

Named-key postings take a typed `StoreKeySet` subclass, a builder
lambda, a plain dict, or a single string for one declared named key:

```python
store.put("user-1", profile,
          PassportKeySet(passport="P111", department="eng"))   # typed key set
store.put("user-1", profile,
          named={"passport": "P111", "department": "eng"})     # raw dict — no checking

store.delete("user-1")                     # mutation id generated
store.clear()                              # one journaled ClearAll
```

Every mutation carries an idempotent `mutation_id` — generated per
call, or pinned for caller-driven retries.

### Reading

```python
row = store.get("emp-1")                   # StoreRow — value + applied-cursor freshness
user  = users.find(email)                  # value or None — skips the row wrapper
user  = users.find_by("user_id", user_id)  # first named-key match
count = counters.get_or_default(bucket, 0) # counters, one line

rows = store.get_by("department", "eng").rows   # every match
page = store.page(after_key, limit=256)         # one bounded page
for row in store.iterate():                     # primary-key order
    export(row)
```

`get`/`get_by` return `StoreRow`/`StoreRows` carrying applied-cursor
freshness evidence — `strict=` bounded-waits for the replica to apply
its committed frontier rather than answering stale. The facade is thin:
rows live on the Nodes (RF3) and bind the same codecs as feeds.
`feed.load(start, sink)` remains the lighter one-shot read when you
don't need named-key projections. See [docs/stores.md](docs/stores.md)
for the full store guide.

## Working with Pipes

Managed Pipes are server-run stream jobs: you declare the business
shape — source Subscription, output Feed, operation — and the server
owns the subscription frontier, compute lease, panes, and the effect
journal. String endpoints resolve inside the declaring Domain; a
`Feed`/`SubscriptionRef`/`StateStore` handle or `Scoped.absolute`
crosses Domains explicitly. The Pipe's own name and its input
Subscription always stay in the declaring Domain. See
[docs/pipes.md](docs/pipes.md) for the full pipe guide and
[examples/cross_domain.py](examples/cross_domain.py) for cross-Domain
wiring.

### Simple — forwarding

```python
orders = client.domain("orders")
orders.define_forward_pipe("relay",
    "orders.sub",          # source Subscription
    "relayed")             # output Feed, same Domain
```

### Enrichment — StateStore lookups

```python
orders.define_enrich_pipe("enrich", "orders.sub",
    "enriched", crm.state_store("customers"),   # cross-Domain store handle
    PipeOperation.enrich(Extractor.record_key(), # join on record key
        freshness=EnrichFreshness.LATEST,
        on_miss=EnrichMiss.QUARANTINE),          # misses → DLQ
    quarantine_feed="enrich.dlq")
```

### Time-based — windowing

```python
from datetime import timedelta

orders.define_window_pipe("hourly", "orders.sub",
    stats.state_store("orders.hourly"),         # cross-Domain pane store
    PipeOperation.window(timedelta(hours=1),    # 1h panes
        Extractor.record_key(), WindowAggregation.count(),
        time=WindowTime.EVENT,
        watermark=timedelta(minutes=5),  # 5m horizon
        grace=timedelta(seconds=30),     # 30s late revisions
        expire=timedelta(hours=2)),      # pane expiry
    emit_feed="hourly.emitted")
```

### Indexes and observability

```python
# Pipe lifecycle: describe, pause, resume, drop.
pipe = orders.pipe("relay")
info = pipe.describe()        # stage, operation, outputs, status
pipe.pause(); pipe.resume(); pipe.drop()
pipes = orders.pipe_infos()   # the Domain-scoped catalog

# Index queries: exact, prefix, or range predicates over typed values.
page = crm.query_index(
    "byregion",
    IndexPredicate.range(
        ["emea"],
        IndexBound.inclusive_bound(0),
        IndexBound.exclusive_bound(99)),
    limit=50,           # rows per page
    strict=True)        # wait for the committed target rather than stale
for row in page.rows:   # row.key / row.payload bytes
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

## Errors, cursors, and time

Errors surface as `WhitewaterError` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, ...) — `Refused` `StaleSession`
`AmbiguousResult` `Conflict` `Unavailable` `LeaseConflict`.
`Unavailable` covers transient refusals; `AmbiguousResult` marks
operations that may have committed — retry through the same
`request_id`, never blindly re-issue.

Cursors are opaque strings: never parsed, ordered, or fabricated.
`event_time_ns`/`ingest_time_ns` are Python `int`s (arbitrary
precision) and travel as decimal strings on the wire — the full signed
64-bit nanosecond range round-trips exactly.

## Build

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
than silently skipping — a spec-required signal, not a bug.

## Full API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`commands`, `wcl`, and the `*SessionClient`/`SubscriptionMemberClient`
classes are the explicit escape hatches for the same wire contract.

| Method | Parameters | Description |
|---|---|---|
| **Client & transport** | | |
| `WhitewaterClient(api_key, endpoints, timeout)` | api key, endpoint list, timeout | Client construction |
| `healthy()` `close()` `use_compression` | — | Liveness, shutdown, LZ4 on feeds/writers |
| **Domains — the only door** | | |
| `create_domain(name)` / `domain(name)` / `get_domain(name)` | relative name | Declare / lazy / eager handle |
| `feed(name[, value_type/value_codec])` `get_feed` | name, type/`codecs.Codec` | Typed feed handle |
| `create_feed` `create_reader` `create_writer` `create_subscription` | relative name | Declare + open inside the Domain |
| `feeds(prefix)` `feed_names()` `readers()` `writers()` `subscriptions()` `pipes()` `indexes()` `state_stores()` | optional prefix | Domain catalog views |
| `name` `domain_id` `revision` `describe()` `history_policy` `refresh()` | — | Domain metadata |
| `set_domain_schema` `set_feed_schema` `clear_domain_schema` `clear_feed_schema` `schema` `feed_schema` `declared_feed_schema` | `FeedSchema`/`SchemaDecl` — `json`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` | Record-schema declarations |
| **Feeds** | | |
| `records` `fetch` `load(start, sink)` | limit, cursor, `ReaderStart` | Committed history reads |
| `writer` `reader` `subscribe` `consumer` `view` `view_types` | name, options | Bound sessions + loops |
| **Writing** | | |
| `FeedWriter.append(k, v)` `append_event` `append_all` | key, value, `FeedEvent` | Blocking write — `AppendResult` on majority commit |
| `FeedEvent.with_request_id` `.with_event_time_ns` `.with_metadata` `.text_metadata` | id, epoch-ns, metadata | Idempotency + event fields |
| `session_epoch` `use_compression` | — | Session + lifecycle |
| **Reading** | | |
| `FeedReader.fetch(limit)` `ack()` `close()` `session_epoch` `acknowledged_cursor` | page bound, cursor | Manual durable-Reader loop |
| `listen(handler)` → `ReaderPump` `for_each` `for m in reader` | record handler | SDK-owned read loops (`stop()` `failure` `running`) |
| `ReaderStart` | `BEGINNING` `END` `now()` `cursor` `timestamp` | First-declare start position |
| `feed_ops().temporary_fetch` `records` `load` | `after_event_time_ns`, `tail`, `wait_ms` | Ad-hoc + replay reads |
| `feed.consumer(name)` → `FeedConsumer` | `start_at` `max_page_size` `on_record` `on_error` `backoff` `last_error` `is_running` | Supervised durable consumer: `run`/`run_async`/`close` |
| **Subscription members** | | |
| `domain.member_client(name)` | `join` `claim` `renew` `release` `fetch` `ack` `transfer` | Shared progress + RF3 work leases |
| `domain.subscribe(reader, feed, start, on_records)` | names, handler | Managed pump loop |
| **StateStores** | | |
| `domain.state_store(name)` `StateStore.named(name)` `define_state_store` `drop_state_store` | name, codecs, source Feed | Store lifecycle + typed handle |
| `put` `delete` `clear` | key, value, named-key postings | Mutations |
| `get` `get_by` `find` `find_by` `get_or_default` `page` `iterate` | key, named key, `strict=`/`limit=`/`after` | Row + named-key reads |
| `add_unique_key` `add_non_unique_key` `named_keys` `drop` `view` `view_types` | key name | Spec + administration |
| **Indexes** | | |
| `query_index(name, predicate, limit, strict)` | `IndexPredicate.exact/prefix/range` + `IndexBound` | Decoded index rows |
| **Pipes** | | |
| `define_forward_pipe` `define_enrich_pipe` `define_window_pipe` | name, `ScopedRef` (str/`Feed`/`Store`/`absolute`), `PipeOperation` + `WindowAggregation`/`WindowTime` | Managed jobs (`timedelta` durations) |
| `pipe(name)` `pipe_infos()` | — | `describe` `pause` `resume` `drop` |
| **Catalog lifecycle** | | |
| `create_*` `describe_*` `rename_*` `drop_*` `seek_reader` `open_writer_session` | names | Feed/Writer/Reader/Subscription/Pipe/StateStore lifecycle on Domain + `describe_domain` `rename_domain` `drop_domain` `list_domains` `list_roles` `list_grants` on the client |
| **Observability** | | |
| `demand_feeds` `demand_readers` `demand_subscriptions` `demand_indexes` `demand_pipes` `hot_keys` `demand_hot_keys` | optional feed | `DemandReport`/`DemandRow` + `HotKey` evidence |
| **Escape hatches & errors** | | |
| `wcl(script)` `commands([...])` `WriterSessionClient` `ReaderSessionClient` `SubscriptionMemberClient` `domain.feed_ops()` | script, raw commands | Byte-level wire access |
| `WhitewaterError` | `.code` | `Refused` `StaleSession` `AmbiguousResult` `Conflict` `Unavailable` `LeaseConflict` |
| `FakeRiverbed` | — | In-process wire-contract test double |
