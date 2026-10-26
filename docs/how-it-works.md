# How the Python SDK works

Everything a caller can do enters through `WhitewaterClient` — and
every HTTP call funnels through one `_transport.Transport`. There are
no other network paths.

## Layers

```
WhitewaterClient ─┬─ wcl / commands / healthy             → POST /v1/admin/*
                  ├─ create_feed / describe_feed / …      → typed WCL builders
                  ├─ feeds   → FeedClient                 → /v1/feeds/*, /v1/readers/temporary/fetch
                  ├─ writer(name, epoch)                  → /v1/writers/append[-batch]
                  ├─ reader(name)                         → /v1/readers/open|fetch|ack|close
                  └─ subscription_member(name)            → /v1/subscriptions/members/*
       │                      │
    Transport            _wire.py (codec)
  (endpoints,         (request building,
   auth, errors)       response mapping)
```

### `Transport`

Holds the endpoint list and API key. `post`/`get` round-robin the
endpoints, attach `authorization: Bearer <key>`, and map every
non-2xx to a `WhitewaterError` subtype by the wire `code` field:
`StaleSession`, `Conflict`, `LeaseConflict`, `Refused`, `Unavailable`
(unreachable/timeout), and `AmbiguousResult` (5xx without a code —
the server may or may not have applied the write; **retry with the
same `request_id`**, never a new one).

### `_wire.py`

The only place request bodies are built and responses decoded. Keys,
payloads, and metadata values are base64 `bytes`; `event_time_ns`/
`ingest_time_ns` are `int` nanoseconds on decimal strings (Python
ints are unbounded, so precision is never lost). `Cursor` is an
opaque `str` subclass — never parsed or ordered.

## Resource management — two doors, one room

`client.wcl(script)` sends raw WCL to `POST /v1/admin/wcl`. The typed
methods are thin builders over it —
`client.create_feed("orders.events")` serializes
`CREATE FEED orders.events` and calls `wcl` internally.

Two validators run before interpolation, raising `ValueError`
locally:

- `_dotted` — domains/feeds: `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`,
  ≤512 bytes.
- `_resource` — writers/readers/subscriptions/roles:
  `^[A-Za-z][A-Za-z0-9_-]{0,127}$`.

`ReaderStart` supplies `START AT` positions: `beginning()`, `now()`,
`cursor(c)` (quotes escaped), `timestamp_ns(ns)`.

Every management call returns `ControlExecution` — `request_id`,
`revision`, `authority`, `warning`, and one `StatementResult` per WCL
statement.

### WCL by example

Raw script — several statements commit as one request (one
`request_id` dedupes the whole batch):

```python
client.wcl("""
    CREATE DOMAIN shop;
    CREATE FEED shop.orders;
    CREATE WRITER orders.writer TO shop.orders;
    OPEN WRITER SESSION orders.writer;
    CREATE READER audit FROM shop.orders START AT BEGINNING;
    SHOW FEEDS;
""")
```

The same calls through the typed builders — each serializes one WCL
statement and calls `wcl` internally:

```python
client.create_domain("shop")                       # CREATE DOMAIN shop;
shop = client.get_domain("shop")                   # verifies + materializes
shop.create_feed("orders")                         # CREATE FEED shop.orders;
client.create_writer("orders.writer", "shop.orders")
client.open_writer_session("orders.writer")        # epoch comes back in .data
client.create_reader("audit", "shop.orders", ReaderStart.beginning())
client.list_feeds()                                # SHOW FEEDS;
client.seek_reader("audit", ReaderStart.now())     # SEEK READER ... TO NOW;
client.drop_feed("shop.orders")                    # DROP FEED shop.orders;
```

`client.create_feed("shop.orders")` with the fully-qualified name is
the flat-name equivalent; `domain.create_feed("orders")` composes the
Domain prefix for you.

### Typed Domains — the friendly door

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`), so
`client.domain(name)` returns a strongly-typed facade that navigates the
namespace instead of concatenating dotted names:

- `client.domain(name)` — a lazy handle. `domain.feed("orders")` is
  pure name composition (`shop.orders`); no I/O until a catalog
  accessor runs. `domain(name)` never fails for a missing Domain.
- `client.get_domain(name)` — `domain()` plus an immediate snapshot
  load: the Domain's existence is verified up front.
- The first catalog accessor issues ONE batched `commands` call
  (`describe domain` + `show` for feeds/readers/writers/subscriptions/
  pipes/indexes/state_stores), joins rows by `space_id`/`feed_id`, and
  caches the snapshot. `domain.refresh()` re-pulls.
- Scoped views: `feed_names()`, `feeds(prefix)`, `readers()`,
  `writers()`, `subscriptions()`, `pipes()`, `indexes()`,
  `state_stores()`, `describe()`, `history_policy`, `domain_id`,
  `revision`.

`client.create_reader(domain, feed)` / `create_writer(domain, feed)`
return fluent builders instead of executing WCL directly — the Domain
overload is detected by type, so the legacy `create_reader(name, feed,
start)` string form is unchanged:

```python
reader = (client.create_reader(shop, shop.feed("orders"))
          .reader_id("fulfillment")                    # required — durable id
          .reader_start_point(ReaderStart.BEGINNING)   # or .END, .cursor(c), .timestamp(dt)
          .capacity(256)
          .open())                                     # FeedReader — decoded messages

writer = (client.create_writer(shop, "orders")         # str names are relative
          .writer_id("orders.ingest")
          .open())                                     # FeedWriter
```

Builder semantics:

- `reader_id`/`writer_id` are required — the durable resource identity.
- `reader_start_point` applies only on first declaration; an existing
  Reader resumes its acknowledged frontier.
- `.declare()` provisions without opening a session (fails when the
  name is taken); `.open()` declares-when-missing then opens.
- A `Feed` object bound to another Domain fails locally — before any
  network call. String names are always relative to the Domain.

`client.commands([...])` is the second door — the typed JSON command
path for what WCL does not spell. Declaring a StateStore with named
keys is the current example (there is no `DEFINE STATESTORE` WCL
statement yet):

```python
client.commands([{
    "command": "define_state_store",
    "name": "accounts.employees",
    "source": {"kind": "manual"},
    "named_keys": [
        {"name": "passport", "unique": True},
        {"name": "department", "unique": False},
    ],
}])
```

(`client.state_stores.define(spec)` posts exactly this body for you.)

`DROP` is spelled in WCL for every resource including stores:
`client.wcl("DROP STATESTORE accounts.employees")` ≡ `store.drop()`.

## Reading

Three shapes, in increasing durability:

1. **`client.feeds.records(feed, limit, after)`** — one-shot
   committed history.
2. **`client.feeds.temporary_fetch(feed, options)`** — anonymous
   ephemeral reader; `tail` + `wait_ms` is a server-side long-poll
   returning early when records arrive. Continue with `next_cursor`,
   never the last record's cursor.
3. **`client.reader(name)`** — durable independent cursor:
   `open` → `fetch` → `ack(delivered_cursor)` → `close`. Progress
   survives restarts.

Two managed wrappers sit on top of those primitives:

- **`feeds.load(feed, start, sink)`** — one-shot bulk read: pages
  `temporary_fetch` from `ReaderStart` until the frontier is drained;
  `sink` is any `records -> None` callback.
- **`client.state_store(name)` / `StateStore.named(name)`** — the thin
  facade over the node-side StateStore (durable, RF3, Fjall): declare
  with `client.state_stores.define(spec)` after `add_key(name, unique=)`
  (up to 10 named secondary keys), then `put(key, payload, named={...})`
  with typed values, `get(key, strict=)`, `get_by(named_key, value)`
  (non-unique — returns `StoreRows`), `delete`, `iterate`, `page`,
  `clear`, `drop`. The facade holds no rows; reads carry freshness
  evidence (`applied_seq`/`committed_seq`/`behind`/`served_by`).
- **`client.subscribe(reader, feed, start, on_records[, on_error])`** —
  a durable consume loop on a background thread: provisions the Reader
  when missing, then `open` → [`fetch` → `on_records` → `ack`] →
  `close`. Transient errors retry; `StaleSession` and hard refusals
  terminate via `on_error`/`sub.failure`. The non-daemon thread keeps
  the process alive until `sub.close()` (or the `with` block exits).

`client.subscription_member(sub)` is the lease vocabulary for
cooperating consumers: `join` → `claim`/`renew`/`release` → `fetch`
→ `ack`.

## Writing

- **Named session**: `open_writer_session(name)` returns a
  `session_epoch` in `results[0].data`; bind `client.writer(name,
  epoch)` and `append` with a stable `request_id` per logical write.
- **Ad-hoc**: `client.feeds.append(feed, record)` — ephemeral writer
  identity; same request-id dedup rule.

## Testing

Unit tests stub the wire with `http.server.HTTPServer`, asserting
serialized statements and response mapping — no live Riverbed
required. `tests/conformance/` replays the shared
`sdk/spec/conformance` fixtures when `WHITEWATER_ENDPOINTS` +
`WHITEWATER_SPEC_DIR` are set; skipped otherwise.
