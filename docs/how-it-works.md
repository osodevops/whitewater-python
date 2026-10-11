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
  `temporary_fetch` from `ReaderStart` until the frontier is drained.
  `sink` is a `RecordStore` (a named keyed in-memory map) or any
  `records -> None` callback.
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
