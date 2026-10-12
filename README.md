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
| `client.feeds` | Committed record reads, anonymous temporary fetches (`tail`/`wait_ms`/`new_only`), ad-hoc appends, `load(feed, start, sink)` one-shot history reads |
| `client.subscribe(reader, feed, start, on_records)` | Managed durable consume: provisions the Reader, runs open → fetch → deliver → ack on a background thread; `sub.close()` (or `with`) to stop |
| `client.state_store(name)` | Thin facade over the node-side StateStore: `put`/`get`/`get_by`/`delete`/`iterate`/`clear`/`drop` with typed named keys and freshness evidence |

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
read when you don't need named-key projections. See
[docs/stores.md](docs/stores.md) for the full store guide.

Errors surface as `WhitewaterError` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, ...). `Unavailable` covers transient refusals;
`AmbiguousResult` marks operations that may have committed — retry
through the same `request_id`, never blindly re-issue.

Cursors are opaque strings: never parsed, ordered, or fabricated.
`event_time_ns`/`ingest_time_ns` are Python `int`s (arbitrary precision)
and travel as decimal strings on the wire — the full signed 64-bit
nanosecond range round-trips exactly.

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
