# RecordStore and load_store — coding guide

A `RecordStore` is a named, thread-safe, in-memory projection of Feed
records. Fill it once with `client.load_store(store).from_feed` (bulk
history) and keep it warm with `client.subscribe` (live tail) — the
Whitewater equivalent of a local materialized view.

## Store anatomy

Two structures per store:

- **Primary map** — `primary key -> Record`. One row per key; a new row
  for an existing key replaces it.
- **Named-key posting maps** — one per declared named key, conceptually
  `employee-store.passport`: `named value -> set of primary keys`.
  Named keys are **not unique** — many rows may share one named value,
  so lookups return a list.

```python
store = (RecordStore.named("employee-store")
         .add_key("passport")           # declare before use
         .add_key("department"))        # at most RecordStore.MAX_NAMED_KEYS (10)
```

`add_key` returns the store for chaining. An eleventh key raises
`ValueError("a RecordStore supports at most 10 named keys")`; blank
names raise `ValueError("named key is required")`.

## Loading history: `client.load_store(store).from_feed(...)`

```python
loaded = client.load_store(store).from_feed(
    "hr.employees",
    ReaderStart.beginning(),            # or .now(), .cursor(c), .timestamp_ns(ns)
    lambda s, record: s.upsert(record)
        .with_key(record.key)                          # primary key
        .with_named_key("passport", record.metadata["pp"])
        .with_named_key("department", "eng"))          # str values are UTF-8
```

Semantics, in order:

1. Pages `temporary_fetch` from `start` until the frontier is drained —
   a **one-shot bounded read**, not a tail.
2. For every record the projection runs `s.upsert(record)`.
3. `with_key(bytes|str|None)` chooses the primary key. Defaults to
   `record.key`; pass anything — a composite `"dept:emp-1"` — and the
   row re-keys (old postings are stripped first). `None` falls back to
   the record's own key/cursor.
4. `with_named_key(name, bytes|str)` attaches one named-key value; call
   it once per declared key you want indexed. An **undeclared** named
   key raises `ValueError` — a projection bug fails the load, never
   silently writes an unindexed row.
5. Returns the count of records read (not rows written — re-keys
   replace, they don't duplicate).

Skip `with_named_key` and the row lands in the primary map only —
fully readable, just not indexable by that named key.

`feeds.load(feed, start, sink)` remains the lighter "dump history"
path; `sink` may be the store itself (rows key on `record.key`, cursor
fallback for unkeyed records) or a `records -> None` batch callback.

## Keeping the store warm: `subscribe`

```python
def project(records):
    for r in records:
        store.upsert(r).with_key(r.key).with_named_key(
            "department", r.metadata["department"])

with client.subscribe("hr-view", "hr.employees",
                      ReaderStart.now(),          # only new records
                      project,
                      on_error=lambda e: print("ended:", e)) as sub:
    main_work()   # non-daemon thread keeps the process alive
# leaving the with-block calls sub.close()
```

Delivery is **at-least-once**: records land in `on_records`, then the
loop acks `delivered_cursor`. A crash between callback and ack replays —
make the projection idempotent (upsert semantics already are).

## Reading the store

```python
store.get("emp-1")                     # Record | None (str keys = UTF-8)
store.get(b"emp-1")                    # bytes overload
store.get_by("passport", "P123")       # [Record, ...] — every match
store.get_by("undeclared", "x")        # raises ValueError
len(store)                             # row count
store.named_keys                       # declared keys, in order
store.name                             # "employee-store"
store.entries()                        # [StoreEntry(primary_key, record)]
store.values()                         # [Record, ...]
store.for_each(lambda e: export(e.record))
```

## Mutating outside a load

```python
store.put(record)                      # primary-key upsert, no named keys
store.put_all(records)                 # subscribe-ready sink shape
store.upsert(record).with_key("custom")
store.delete("emp-1")                  # removes row + all its postings
store.clear()                          # empties everything
```

`delete` and `clear` strip named-key postings too — a deleted row never
lingers in a `get_by` result.

## Copying and parallel work

```python
replica = RecordStore.named("employee-backup")
store.copy(replica)                    # rows + named postings; declares
                                       # missing keys on the target
store.for_each_parallel(3, lambda entry: rebuild(entry))
```

`for_each_parallel` fans a stable snapshot over a `ThreadPoolExecutor`
with `max_workers=workers` — **threads, not processes**: they
interleave I/O-bound work, but CPU-bound callbacks stay serialized by
the GIL. For CPU-heavy projections, use `multiprocessing` yourself.
`workers < 1` raises `ValueError`; a callback's exception propagates
after the pool drains.

## Error handling reference

| Where | Type | Meaning / remedy |
|---|---|---|
| `add_key` #11 / blank | `ValueError` | Store key budget (10) or empty name — construction-time contract. |
| `get_by`/`with_named_key` undeclared | `ValueError` | Programming error — declare the key or fix the name. |
| `from_feed` mid-read | `WhitewaterError` (usually `Unavailable`) | Transport refused a page; retry the call — re-upserts are idempotent. |
| `subscribe` transient | retried internally | `Unavailable`/`AmbiguousResult` backoff inside the loop. |
| `subscribe` terminal | `on_error` + `sub.failure` | `StaleSession` (another client opened your Reader — pick a unique name or take over deliberately), `Refused`, `Conflict`. The loop is dead; inspect, fix, re-subscribe. |

Catch precise subclasses — `except StaleSession:` — and read
`error.status`/`error.code` for wire detail.

## What a RecordStore is **not**

- **Not durable** — it dies with the process. Rebuild is cheap:
  `from_feed` from `ReaderStart.beginning()` (or a snapshot Cursor).
- **Not a server StateStore** — the server-side replicated store model
  is a separate, later seam. This is the client-side projection.
- **Not unique on named keys** — `get_by` always returns a list, even
  when it happens to hold one row.
