# StateStore — coding guide

There is exactly one store: the **node-side StateStore** — durable,
RF3-replicated, Fjall-backed, and queryable from any Node. The SDK's
`StateStore` is a thin facade over it: it holds no rows, keeps no local
projection, and every operation is an HTTP call with freshness evidence.

```python
store = (StateStore.named("accounts.employees")
         .add_key("passport", unique=True)   # one row per value
         .add_key("department"))             # value -> many rows

client.state_stores.define(store)           # declares on the Control Plane
                                            # and returns it bound
```

`add_key` records declaration intent used by `define` — named keys are
declared at creation; at most `StateStore.MAX_NAMED_KEYS` (10), unique
or not. A named-key declaration only affects `define`; adding a key to
an existing store has no effect server-side.

## Writing

```python
store.put(b"user-1", b"profile-1",
          named={"passport": "P111", "department": "eng"})
store.put(b"user-2", b"profile-2",
          named={"department": "eng"},
          mutation_id=uuid4())               # caller-owned idempotency
store.delete(b"user-1")
```

- `put(key, payload, named=...)` commits one journaled `put_row`
  through the store's RF3 quorum. `key`/`payload` accept `bytes` or
  `str` (UTF-8).
- `named` values are **typed**: `str` -> text, `int` -> i64,
  `bool` -> bool, `bytes` -> bytes, `None` -> null. A list value
  carries one typed value per posting component. You never hand-encode
  `getBytes()` for ordinary strings/numbers.
- Named values must match the store's declared keys — the server
  refuses undeclared names. `unique` claims arbitrate
  first-committed-wins by journal order.
- `mutation_id` is the idempotency identity: retry a timed-out put
  with the same id and it dedupes; reuse it with different contents
  and it conflicts.

## Reading

```python
row = store.get(b"user-1")                   # StoreRow | None
row.payload                                  # bytes
row.freshness                                # applied_seq / committed_seq
                                             # / behind / served_by

rows = store.get_by("department", "eng")     # StoreRows — every match
rows.rows                                    # [StoreRow(key, payload, ...)]
rows.freshness.served_by                     # which Node answered
```

`get_by(named_key, value)` is the group/list lookup: one non-unique
named value resolves to **every** primary row carrying it. Pass
`strict=True` (with optional `wait_ms`) on `get`/`get_by` and the
serving replica bounded-waits for its applied journal to reach the
committed frontier rather than answering stale; non-strict reads still
report `freshness.behind` honestly.

## Iterating

```python
for row in store.iterate():                  # every row, primary-key order
    ...
page = store.page(limit=256)                 # one bounded page
page.next_key                                # continuation
page.pinned_seq                              # journal position the page
                                             # was pinned to
```

`iterate` pages `/v1/state-stores/iterate` — the same bounded,
ordered row set replica catch-up serves — so `pinned_seq` is the
iteration's freshness evidence.

## Lifecycle

```python
store.clear()                                # one journaled ClearAll
client.state_stores.drop(store.name)         # or store.drop()
```

- `clear` empties the store as one durable, idempotent mutation;
  replicas sweep rows and postings in bounded batches.
- `drop` tombstones the definition — the name cannot be recreated and
  every data path refuses. Rows already applied on replicas remain
  until a bounded reclaim pass.
- Feed-sourced stores: `client.state_stores.define(spec, source=
  "accounts.events")` materializes committed Feed history; named keys
  require a manual store until declared projections exist, and `sync`/
  `rebuild` are server-side operations, not SDK methods.

## What a StateStore facade is **not**

- **Not a local store** — there is no in-process copy to drift from
  the Nodes. Every read goes to a replica; freshness evidence tells
  you how current it was.
- **Not unique on non-unique keys** — `get_by` always returns a list.

## Errors

| Where | Type | Meaning / remedy |
|---|---|---|
| `add_key` #11 / duplicate / blank | `ValueError` | Declaration contract (10-key bound, distinct names). |
| `define` spec + `source` + named keys | `ValueError` | Named keys need a manual store — checked before the call. |
| Data op on unbound spec | `ValueError` | `StateStore.named` builds a spec; bind via `define` or `client.state_store(name)`. |
| Undeclared named key / missing store | `Refused` (400) | Server-side contract — fix the declaration. |
| `mutation_id` reused differently | `Conflict` (409) | Retry with the original contents or a fresh id. |
| No quorum / replica down | `Unavailable` (503) | Retry — the journal needs a quorum to commit. |
| `strict` read can't catch up | `Conflict` (`index_behind`) | Bounded wait expired; retry or drop `strict`. |
