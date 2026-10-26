# Demo: order-fulfillment service

A complete microservice shape: provision a Domain + Feed, append
orders, and pump them through a processor with at-least-once
semantics, a dead-letter path, and graceful shutdown.

The contract in one line: **return = processed, raise = replay** — the
SDK acks a page only after every record in it has been handled.

```python
import os
import signal
import whitewater
from whitewater import ReaderStart, WhitewaterClient

client = WhitewaterClient(
    api_key=os.environ["WHITEWATER_API_KEY"],
    endpoints=["http://localhost:7371", "http://localhost:7372"])

# ---- provision ------------------------------------------------------
client.create_domain("shop")
shop = client.get_domain("shop")
shop.create_feed("orders")
shop.create_feed("orders.dlq")            # dead-letter feed

# ---- produce (usually a different service) --------------------------
# Bind codecs on the Feed handle — the SDK serializes, never b"...".
orders = shop.feed("orders", value_type=str)          # JSON text payloads
writer = shop.create_writer(orders).writer_id("orders.ingest").open()
writer.append("order-1", '{"total":42}')

# ---- consume ---------------------------------------------------------
dlq = shop.feed("orders.dlq", value_type=str)
dead_letter = shop.create_writer(dlq).writer_id("fulfillment.dlq").open()
reader = (shop.create_reader(orders)
          .reader_id("fulfillment")          # durable id — survives restarts
          .reader_start_point(ReaderStart.BEGINNING)   # first-declare only
          .max_page_size(250)                # max records per fetch page
          .open())

def handle(m):
    try:
        orders_db.upsert(order_id(m), order(m))       # idempotent side effect
    except Exception as e:
        if permanent(e):
            dead_letter.append(m.key, m.value)         # quarantine → continue → acked
            return
        raise                                          # transient → stop → replay

# SDK-owned thread — returns immediately; the service keeps running.
pump = reader.listen(handle)

# ---- shutdown ---------------------------------------------------------
# pump.stop() stops the thread + closes the session; the in-flight
# page stays unacked and replays on the next open. `with pump:` does
# the same via the context manager.
signal.signal(signal.SIGINT, lambda *_: pump.stop())

# Supervise: a dead pump surfaces through pump.failure — restart or
# alert from a health check:
#   if pump.failure is not None: restart()
```

## Notes

- `reader_id` is durable application state — the Kafka `groupId`
  analogue. `reader_start_point` applies only on first declare;
  restarts resume the acknowledged frontier.
- Acks are per fetched page. A crash mid-page replays the whole page —
  that's what makes it at-least-once. Process idempotently.
- `max_page_size(250)` bounds both the page and the replay window.
- `for m in reader:` iterates with the same ack-per-page semantics;
  `reader.for_each(h)` is the blocking callback form. Manual mode:
  `fetch()` → process → `ack()`. Never ack before processing — that's
  at-most-once.
