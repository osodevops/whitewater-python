"""Durable consumption with typed codecs: a named Reader owns an
independent cursor on the Riverbed, so progress survives client
restarts — the "subscribe" pattern for applications, versus the
ephemeral tail in tail_feed.py.

For a managed loop — auto-provisioning, retry policy, and a
close()-able handle — use ``feed.subscribe(...)`` instead of this
explicit form.

The loop below is explicit — Whitewater Readers pull rather than push:

    open -> [ fetch -> process -> ack ] -> close

ack() is the commit point: acknowledged progress is durable, so a
restarted client resumes after the last acked position. Process
records BEFORE acking them — ack-then-crash loses those records.
"""

import time
from dataclasses import dataclass

from whitewater import WhitewaterClient


@dataclass
class Order:
    id: str
    total: int


client = WhitewaterClient(
    api_key="whitewater-local-development-admin-key",
    endpoints=[
        "http://localhost:7371",
        "http://localhost:7372",
        "http://localhost:7373",
    ],
)

client.create_domain("demo")
client.create_feed("demo.orders")

# One binding carries the feed name + codecs. Order maps to JSON;
# codecs.of(...) binds protobuf/msgpack/custom formats the same way.
orders = client.feed("demo.orders", value_type=Order)

# Declares the Reader if missing (durable Control Plane state), opens
# the session, and owns the fence. Another client opening the same
# Reader supersedes this session (stale_epoch on the next fetch).
with orders.reader("demo-reader", capacity=64) as reader:
    while True:
        # fetch() returns immediately with whatever is committed — a
        # Reader is not a long-poll. For "sit and wait" behavior, use
        # feed.fetch with tail/wait_ms or pace empty fetches.
        page = reader.fetch(50)

        for message in page.records:
            print(f"{message.key} -> {message.value} @ {message.ingest_time_ns}")

        # Commit durable progress AFTER processing the page. The page's
        # delivered_cursor covers every range the fetch drew from — ack
        # it, not the last record's cursor.
        reader.ack()

        if not page.records:
            time.sleep(0.5)  # poll cadence for live reads
# close() releases the session fence
