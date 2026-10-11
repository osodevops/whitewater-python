"""Durable consumption: a named Reader owns an independent cursor on the
Riverbed, so progress survives client restarts — the "subscribe"
pattern for applications, versus the ephemeral tail in tail_feed.py.

The loop is explicit — Whitewater Readers pull rather than push:

    open -> [ fetch -> process -> ack ] -> close

ack() is the commit point: acknowledged progress is durable, so a
restarted client resumes after the last acked position. Process
records BEFORE acking them — ack-then-crash loses those records.
"""

import time

from whitewater import ReaderStart, WhitewaterClient

client = WhitewaterClient(
    api_key="whitewater-local-development-admin-key",
    endpoints=[
        "http://localhost:7371",
        "http://localhost:7372",
        "http://localhost:7373",
    ],
)

# One-time provisioning: the Reader declaration is durable Control
# Plane state, not a per-session object.
client.create_domain("demo")
client.create_feed("demo.tail")
client.create_reader("demo-reader", "demo.tail", ReaderStart.beginning())

reader = client.reader("demo-reader")

# open() fences this client as the session owner. Another client
# opening the same Reader supersedes this session (stale_epoch on the
# next fetch).
session = reader.open(64)

try:
    while True:
        # fetch() returns immediately with whatever is committed — a
        # Reader is not a long-poll. For "sit and wait" behavior, use
        # temporary_fetch(tail, wait_ms) or pace empty fetches.
        page = reader.fetch(session, 50)

        for record in page.records:
            print(f"{record.payload.decode()} @ {record.ingest_time_ns}")

        # Commit durable progress AFTER processing the page. The page's
        # delivered_cursor covers every range the fetch drew from — ack
        # it, not the last record's cursor.
        if page.delivered_cursor is not None:
            session = reader.ack(session, page.delivered_cursor)

        if not page.records:
            time.sleep(0.5)  # poll cadence for live reads
finally:
    reader.close(session)  # release the session fence
