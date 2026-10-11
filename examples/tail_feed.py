"""Ephemeral consumption: an anonymous temporary Reader with server-side
tail + wait — the "subscribe" pattern for live views, UIs, and REPLs.
Nothing is provisioned; the reader vanishes when the process exits.

The loop:

    temporary_fetch(tail, wait_ms) -> records -> next_cursor -> repeat

wait_ms makes each call a long-poll: the Node holds the request until
records commit or the wait expires, then answers — so the loop does
NOT burn a request per poll interval; it sits inside the fetch.
"""

from whitewater import TemporaryFetchOptions, WhitewaterClient

client = WhitewaterClient(
    api_key="whitewater-local-development-admin-key",
    endpoints=[
        "http://localhost:7371",
        "http://localhost:7372",
        "http://localhost:7373",
    ],
)

client.create_domain("demo")
client.create_feed("demo.tail")

after = None  # None -> feed tail position

while True:
    # Blocks up to wait_ms server-side; returns early when records land.
    page = client.feeds.temporary_fetch(
        "demo.tail",
        TemporaryFetchOptions.tailed(after, limit=50, wait_ms=2500),
    )

    for record in page.records:
        print(f"{record.payload.decode()} @ {record.ingest_time_ns}")

    # Continue from the PAGE frontier, not the last record's cursor — the
    # page can be filtered or empty while the frontier still advanced.
    after = page.next_cursor
