"""Managed Pipes — server-side processing declared once, driven forever.

A Pipe consumes a durable Subscription and applies one operation to each
delivered record; the Riverbed owns the drive loop, effect journaling,
and replay, so there is no consumer code to run:

    Forward  — copy each record to an output Feed unchanged
    Enrich   — point-lookup a StateStore row by a projected Key and stamp
               the payload into output metadata (journaled, so replayed
               outputs are byte-identical)
    Window   — accumulate records into fixed panes of a managed
               StateStore view

Declaration is a control command, not a process::

    client.define_forward_pipe("ops.relay", "relay-sub", "ops.archive")

then the Pipe lifecycle is operational::

    client.pipe("ops.enrich").pause()     # cooperative; frontier stays put
    client.pipe("ops.enrich").resume()    # resumes the durable frontier

Everything is named — Feeds, Stores, Subscriptions, Pipes. Partitions
and Nodes never appear in the surface.

Run against the dev cluster:

    WHITEWATER_ENDPOINTS=http://localhost:7371 python examples/declare_pipes.py
"""

import os

from whitewater import (
    EnrichMiss,
    Extractor,
    PipeOperation,
    WhitewaterClient,
    WindowAggregation,
    WindowTime,
)

client = WhitewaterClient(
    api_key=os.environ.get(
        "WHITEWATER_API_KEY", "whitewater-local-development-admin-key"
    ),
    endpoints=os.environ.get(
        "WHITEWATER_ENDPOINTS",
        "http://localhost:7371,http://localhost:7372,http://localhost:7373",
    ).split(","),
)

# ---- topology: one source feed, three subscriptions ---------------------
# Each Pipe drives its own Subscription, so relay/enrich/window keep
# independent durable frontiers over the same input.

client.create_domain("ops")
client.create_feed("ops.events")
client.create_feed("ops.archive")     # forward output
client.create_feed("ops.enriched")    # enrich output
client.create_feed("ops.misses")      # enrich quarantine target
client.create_feed("ops.panes")       # window emit feed

# ops.profiles (enrich lookup) and ops.minute.totals (window panes) are
# declared StateStores — see examples/enrich_users.py for store setup.
client.create_subscription("relay-sub", "ops.events")
client.create_subscription("enrich-sub", "ops.events")
client.create_subscription("window-sub", "ops.events")

# ---- declarations --------------------------------------------------------

# Forward: archive every delivered record unchanged.
client.define_forward_pipe("ops.relay", "relay-sub", "ops.archive")

# Enrich: look up ops.profiles by the record's JSON "user_id"; misses go
# to the quarantine feed instead of emitting unmatched output.
client.define_enrich_pipe(
    "ops.enrich",
    "enrich-sub",
    "ops.enriched",
    "ops.profiles",
    PipeOperation.enrich(
        Extractor.json("user_id"),
        on_miss=EnrichMiss.QUARANTINE,
    ),
    quarantine_feed="ops.misses",
)

# Window: sum "amount" per record Key into 60s event-time panes on
# ops.minute.totals; panes close 5s late-bound and emit to ops.panes.
client.define_window_pipe(
    "ops.rollups",
    "window-sub",
    "ops.minute.totals",
    PipeOperation.window(
        60_000_000_000,
        Extractor.record_key(),
        WindowAggregation.sum(Extractor.json("amount")),
        time=WindowTime.EVENT,
        max_late_ns=5_000_000_000,
        expire_ns=3_600_000_000_000,
    ),
    emit_feed="ops.panes",
)

# ---- lifecycle --------------------------------------------------------------
# Described stage, pause, and resume are catalog operations — the drive
# itself is never in this process.

pipe = client.pipe("ops.enrich")
info = pipe.describe()
print(f"{info.name}: stage={info.stage} status={info.status}")

pipe.pause()    # in-flight effects finish; frontier holds
pipe.resume()   # drives resume from the durable frontier

for declared in client.pipes():
    print(f"{declared.name}: {declared.stage}")
