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

Declaration is a control command on the owning Domain, not a process::

    ops.define_forward_pipe("relay", "relay.sub", "archive")

Every string resolves inside the declaring Domain; a ``Feed``/
``SubscriptionRef`` handle or ``Scoped.absolute`` points an endpoint at
another Domain (see cross_domain.py). The Pipe lifecycle is
operational::

    ops.pipe("enrich").pause()     # cooperative; frontier stays put
    ops.pipe("enrich").resume()    # resumes the durable frontier

Everything is named — Feeds, Stores, Subscriptions, Pipes. Partitions
and Nodes never appear in the surface.

Run against the dev cluster:

    WHITEWATER_ENDPOINTS=http://localhost:7371 python examples/declare_pipes.py
"""

import os
from datetime import timedelta

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

ops = client.create_domain("ops")
ops.create_feed("events")
ops.create_feed("archive")     # forward output
ops.create_feed("enriched")    # enrich output
ops.create_feed("misses")      # enrich quarantine target
ops.create_feed("panes")       # window emit feed

# The enrich lookup store (manual) and the window pane view — a window
# Pipe's output store must declare non-unique 'key' and 'pane' named
# keys so pane rows stay enumerable without composite row keys.
ops.define_state_store(ops.state_store("profiles"))
ops.define_state_store(
    ops.state_store("minute.totals").add_key("key").add_key("pane")
)
ops.create_subscription("relay.sub", "events")
ops.create_subscription("enrich.sub", "events")
ops.create_subscription("window.sub", "events")

# ---- declarations --------------------------------------------------------

# Forward: archive every delivered record unchanged.
ops.define_forward_pipe("relay", "relay.sub", "archive")

# Enrich: look up ops.profiles by the record's JSON "user_id"; misses go
# to the quarantine feed instead of emitting unmatched output.
ops.define_enrich_pipe(
    "enrich",
    "enrich.sub",
    "enriched",
    "profiles",
    PipeOperation.enrich(
        Extractor.json("user_id"),
        on_miss=EnrichMiss.QUARANTINE,
    ),
    quarantine_feed="misses",
)

# Window: sum "amount" per record Key into 60s event-time panes on
# ops.minute.totals; panes close 5s late-bound and emit to ops.panes.
ops.define_window_pipe(
    "rollups",
    "window.sub",
    "minute.totals",
    PipeOperation.window(
        timedelta(minutes=1),
        Extractor.record_key(),
        WindowAggregation.sum(Extractor.json("amount")),
        time=WindowTime.EVENT,
        max_late=timedelta(seconds=5),
        expire=timedelta(hours=1),
    ),
    emit_feed="panes",
)

# ---- lifecycle --------------------------------------------------------------
# Described stage, pause, and resume are catalog operations — the drive
# itself is never in this process.

pipe = ops.pipe("enrich")
info = pipe.describe()
print(f"{info.name}: stage={info.stage} status={info.status}")

pipe.pause()    # in-flight effects finish; frontier holds
pipe.resume()   # drives resume from the durable frontier

for declared in ops.pipe_infos():
    print(f"{declared.name}: {declared.stage}")
