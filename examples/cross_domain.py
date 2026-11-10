"""Cross-Domain Pipes — a Pipe is owned by one Domain but its endpoints
can live in another.

Business flow: the ``purchasing`` Domain records purchase orders; the
``warehouse`` Domain wants them relayed into its own ``inbound`` Feed for
fulfilment processing. Neither team reaches into the other's namespace
by naming convention — the warehouse declares a Pipe whose *source*
subscription points back at the purchasing Feed:

    purchasing.orders  --(purchasing.fulfil.sub)-->  warehouse.inbound

Two ways to cross a Domain boundary, both explicit:

    warehouse.create_subscription(
        "fulfil.sub", purchasing.feed("orders"))   # handle: absolute name
    warehouse.create_subscription(
        "fulfil.sub", Scoped.absolute("purchasing.orders"))

Plain strings always resolve inside the declaring Domain, so there is
no silent global namespace — a cross-Domain reference is a deliberate,
readable act.

Run against the dev cluster:

    WHITEWATER_ENDPOINTS=http://localhost:7371 python examples/cross_domain.py
"""

import os
import time

from whitewater import WhitewaterClient

client = WhitewaterClient(
    api_key=os.environ.get(
        "WHITEWATER_API_KEY", "whitewater-local-development-admin-key"
    ),
    endpoints=os.environ.get(
        "WHITEWATER_ENDPOINTS",
        "http://localhost:7371,http://localhost:7372,http://localhost:7373",
    ).split(","),
)

# ---- topology ------------------------------------------------------
# Two business namespaces. The purchasing team owns orders; the
# warehouse team owns inbound fulfilment work.

purchasing = client.create_domain("purchasing")
purchasing.create_feed("orders")

warehouse = client.create_domain("warehouse")
warehouse.create_feed("inbound")

# The warehouse declares both the Subscription and the Pipe. The
# Subscription SOURCE is the purchasing feed — a ``Feed`` handle carries
# its absolute Domain-qualified name, so this reads as a deliberate
# cross-Domain link rather than a relative-name accident.
warehouse.create_subscription("fulfil.sub", purchasing.feed("orders"))
warehouse.define_forward_pipe(
    "fulfil",                    # -> warehouse.fulfil
    "fulfil.sub",                # -> warehouse.fulfil.sub
    warehouse.feed("inbound"),   # -> warehouse.inbound
)

# Same declaration with the explicit absolute form:
#
#     from whitewater import Scoped
#     warehouse.create_subscription(
#         "fulfil.sub", Scoped.absolute("purchasing.orders"))

# ---- drive ------------------------------------------------------------
# Purchasing writes orders; the Riverbed relays them into
# warehouse.inbound under the warehouse-owned Pipe's durable frontier.

orders_writer = purchasing.feed("orders").writer("erp")
orders_writer.append("po-1001", {"sku": "WIDGET-9", "qty": 40})
orders_writer.append("po-1002", {"sku": "GADGET-2", "qty": 7})

inbound = warehouse.feed("inbound")
deadline = time.time() + 10
seen = []
while time.time() < deadline and len(seen) < 2:
    page = inbound.fetch(limit=10)
    seen.extend(m.key for m in page.records)
    if not page.records:
        time.sleep(0.25)
print("warehouse received:", seen)

# The Pipe is warehouse's: lifecycle, frontier, and catalog all live in
# the warehouse Domain, even though the source does not.
info = warehouse.pipe("fulfil").describe()
print(f"{info.name}: stage={info.stage} status={info.status}")
