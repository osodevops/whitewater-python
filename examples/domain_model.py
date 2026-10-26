"""The strongly-typed Domain facade — navigate the namespace instead of
composing dotted names by hand.

    shop = client.get_domain("shop")    # materialized catalog view
    orders = shop.feed("orders")        # typed Feed handle, zero I/O
    reader = (client.create_reader(shop, orders)
              .reader_id("fulfillment")
              .reader_start_point(ReaderStart.BEGINNING)
              .open())

Run a Riverbed (scripts/test-*.py bring one up on 7371–7373), then:

    python examples/domain_model.py
"""

from dataclasses import dataclass

from whitewater import ReaderStart, WhitewaterClient


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

client.create_domain("shop")
client.create_feed("shop.orders")
client.create_feed("shop.returns")

# get_domain() verifies + materializes now; domain() is the lazy handle.
shop = client.get_domain("shop")

print("feeds:", shop.feed_names())
print("order-prefix feeds:", [f.name for f in shop.feeds("order")])

orders = shop.feed("orders", value_type=Order)  # typed Feed, zero I/O

writer = (
    client.create_writer(shop, orders)
    .writer_id("orders.ingest")
    .open()
)
writer.append("order-1", Order("order-1", 42))

with (
    client.create_reader(shop, orders)
    .reader_id("fulfillment")  # durable id — the Kafka groupId analogue
    .reader_start_point(ReaderStart.BEGINNING)  # first-declare only
    .capacity(256)
    .open()
) as reader:
    for record in reader.fetch().records:
        print("read", record.key, record.value)
    reader.ack()  # durable progress

shop.refresh()  # re-pull the catalog snapshot
print("readers:", shop.readers())
print("writers:", shop.writers())
