"""``client.domain(name)`` / ``client.get_domain(name)`` return the
strongly-typed Domain facade: scoped catalog views, typed Feed handles,
and fluent Reader/Writer builders bound to the Domain. These tests drive
the facade over real loopback HTTP against ``FakeRiverbed``."""

import pytest

from whitewater import (
    Domain,
    FakeRiverbed,
    Feed,
    ReaderStart,
    Refused,
    WhitewaterClient,
    FeedSchema,
    SchemaDecl,
)


@pytest.fixture
def client():
    with FakeRiverbed() as fake:
        yield fake.client()


def seeded(client: WhitewaterClient) -> Domain:
    client.create_domain("shop")
    client.create_feed("shop.orders")
    client.create_feed("shop.returns")
    client.create_feed("shop.orders.daily")
    client.create_feed("other.events")
    return client.get_domain("shop")


class TestDomainHandles:
    def test_domain_is_lazy(self, client):
        # No catalog read until an accessor needs the snapshot: a handle
        # on a missing Domain still composes Feed names locally.
        domain = client.domain("unmade")
        feed = domain.feed("orders")
        assert feed.name == "unmade.orders"
        with pytest.raises(Refused):
            domain.feed_names()

    def test_get_domain_materializes(self, client):
        domain = seeded(client)
        assert domain.name == "shop"
        assert domain.domain_id
        assert domain.revision > 0
        assert domain.describe()["status"] == "active"

    def test_get_domain_refuses_missing(self, client):
        with pytest.raises(Refused):
            client.get_domain("ghost")

    def test_feed_names_scoped_to_the_domain(self, client):
        domain = seeded(client)
        assert domain.feed_names() == [
            "shop.orders",
            "shop.orders.daily",
            "shop.returns",
        ]

    def test_feeds_prefix_filters_relative_names(self, client):
        domain = seeded(client)
        assert [f.name for f in domain.feeds()] == [
            "shop.orders",
            "shop.orders.daily",
            "shop.returns",
        ]
        assert [f.name for f in domain.feeds("order")] == [
            "shop.orders",
            "shop.orders.daily",
        ]
        assert [f.name for f in domain.feeds("returns")] == ["shop.returns"]

    def test_refresh_pulls_new_rows(self, client):
        domain = seeded(client)
        assert "shop.payments" not in domain.feed_names()
        client.create_feed("shop.payments")
        domain.refresh()
        assert "shop.payments" in domain.feed_names()

    def test_scoped_resource_listings(self, client):
        domain = seeded(client)
        client.create_writer("orders.ingest", "shop.orders")
        client.create_reader("fulfillment", "shop.orders")
        client.create_subscription("shop.subs.orders", "shop.orders")
        client.create_writer("foreign.writer", "other.events")
        domain.refresh()
        assert domain.writers() == ["orders.ingest"]
        assert domain.readers() == ["fulfillment"]
        assert domain.subscriptions() == ["shop.subs.orders"]

    def test_feed_compose_relative_and_qualified(self, client):
        domain = client.domain("shop")
        assert domain.feed("orders").name == "shop.orders"
        assert domain.feed("shop.orders").name == "shop.orders"
        assert domain.get_feed("orders").name == "shop.orders"

    def test_feed_binds_codecs(self, client):
        import whitewater

        domain = client.domain("shop")
        feed = domain.feed("orders", key_type=str)
        assert isinstance(feed, Feed)


class TestReaderBuilder:
    def test_open_declares_reader_and_reads(self, client):
        client.create_feed("shop.orders")
        writer = (
            client.create_writer(client.domain("shop"), "orders")
            .writer_id("ingest")
            .open()
        )
        writer.append("order-1", b'{"total": 9}')

        reader = (
            client.create_reader(client.domain("shop"), "orders")
            .reader_id("fulfillment")
            .reader_start_point(ReaderStart.BEGINNING)
            .open()
        )
        page = reader.fetch()
        assert page.records
        assert page.records[0].key == "order-1"
        reader.close()

    def test_string_feed_resolves_relative(self, client):
        client.create_feed("shop.orders")
        builder = client.create_reader(client.domain("shop"), "orders")
        assert builder._feed.name == "shop.orders"

    def test_reader_id_is_required(self, client):
        client.create_feed("shop.orders")
        with pytest.raises(ValueError, match="reader_id"):
            client.create_reader(client.domain("shop"), "orders").open()

    def test_cross_domain_feed_refused_locally(self, client):
        client.create_domain("shop")
        client.create_domain("other")
        client.create_feed("other.events")
        foreign = client.domain("other").feed("events")
        with pytest.raises(ValueError, match="outside Domain"):
            client.create_reader(client.domain("shop"), foreign)

    def test_string_feed_names_are_relative(self, client):
        # Strings compose inside the Domain ("orders" → "shop.orders");
        # the cross-domain refusal applies to Feed objects bound outside.
        client.create_domain("shop")
        builder = client.create_reader(client.domain("shop"), "other.events")
        assert builder._feed.name == "shop.other.events"

    def test_start_point_applies_on_first_declare(self, client):
        client.create_feed("shop.orders")
        shop = client.domain("shop")
        client.create_reader(shop, "orders").reader_id("r1").declare()
        described = client.describe_reader("r1")
        assert described.results[0].data["start"]["kind"] == "beginning"

    def test_declare_fails_when_already_declared(self, client):
        client.create_feed("shop.orders")
        shop = client.domain("shop")
        client.create_reader(shop, "orders").reader_id("r1").declare()
        with pytest.raises(Refused):
            client.create_reader(shop, "orders").reader_id("r1").declare()


class TestWriterBuilder:
    def test_open_declares_writer_and_appends(self, client):
        client.create_feed("shop.orders")
        writer = (
            client.create_writer(client.domain("shop"), "orders")
            .writer_id("orders.ingest")
            .open()
        )
        writer.append("k1", b"payload")

    def test_writer_id_is_required(self, client):
        client.create_feed("shop.orders")
        with pytest.raises(ValueError, match="writer_id"):
            client.create_writer(client.domain("shop"), "orders").open()


class TestReaderStart:
    def test_constants(self):
        assert ReaderStart.BEGINNING.wcl == "BEGINNING"
        assert ReaderStart.END.wcl == "NOW"

    def test_timestamp_from_datetime(self):
        import datetime

        start = ReaderStart.timestamp(
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        )
        assert start.wcl == f"TIMESTAMP {1704067200 * 1_000_000_000}"

    def test_timestamp_requires_aware_datetime(self):
        import datetime

        with pytest.raises(ValueError):
            ReaderStart.timestamp(datetime.datetime(2024, 1, 1))


def test_create_feed_scopes_inside_the_domain(client):
    """shop.create_feed("orders") issues CREATE FEED shop.orders."""
    client.create_domain("shop")
    domain = client.get_domain("shop")
    domain.create_feed("orders")
    domain.refresh()
    assert domain.feed("orders").name == "shop.orders"
    assert "shop.orders" in domain.feed_names()
    # A bare segment can't escape the Domain — relative names compose
    # under shop., never into a sibling namespace.
    assert domain.feed("x.y").name == "shop.x.y"


def test_domain_hanging_builders_compose_and_open(client):
    """shop.create_writer/create_reader resolve relative names — the
    service shape hangs entirely off the Domain."""
    client.create_domain("shop")
    shop = client.get_domain("shop")
    shop.create_feed("orders")

    writer = shop.create_writer("orders").writer_id("ingest").open()
    writer.append("order-1", b"1")

    reader = (shop.create_reader("orders")
              .reader_id("fulfillment")
              .reader_start_point(ReaderStart.BEGINNING)
              .open())
    assert reader.fetch().records[0].key == "order-1"
    reader.close()


def test_reader_for_each_acks_handled_pages(client):
    client.create_feed("shop.orders")
    shop = client.domain("shop")
    writer = shop.create_writer("orders").writer_id("w").open()
    writer.append("a", b"1")
    writer.append("b", b"2")

    reader = (shop.create_reader("orders")
              .reader_id("pump")
              .reader_start_point(ReaderStart.BEGINNING)
              .capacity(1)
              .open())
    seen = []
    reader.for_each(lambda m: seen.append(m.key) or len(seen) < 2)
    assert seen == ["a", "b"]
    assert reader.acknowledged_cursor is not None   # page 1 committed
    reader.close()

    # The unacked tail replays on the next open.
    again = shop.create_reader("orders").reader_id("pump").open()
    assert again.fetch().records[0].key == "b"
    again.close()


def test_reader_iterates_records_per_page(client):
    client.create_feed("shop.orders")
    shop = client.domain("shop")
    shop.create_writer("orders").writer_id("w").open().append("a", b"1")
    reader = (shop.create_reader("orders")
              .reader_id("iter")
              .reader_start_point(ReaderStart.BEGINNING)
              .open())
    for message in reader:
        assert message.key == "a"
        break                       # page unacked → replays later
    reader.close()


def test_feed_schema_layers_feed_over_domain(client):
    client.wcl("""
        CREATE DOMAIN shop;
        CREATE FEED shop.orders;
        CREATE FEED shop.returns;
    """)
    client.set_domain_schema(
        "shop", FeedSchema(value=SchemaDecl("json", subject="order-v1")))
    client.set_feed_schema(
        "shop.orders", FeedSchema(key=SchemaDecl("text")))

    shop = client.get_domain("shop")
    assert shop.schema().value.format == "json"
    assert shop.schema().value.subject == "order-v1"

    effective = shop.feed_schema("shop.orders")
    assert effective.key.format == "text"
    assert effective.value.format == "json"          # inherited
    declared = shop.declared_feed_schema("shop.orders")
    assert declared.key.format == "text"
    assert declared.value is None

    inherited = shop.feed_schema("shop.returns")
    assert inherited.value.format == "json"
    assert inherited.key is None
    assert shop.declared_feed_schema("shop.returns") is None


def test_clear_feed_schema_reverts_to_inheritance(client):
    client.wcl("CREATE DOMAIN shop; CREATE FEED shop.orders;")
    client.set_domain_schema("shop", FeedSchema(value=SchemaDecl("json")))
    client.set_feed_schema("shop.orders", FeedSchema(value=SchemaDecl("avro")))

    shop = client.get_domain("shop")
    assert shop.feed_schema("shop.orders").value.format == "avro"

    client.clear_feed_schema("shop.orders")
    shop.refresh()
    assert shop.feed_schema("shop.orders").value.format == "json"
    assert shop.declared_feed_schema("shop.orders") is None


def test_set_schema_via_wcl_and_show_schemas(client):
    client.wcl("""
        CREATE DOMAIN shop;
        CREATE FEED shop.orders;
        SET SCHEMA ON DOMAIN shop VALUE json SUBJECT 'order-v1';
        SET SCHEMA ON FEED shop.orders KEY protobuf DEFINITION 'syntax = "proto3"';
    """)
    rows = client.wcl("SHOW SCHEMAS").results[0].data
    by_scope = {(r["scope_kind"], r["name"]): r["schema"] for r in rows}
    assert by_scope[("domain", "shop")]["value"]["subject"] == "order-v1"
    assert by_scope[("feed", "shop.orders")]["key"]["format"] == "protobuf"

    shop = client.get_domain("shop")
    effective = shop.feed_schema("shop.orders")
    assert effective.key.format == "protobuf"
    assert effective.value.format == "json"


def test_schema_wcl_via_typed_commands_matches(client):
    client.wcl("CREATE DOMAIN shop; CREATE FEED shop.orders;")
    client.set_feed_schema("shop.orders",
                           FeedSchema(value=SchemaDecl("custom.msgpack")))
    described = client.commands(
        [{"command": "describe", "kind": "feed", "name": "shop.orders"}]
    ).results[0].data
    assert described["schema"]["value"]["format"] == "custom.msgpack"
    assert described["effective_schema"]["value"]["format"] == "custom.msgpack"
