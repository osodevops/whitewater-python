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
