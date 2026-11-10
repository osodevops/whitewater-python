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
    Scoped,
    WhitewaterClient,
    FeedSchema,
    SchemaDecl,
)


@pytest.fixture
def client():
    with FakeRiverbed() as fake:
        yield fake.client()


def seeded(client: WhitewaterClient) -> Domain:
    shop = client.create_domain("shop")
    shop.create_feed("orders")
    shop.create_feed("returns")
    shop.create_feed("orders.daily")
    client.domain("other").create_feed("events")
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
        domain.create_feed("payments")
        domain.refresh()
        assert "shop.payments" in domain.feed_names()

    def test_scoped_resource_listings(self, client):
        domain = seeded(client)
        client._create_writer("orders.ingest", "shop.orders")
        client._create_reader("fulfillment", "shop.orders")
        client._create_subscription("shop.subs.orders", "shop.orders")
        client._create_writer("foreign.writer", "other.events")
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
        client.domain("shop").create_feed("orders")
        writer = (
            client.domain("shop").create_writer("orders")
            .writer_id("ingest")
            .open()
        )
        writer.append("order-1", '{"total": 9}')

        reader = (
            client.domain("shop").create_reader("orders")
            .reader_id("fulfillment")
            .reader_start_point(ReaderStart.BEGINNING)
            .open()
        )
        page = reader.fetch()
        assert page.records
        assert page.records[0].key == "order-1"
        reader.close()

    def test_string_feed_resolves_relative(self, client):
        client.domain("shop").create_feed("orders")
        builder = client.domain("shop").create_reader("orders")
        assert builder._feed.name == "shop.orders"

    def test_reader_id_is_required(self, client):
        client.domain("shop").create_feed("orders")
        with pytest.raises(ValueError, match="reader_id"):
            client.domain("shop").create_reader("orders").open()

    def test_cross_domain_feed_refused_locally(self, client):
        client.create_domain("shop")
        client.create_domain("other")
        client.domain("other").create_feed("events")
        foreign = client.domain("other").feed("events")
        with pytest.raises(ValueError, match="outside Domain"):
            client.domain("shop").create_reader(foreign)

    def test_string_feed_names_are_relative(self, client):
        # Strings compose inside the Domain ("orders" → "shop.orders");
        # the cross-domain refusal applies to Feed objects bound outside.
        client.create_domain("shop")
        builder = client.domain("shop").create_reader("other.events")
        assert builder._feed.name == "shop.other.events"

    def test_start_point_applies_on_first_declare(self, client):
        shop = client.domain("shop")
        shop.create_feed("orders")
        shop.create_reader("orders").reader_id("r1").declare()
        described = shop.describe_reader("r1")
        assert described.results[0].data["start"]["kind"] == "beginning"

    def test_declare_fails_when_already_declared(self, client):
        shop = client.domain("shop")
        shop.create_feed("orders")
        shop.create_reader("orders").reader_id("r1").declare()
        with pytest.raises(Refused):
            shop.create_reader("orders").reader_id("r1").declare()


class TestWriterBuilder:
    def test_open_declares_writer_and_appends(self, client):
        client.domain("shop").create_feed("orders")
        writer = (
            client.domain("shop").create_writer("orders")
            .writer_id("orders.ingest")
            .open()
        )
        writer.append("k1", "payload")

    def test_writer_id_is_required(self, client):
        client.domain("shop").create_feed("orders")
        with pytest.raises(ValueError, match="writer_id"):
            client.domain("shop").create_writer("orders").open()


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
    writer.append("order-1", "1")

    reader = (shop.create_reader("orders")
              .reader_id("fulfillment")
              .reader_start_point(ReaderStart.BEGINNING)
              .open())
    assert reader.fetch().records[0].key == "order-1"
    reader.close()


def test_reader_for_each_acks_handled_pages(client):
    shop = client.domain("shop")
    shop.create_feed("orders")
    writer = shop.create_writer("orders").writer_id("w").open()
    writer.append("a", "1")
    writer.append("b", "2")

    reader = (shop.create_reader("orders")
              .reader_id("pump")
              .reader_start_point(ReaderStart.BEGINNING)
              .max_page_size(1)
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


def test_reader_listen_pumps_on_an_sdk_owned_thread(client):
    """reader.listen owns the pump thread — stop() closes the session;
    a raised handler surfaces via pump.failure and the page replays."""
    import time
    shop = client.domain("shop")
    shop.create_feed("orders")
    shop.create_writer("orders").writer_id("w").open().append("a", "1")

    reader = (shop.create_reader("orders")
              .reader_id("listener")
              .reader_start_point(ReaderStart.BEGINNING)
              .open())
    seen = []
    pump = reader.listen(lambda m: seen.append(m.key))
    for _ in range(200):
        if seen:
            break
        time.sleep(0.01)
    assert seen == ["a"]
    assert pump.failure is None
    pump.stop()                          # thread + session released
    assert not pump.running

    # A raised handler stops the pump and the page replays.
    reader2 = (shop.create_reader("orders")
               .reader_id("failing")
               .reader_start_point(ReaderStart.BEGINNING)
               .open())
    boom = RuntimeError("boom")

    def bad(_m):
        raise boom

    pump2 = reader2.listen(bad)
    for _ in range(200):
        if pump2.failure is not None:
            break
        time.sleep(0.01)
    assert pump2.failure is boom

    again = shop.create_reader("orders").reader_id("failing").open()
    assert again.fetch().records[0].key == "a"
    again.close()


def test_reader_iterates_records_per_page(client):
    shop = client.domain("shop")
    shop.create_feed("orders")
    shop.create_writer("orders").writer_id("w").open().append("a", "1")
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
    client.domain("shop").set_feed_schema(
        "orders", FeedSchema(key=SchemaDecl("text")))

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
    client.domain("shop").set_feed_schema("orders", FeedSchema(value=SchemaDecl("avro")))

    shop = client.get_domain("shop")
    assert shop.feed_schema("shop.orders").value.format == "avro"

    shop.clear_feed_schema("orders")
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
    client.domain("shop").set_feed_schema(
        "orders", FeedSchema(value=SchemaDecl("custom.msgpack")))
    described = client.commands(
        [{"command": "describe", "kind": "feed", "name": "shop.orders"}]
    ).results[0].data
    assert described["schema"]["value"]["format"] == "custom.msgpack"
    assert described["effective_schema"]["value"]["format"] == "custom.msgpack"


class TestDomainSurface:
    """Domain-first surface: every resource name resolves inside the
    Domain, and cross-Domain pipe endpoints come only from explicit
    handles or ``Scoped.absolute``."""

    def test_create_domain_returns_handle(self, client):
        shop = client.create_domain("shop")
        assert isinstance(shop, Domain)
        assert shop.name == "shop"
        shop.create_feed("orders")
        assert "shop.orders" in client.get_domain("shop").feed_names()

    def test_raw_reader_and_writer_decls_resolve_in_domain(self, client):
        shop = client.create_domain("shop")
        shop.create_feed("orders")
        # create_reader(name, feed) — reader name first, feed scoped.
        shop.create_reader("audit", "orders")
        shop.create_writer("ingest", "orders")
        shop.refresh()
        assert "shop.audit" in shop.readers()
        assert "shop.ingest" in shop.writers()
        # The session clients resolve the same way — no flat name leaks.
        session = shop.reader("audit").open(capacity=4)
        shop.reader("audit").close(session)

    def test_create_subscription_scopes_feed(self, client):
        shop = client.create_domain("shop")
        shop.create_feed("orders")
        shop.create_subscription("audit", "orders")
        assert "shop.audit" in shop.subscriptions()

    def test_scoped_resolves_relative_and_absolute(self, client):
        shop = client.domain("shop")
        assert shop.scoped("orders").resolve(shop) == "shop.orders"
        assert Scoped.relative("orders").resolve(shop) == "shop.orders"
        assert Scoped.absolute("other.events").resolve(shop) == "other.events"
        # A handle from another Domain resolves to its qualified name.
        other_feed = client.domain("other").feed("events")
        assert Scoped.absolute(other_feed.name).resolve(shop) == "other.events"

    def test_cross_domain_pipe_declares_qualified_endpoints(self, client):
        purchasing = client.create_domain("purchasing")
        purchasing.create_feed("orders")
        warehouse = client.create_domain("warehouse")
        warehouse.create_feed("inbound")

        # The warehouse owns the pipe; the source subscription reaches
        # back into purchasing via a Feed handle (absolute name).
        warehouse.create_subscription("fulfil.sub", purchasing.feed("orders"))
        warehouse.define_forward_pipe(
            "fulfil", "fulfil.sub", warehouse.feed("inbound"))

        warehouse.refresh()
        assert "warehouse.fulfil" in warehouse.pipes()
        assert "warehouse.fulfil.sub" in warehouse.subscriptions()
        # The declared pipe is usable end-to-end on the fake.
        assert warehouse.pipe("fulfil").describe().name == "warehouse.fulfil"

    def test_scoped_absolute_declares_cross_domain(self, client):
        purchasing = client.create_domain("purchasing")
        purchasing.create_feed("orders")
        warehouse = client.create_domain("warehouse")
        warehouse.create_feed("inbound")
        warehouse.create_subscription(
            "audit.sub", Scoped.absolute("purchasing.orders"))
        warehouse.refresh()
        assert "warehouse.audit.sub" in warehouse.subscriptions()

    def test_lifecycle_ops_resolve_in_domain(self, client):
        shop = client.create_domain("shop")
        shop.create_feed("orders")
        shop.create_reader("audit", "orders")
        described = shop.describe_feed("orders")
        assert described.results[0].data["name"] == "shop.orders"
        shop.rename_reader("audit", "audit2")
        shop.refresh()
        assert "shop.audit2" in shop.readers()
        shop.drop_reader("audit2")
        shop.refresh()
        assert "shop.audit2" not in shop.readers()

    def test_flat_client_resource_methods_are_private(self, client):
        """The blocked flat route: no public client.feed/create_feed."""
        for attr in ("feed", "create_feed", "create_writer",
                     "create_reader", "create_subscription", "reader",
                     "writer", "state_store", "pipe", "subscribe",
                     "subscription_member", "drop_feed", "list_feeds"):
            assert not callable(getattr(client, attr, None)), attr
