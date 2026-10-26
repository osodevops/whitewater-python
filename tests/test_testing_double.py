"""Unit tests for the public ``FakeRiverbed`` test double.

Every interaction goes through ``WhitewaterClient`` over real loopback
HTTP — the same transport, wire serialization, idempotency keys, and
error mapping application code sees against a live Riverbed.
"""

import threading
import uuid

import pytest

import whitewater
from whitewater import (
    Conflict,
    FakeRiverbed,
    FeedEvent,
    ReaderStart,
    Refused,
    StaleSession,
    StateStore,
    TemporaryFetchOptions,
)


@pytest.fixture
def riverbed():
    with FakeRiverbed() as fake:
        yield fake


@pytest.fixture
def client(riverbed):
    return riverbed.client()


def test_exported_and_serves_health(riverbed, client):
    assert whitewater.FakeRiverbed is FakeRiverbed
    assert riverbed.endpoints[0].startswith("http://127.0.0.1:")
    assert client.healthy()
    assert ("GET", "/health", {}) in riverbed.requests


def test_append_fetch_ack_then_reopen_stays_drained(riverbed, client):
    orders = client.feed("orders.events")
    writer = orders.writer("ingest")
    writer.append("order-1", "created")
    writer.append("order-2", "paid")

    with orders.reader("audit") as reader:
        page = reader.fetch()
        assert [m.key for m in page.records] == ["order-1", "order-2"]
        assert [m.value for m in page.records] == ["created", "paid"]
        reader.ack()

    # Acknowledged progress is durable: a reopened session does not
    # redeliver it.
    with orders.reader("audit") as reader:
        assert reader.fetch().records == []


def test_unacknowledged_close_redelivers(riverbed, client):
    orders = client.feed("orders.events")
    orders.writer("ingest").append("order-1", "created")

    with orders.reader("audit") as reader:
        assert [m.value for m in reader.fetch().records] == ["created"]
        reader.close()  # no ack — the page stays pending

    with orders.reader("audit") as reader:
        page = reader.fetch()
        assert [m.value for m in page.records] == ["created"]
        reader.ack()

    with orders.reader("audit") as reader:
        assert reader.fetch().records == []


def test_fetch_request_id_replays_committed_page(riverbed, client):
    orders = client.feed("orders.events")
    orders.writer("ingest").append("order-1", "created")

    session_client = client.reader("audit")
    client.create_reader("audit", "orders.events")
    session = session_client.open(capacity=8)
    request_id = uuid.uuid4()
    first = session_client.fetch(session, request_id=request_id)
    retry = session_client.fetch(session, request_id=request_id)
    assert [r.cursor for r in retry.records] == [r.cursor for r in first.records]
    session_client.close(session)


def test_append_request_id_deduplicates(riverbed, client):
    orders = client.feed("orders.events")
    writer = orders.writer("ingest")

    request_id = uuid.uuid4()
    first = writer.append("order-1", "created", request_id=request_id)
    retry = writer.append("order-1", "created", request_id=request_id)

    assert not first.deduplicated
    assert retry.deduplicated
    assert retry.message_id == first.message_id
    assert retry.cursor == first.cursor
    assert len(orders.records()) == 1

    # The same request identity under different bytes is request poison,
    # not a duplicate.
    with pytest.raises(Conflict):
        writer.append("order-1", "tampered", request_id=request_id)
    assert len(orders.records()) == 1


def test_append_batch(riverbed, client):
    orders = client.feed("orders.events")
    writer = orders.writer("ingest")
    result = writer.append_all(
        [FeedEvent("order-1", "a"), FeedEvent("order-2", "b")]
    )
    assert [r.deduplicated for r in result.results] == [False, False]
    assert [m.value for m in orders.records()] == ["a", "b"]


def test_adhoc_append_sequence_fencing(riverbed, client):
    client.create_feed("orders.events")
    orders = client.feed("orders.events")
    session = uuid.uuid4()
    orders.append(session, 1, 1, "k", "a")
    orders.append(session, 1, 2, "k", "b")
    with pytest.raises(Conflict):  # gap: expected 3
        orders.append(session, 1, 9, "k", "c")
    with pytest.raises(Conflict):  # stale: 2 already committed
        orders.append(session, 1, 2, "k", "d")
    assert [m.value for m in orders.records()] == ["a", "b"]


def test_stale_writer_epoch_maps_to_stale_session(riverbed, client):
    orders = client.feed("orders.events")
    stale = orders.writer("ingest")
    live = orders.writer("ingest")  # a new session supersedes the old epoch
    assert live.session_epoch == stale.session_epoch + 1

    with pytest.raises(StaleSession):
        stale.append("order-9", "late")
    live.append("order-9", "late")  # the current epoch still commits


def test_revoked_writer_session_maps_to_stale_session(riverbed, client):
    orders = client.feed("orders.events")
    writer = orders.writer("ingest")
    client.wcl(f"REVOKE WRITER SESSION ingest EPOCH {writer.session_epoch}")
    with pytest.raises(StaleSession):
        writer.append("k", "v")


def test_temporary_fetch_and_records(riverbed, client):
    orders = client.feed("orders.events")
    writer = orders.writer("ingest")
    for index in range(3):
        writer.append(f"order-{index}", f"event-{index}".encode())

    page = orders.fetch(TemporaryFetchOptions(limit=2))
    assert [m.value for m in page.records] == ["event-0", "event-1"]

    rest = orders.fetch(TemporaryFetchOptions(after=page.next_cursor))
    assert [m.value for m in rest.records] == ["event-2"]

    tail = orders.fetch(TemporaryFetchOptions(new_only=True))
    assert tail.records == []
    assert tail.next_cursor is not None

    history = orders.records(limit=2)
    assert [m.key for m in history] == ["order-0", "order-1"]
    remainder = orders.records(after=history[-1].cursor)
    assert [m.key for m in remainder] == ["order-2"]


def test_state_store_manual_lifecycle(riverbed, client):
    store = client.state_stores.define(
        StateStore.named("ops.profiles").add_key("department")
    )
    first = store.put("emp-1", "alice", named={"department": "eng"})
    assert not first.deduplicated
    store.put("emp-2", "bob", named={"department": "ops"})

    row = store.get("emp-1")
    assert row is not None
    assert row.payload == "alice"
    assert row.applied_cursor is not None
    assert row.freshness is not None and not row.freshness.behind
    assert store.get("ghost") is None

    assert [r.key for r in store.get_by("department", "eng").rows] == ["emp-1"]
    assert store.get_by("department", "eng").rows[0].payload == "alice"

    # iterate pages in primary-key order and continue through next_key.
    assert [r.key for r in store.iterate(limit=1)] == ["emp-1", "emp-2"]

    store.delete("emp-1")
    assert store.get("emp-1") is None
    assert store.get_by("department", "eng").rows == []

    store.clear()
    assert list(store.iterate()) == []


def test_state_store_mutation_id_dedupes(riverbed, client):
    store = client.state_stores.define(StateStore.named("ops.mutations"))
    mutation_id = uuid.uuid4()
    first = store.put("k", "v1", mutation_id=mutation_id)
    retry = store.put("k", "v1", mutation_id=mutation_id)
    assert retry.deduplicated
    assert retry.sequence == first.sequence
    assert retry.mutation_id == first.mutation_id
    with pytest.raises(Conflict):
        store.put("k", "different", mutation_id=mutation_id)


def test_state_store_named_key_rules(riverbed, client):
    store = client.state_stores.define(
        StateStore.named("ops.directory").add_key("email", unique=True)
    )
    store.put("user-1", "alice", named={"email": "alice@corp.test"})
    with pytest.raises(Conflict):
        store.put("user-2", "bob", named={"email": "alice@corp.test"})
    with pytest.raises(Refused):
        store.put("user-3", "carol", named={"undeclared": "x"})


def test_wcl_catalog_declarations(riverbed, client):
    execution = client.wcl(
        "CREATE DOMAIN orders;"
        "CREATE FEED orders.events;"
        "CREATE WRITER ingest TO orders.events;"
        "CREATE READER audit FROM orders.events START AT BEGINNING;"
    )
    assert execution.authority == "control_plane"
    assert [r.statement for r in execution.results] == [
        "CREATE DOMAIN",
        "CREATE FEED",
        "CREATE WRITER",
        "CREATE READER",
    ]

    described = client.describe_feed("orders.events")
    assert described.results[0].data["name"] == "orders.events"
    assert described.results[0].data["feed_id"]

    shown = client.list_feeds()
    assert [r["name"] for r in shown.results[0].data] == ["orders.events"]

    with pytest.raises(Refused, match="exists"):
        client.create_feed("orders.events")

    # Control executions dedupe on request_id like the real journal.
    request_id = uuid.uuid4()
    again = client.wcl(
        "CREATE DOMAIN ops", request_id=request_id
    )
    repeat = client.wcl("CREATE DOMAIN other", request_id=request_id)
    assert repeat.request_id == again.request_id
    assert [r.statement for r in repeat.results] == [r.statement for r in again.results]

    client.drop_reader("audit")
    with pytest.raises(Refused):
        client.reader("audit").open()


def test_reader_start_variants(riverbed, client):
    orders = client.feed("orders.events")
    writer = orders.writer("ingest")
    writer.append("k1", "first")
    second = writer.append("k2", "second")
    writer.append("k3", "third")

    client.create_reader(
        "from.cursor", "orders.events", start=ReaderStart.cursor(second.cursor)
    )
    session = client.reader("from.cursor").open(capacity=8)
    page = client.reader("from.cursor").fetch(session)
    assert [r.payload for r in page.records] == [b"third"]
    client.reader("from.cursor").close(session)


def test_unknown_path_and_errors_decode(riverbed, client):
    # Unknown routes still return the JSON envelope the transport expects.
    with pytest.raises(Refused):
        client._transport.post("/v1/no-such-endpoint", {})
    with pytest.raises(Refused):
        client.commands([{"command": "not_a_command"}])


def test_subscribe_delivers_and_acks(riverbed, client):
    orders = client.feed("orders.events")
    orders.writer("ingest").append("order-1", "created")

    received = []
    done = threading.Event()

    def on_records(records):
        received.extend(records)
        done.set()

    errors = []
    with client.subscribe(
        "audit",
        "orders.events",
        ReaderStart.beginning(),
        on_records,
        on_error=errors.append,
    ) as subscription:
        assert done.wait(timeout=10)
        assert [r.payload for r in received] == [b"created"]
        subscription.close()
    assert not errors
    assert not subscription.running


def test_reader_session_close_and_reopen_via_low_level_client(riverbed, client):
    """ReaderSessionClient round-trips the same durable progress the
    ``FeedReader`` facade uses."""
    orders = client.feed("orders.events")
    orders.writer("ingest").append("k", "v")
    client.create_reader("audit", "orders.events")

    reader = client.reader("audit")
    session = reader.open(capacity=4)
    page = reader.fetch(session)
    assert [r.payload for r in page.records] == [b"v"]
    session = reader.ack(session, page.delivered_cursor)
    session = reader.close(session)

    reopened = reader.open(capacity=4)
    assert reopened.session_epoch == session.session_epoch + 1
    assert reader.fetch(reopened).records == []
    reader.close(reopened)
