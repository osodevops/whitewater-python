"""Live store smoke against a running Riverbed — create Domain + Feed,
append keyed records with metadata, then ``load_store`` a projected
history read and named-key lookups.

Runs only when WHITEWATER_ENDPOINTS is set (comma-separated), e.g.
``http://localhost:7371,http://localhost:7372,http://localhost:7373``;
WHITEWATER_API_KEY defaults to the documented development key.
"""

import os
import time
import uuid

import pytest

from whitewater import FeedAppend, ReaderStart, RecordStore, WhitewaterClient

ENDPOINTS = os.environ.get("WHITEWATER_ENDPOINTS")

pytestmark = pytest.mark.skipif(
    not ENDPOINTS, reason="WHITEWATER_ENDPOINTS unset — no live Riverbed"
)


def test_create_write_load_and_query_a_store():
    client = WhitewaterClient(
        api_key=os.environ.get(
            "WHITEWATER_API_KEY", "whitewater-local-development-admin-key"
        ),
        endpoints=[e.strip() for e in ENDPOINTS.split(",")],
    )

    domain = "smoke.py%x" % (time.time_ns() & 0x7FFFFFFF)
    feed = f"{domain}.events"
    assert client.healthy()
    client.create_domain(domain)
    client.create_feed(feed)

    session = uuid.uuid4()
    rows = [
        (b"emp-1", b"alice", b"eng"),
        (b"emp-2", b"bob", b"eng"),
        (b"emp-3", b"cara", b"ops"),
    ]
    for i, (key, payload, department) in enumerate(rows):
        client.feeds.append(
            feed,
            FeedAppend(
                request_id=uuid.uuid4(),
                writer_session_id=session,
                writer_epoch=1,
                sequence=i + 1,
                key=key,
                payload=payload,
                metadata={"department": department},
            ),
        )

    store = RecordStore.named("employee-store").add_key("department")
    loaded = client.load_store(store).from_feed(
        feed,
        ReaderStart.beginning(),
        lambda s, record: s.upsert(record)
        .with_key(record.key)
        .with_named_key("department", record.metadata["department"]),
    )

    assert loaded == 3
    assert len(store) == 3
    assert store.get("emp-1").payload == b"alice"
    assert len(store.get_by("department", "eng")) == 2
    assert len(store.get_by("department", "ops")) == 1

    copy = RecordStore.named("employee-copy")
    store.copy(copy)
    assert len(copy.get_by("department", "eng")) == 2
    assert store.delete("emp-1")
    assert len(store.get_by("department", "eng")) == 1

    visited = []
    store.for_each_parallel(
        2,
        lambda entry: visited.append(
            (entry.primary_key, entry.record.payload)
        ),
    )
    assert len(visited) == 2
