"""Live StateStore smoke against a running Riverbed — create a Domain,
declare a named-key StateStore, then put/get/get_by/iterate/clear/drop
through the facade against the node-side store.

Runs only when WHITEWATER_ENDPOINTS is set (comma-separated), e.g.
``http://localhost:7371,http://localhost:7372,http://localhost:7373``;
WHITEWATER_API_KEY defaults to the documented development key.
"""

import os
import time
import uuid

import pytest

from whitewater import StateStore, WhitewaterClient

def _endpoints() -> list:
    """Accept ``http://a,b`` or ``a;b`` style env lists; bare host:port
    pairs get the development http scheme."""
    raw = os.environ.get("WHITEWATER_ENDPOINTS") or ""
    parts = [p.strip() for chunk in raw.split(";") for p in chunk.split(",")]
    return [
        p if "://" in p else f"http://{p}" for p in parts if p
    ]


pytestmark = pytest.mark.skipif(
    not _endpoints(), reason="WHITEWATER_ENDPOINTS unset — no live Riverbed"
)


def test_named_key_store_lifecycle_live():
    client = WhitewaterClient(
        api_key=os.environ.get(
            "WHITEWATER_API_KEY", "whitewater-local-development-admin-key"
        ),
        endpoints=_endpoints(),
    )

    domain = "smoke.py%x" % (time.time_ns() & 0x7FFFFFFF)
    name = f"{domain}.employees"
    assert client.healthy()
    client.create_domain(domain)

    store = client.state_stores.define(
        StateStore.named(name)
        .add_key("passport", unique=True)
        .add_key("department")
    )

    store.put(
        b"user-1",
        b"profile-1",
        named={"passport": "P111", "department": "eng"},
    )
    store.put(
        b"user-2",
        b"profile-2",
        named={"passport": "P222", "department": "eng"},
        mutation_id=uuid.uuid4(),
    )

    found = store.get(b"user-1", strict=True)
    assert found is not None and found.payload == b"profile-1"
    assert found.freshness.behind is False

    team = store.get_by("department", "eng", strict=True)
    assert sorted(row.key for row in team.rows) == [b"user-1", b"user-2"]

    solo = store.get_by("passport", "P222")
    assert [row.key for row in solo.rows] == [b"user-2"]

    assert sorted(row.key for row in store.iterate()) == [b"user-1", b"user-2"]

    store.clear()
    assert store.get(b"user-1") is None
    assert store.get_by("department", "eng").rows == []

    client.state_stores.drop(name)
