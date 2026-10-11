"""Error classification: wire (status, code) -> typed exceptions."""

import pytest

from whitewater import (
    AmbiguousResult,
    Conflict,
    LeaseConflict,
    Refused,
    StaleSession,
    Unavailable,
)
from whitewater.errors import map_error


def test_wire_codes_map_to_typed_exceptions():
    error = map_error(409, {"error": "epoch superseded", "code": "stale_epoch"})
    assert isinstance(error, StaleSession)
    assert error.code == "stale_epoch"

    for code in ("lease.busy", "lease.stale_lease", "lease.capacity"):
        error = map_error(409, {"error": "x", "code": code})
        assert isinstance(error, LeaseConflict), code
        assert error.code == code


def test_status_class_is_the_fallback_without_a_code():
    assert isinstance(map_error(409, {"error": "fencing"}), Conflict)
    assert isinstance(
        map_error(503, {"error": "frozen for split cutover; retry safely"}),
        Unavailable,
    )
    assert isinstance(map_error(500, {"error": "internal"}), AmbiguousResult)
    assert isinstance(map_error(400, {"error": "bad name"}), Refused)


def test_stale_sequence_stays_a_conflict_for_raw_appends():
    error = map_error(409, {"error": "seq hole", "code": "writer.stale_sequence"})
    assert isinstance(error, Conflict)
    assert error.code == "writer.stale_sequence"


def test_malformed_body_falls_back_to_status_text():
    error = map_error(502, None)
    assert isinstance(error, AmbiguousResult)
    assert "502" in str(error)


def test_lease_refusal_shape_from_member_claim():
    with pytest.raises(LeaseConflict):
        raise map_error(409, {"error": "work claimed", "code": "lease.busy"})
