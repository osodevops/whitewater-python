"""Whitewater error hierarchy mapped from wire (status, code) pairs.

Wire ``code`` values drive classification; the HTTP status is the
fallback. ``Unavailable`` covers transient refusals the caller may retry
after ``feedback.retry_after_ms`` guidance; ``AmbiguousResult`` means the
operation may have committed — idempotent retry via the same
``request_id`` is the safe path, never blind re-issue.
"""

from __future__ import annotations

from typing import Any, Optional


class WhitewaterError(Exception):
    """Base for every Riverbed refusal."""

    def __init__(self, status: int, code: Optional[str], message: str):
        super().__init__(message)
        self.status = status
        self.code = code


class Refused(WhitewaterError):
    """Non-retryable refusal (4xx without a more specific code)."""


class Conflict(WhitewaterError):
    """409 refusal — fencing, epoch, or sequencing conflict."""


class StaleSession(Conflict):
    """``stale_epoch`` — the Writer/Reader session was superseded."""


class LeaseConflict(Conflict):
    """``lease.*`` — subscription work lease contested, stale, or full."""


class Unavailable(WhitewaterError):
    """Transient: node unreachable, 503, or frozen-for-cutover."""


class AmbiguousResult(WhitewaterError):
    """The operation may or may not have committed (5xx, timeout)."""


_LEASE_CODES = {"lease.busy", "lease.stale_lease", "lease.capacity"}


def map_error(status: int, body: Any) -> WhitewaterError:
    message = "http %d" % status
    code: Optional[str] = None
    if isinstance(body, dict):
        message = str(body.get("error") or message)
        raw_code = body.get("code")
        if isinstance(raw_code, str):
            code = raw_code
    if code == "stale_epoch":
        return StaleSession(status, code, message)
    if code in _LEASE_CODES:
        return LeaseConflict(status, code, message)
    if status == 409:
        return Conflict(status, code, message)
    if status == 503 or status == 0:
        return Unavailable(status, code, message)
    if status >= 500:
        return AmbiguousResult(status, code, message)
    return Refused(status, code, message)
