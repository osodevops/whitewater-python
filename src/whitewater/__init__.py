"""Whitewater SDK for Python.

Implements the versioned ``whitewater-protocol`` wire contract defined
in the main repository under ``sdk/spec/``.
"""

from .client import (
    AdminClient,
    FeedClient,
    ReaderSessionClient,
    SubscriptionMemberClient,
    WhitewaterClient,
    WriterSessionClient,
)
from .errors import (
    AmbiguousResult,
    Conflict,
    LeaseConflict,
    Refused,
    StaleSession,
    Unavailable,
    WhitewaterError,
)
from .models import (
    AppendBatchResult,
    AppendResult,
    ControlExecution,
    Cursor,
    FeedAppend,
    Joined,
    MemberPage,
    ReaderPage,
    ReaderSession,
    Record,
    ServerFeedback,
    StatementResult,
    TemporaryFetchOptions,
    TemporaryFetchPage,
    WorkLease,
)

__all__ = [
    "AdminClient",
    "AmbiguousResult",
    "AppendBatchResult",
    "AppendResult",
    "Conflict",
    "ControlExecution",
    "Cursor",
    "FeedAppend",
    "FeedClient",
    "Joined",
    "LeaseConflict",
    "MemberPage",
    "ReaderPage",
    "ReaderSession",
    "ReaderSessionClient",
    "Record",
    "Refused",
    "ServerFeedback",
    "StaleSession",
    "StatementResult",
    "SubscriptionMemberClient",
    "TemporaryFetchOptions",
    "TemporaryFetchPage",
    "Unavailable",
    "WhitewaterClient",
    "WhitewaterError",
    "WorkLease",
    "WriterSessionClient",
]

__version__ = "0.1.0.dev0"
