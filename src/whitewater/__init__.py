"""Whitewater SDK for Python.

Implements the versioned ``whitewater-protocol`` wire contract defined
in the main repository under ``sdk/spec/``.
"""

from .client import (
    FeedClient,
    ReaderSessionClient,
    Subscription,
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
    ReaderStart,
    Record,
    RecordStore,
    ServerFeedback,
    StatementResult,
    TemporaryFetchOptions,
    TemporaryFetchPage,
    WorkLease,
)

__all__ = [
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
    "ReaderStart",
    "Record",
    "RecordStore",
    "Refused",
    "ServerFeedback",
    "StaleSession",
    "StatementResult",
    "Subscription",
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
