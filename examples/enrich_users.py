"""Permanent enrichment consumer — the Whitewater equivalent of a Kafka
consumer loop, with the plumbing removed::

    for msg in feed.consumer("enricher"):
        process(msg)

    # or: feed.consumer("enricher").on_record(process).run()

``feed.consumer(name)`` binds the durable named Reader (Kafka's
consumer-group equivalent): fetch -> deliver -> ack runs on a supervised
loop inside the SDK — transient failures retry with backoff, a fenced
session re-opens, and progress resumes from the server-side cursor
across restarts. No offset management, ever.

Prerequisites (against the dev cluster)::

    client.create_domain("users")
    client.create_feed("users.update.request")
    client.create_feed("users.update.enriched.request")
    client.create_feed("users.update.dlq")
    # users.profiles declared with named key "user_id"; users.stats manual.

Replay honesty: between "enriched append committed" and "input acked" a
crash replays the input — so every effect carries a request_id derived
from the input message_id, which the server dedups. Replay becomes a
no-op instead of a duplicate. Poison pills are the app's job: this
example routes them to the DLQ.
"""

import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from whitewater import WhitewaterClient


@dataclass
class UpdateRequest:
    request_id: str
    user_email: Optional[str]
    user_id: Optional[str]
    changes: dict = field(default_factory=dict)


@dataclass
class EnrichedUpdate:
    request_id: str
    user_id: str
    email: str
    display_name: str
    region: str
    changes: dict = field(default_factory=dict)


@dataclass
class UserProfile:
    user_id: str
    email: str
    display_name: str
    region: str


@dataclass
class DeadLetter:
    reason: str
    source_feed: str
    source_message_id: str
    source_cursor: str
    original: dict


client = WhitewaterClient(
    api_key=os.environ.get("WHITEWATER_API_KEY",
                           "whitewater-local-development-admin-key"),
    endpoints=[
        "http://localhost:7371",
        "http://localhost:7372",
        "http://localhost:7373",
    ],
)

# ---- surfaces, codec-bound once -------------------------------------

in_feed = client.feed("users.update.request", value_type=UpdateRequest)
out_feed = client.feed("users.update.enriched.request",
                       value_type=EnrichedUpdate)
dlq_feed = client.feed("users.update.dlq", value_type=DeadLetter)

# users store: primary key email, declared named key "user_id".
users = client.state_store("users.profiles", value_type=UserProfile)
# counters store: bucket key -> running total.
counters = client.state_store("users.stats", value_type=int)

out_writer = out_feed.writer("enricher")
dlq_writer = dlq_feed.writer("enricher")


def deterministic_uuid(seed: uuid.UUID) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"whitewater:{seed}")


def process(msg) -> None:
    req = msg.value

    # 1. primary-key lookup by email — value only, no row unwrapping
    profile: Optional[UserProfile] = (
        users.find(req.user_email) if req.user_email else None
    )

    # 2. named-key fallback — the declared "user_id" index on the store
    if profile is None and req.user_id:
        profile = users.find_by("user_id", req.user_id)

    # Idempotency key for every effect this input produces: a replay
    # after a crash re-sends the same request_id, which the server
    # dedups to the original append — no double-emit, ever.
    effect_id = deterministic_uuid(msg.message_id)

    if profile is None:
        dlq_writer.append(
            req.user_email,
            DeadLetter(
                reason="user not found",
                source_feed="users.update.request",
                source_message_id=str(msg.message_id),
                source_cursor=str(msg.cursor),
                original=req.__dict__,
            ),
            request_id=effect_id,
            metadata={"reason": b"enrichment-miss"},
        )
        return

    out_writer.append(
        profile.user_id,
        EnrichedUpdate(
            request_id=req.request_id,
            user_id=profile.user_id,
            email=profile.email,
            display_name=profile.display_name,
            region=profile.region,
            changes=req.changes,
        ),
        request_id=effect_id,
    )

    # 3. counters — bucket keys roll up day/week/month. RMW is safe here
    #    because this service is the only writer to these keys.
    today = datetime.now(timezone.utc).date()
    iso = today.isocalendar()
    for bucket in (
        f"updates.day.{today}",
        f"updates.week.{iso.year}-W{iso.week:02d}",
        f"updates.month.{today.year}-{today.month:02d}",
    ):
        counters.put(bucket, counters.get_or_default(bucket, 0) + 1)


if __name__ == "__main__":
    # The permanent loop: durable "enricher" Reader, supervised retries,
    # per-record delivery, ack after the handler returns. Ctrl+C /
    # close() ends it cleanly — the in-flight page finishes and acks.
    try:
        for msg in in_feed.consumer("enricher"):
            process(msg)
    except KeyboardInterrupt:
        pass
