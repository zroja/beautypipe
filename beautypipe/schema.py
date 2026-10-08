"""Decoding and validation of event payloads, with schema versions.

Two decoders:

* `decode_v1_strict`: what a consumer written against schema v1 does. Anything that is not exactly
  a v1 event is rejected.
* `decode`: understands v1 and v2 and validates values. Unknown versions are rejected.

Both raise `BadEvent` with a short machine-readable reason, which is what ends up in the
dead-letter topic.
"""

import json

from .events import Event

EVENT_TYPES = {"view", "search", "review"}
V1_FIELDS = {"event_id", "event_type", "product_id", "shade_id", "user_id", "rating", "produced_at_ms"}
V2_FIELDS = {
    "schema_version", "event_id", "event_type", "product_id", "shade_id",
    "customer_id", "rating", "occurred_at_ms", "channel",
}


class BadEvent(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _int(doc: dict, field: str) -> int:
    value = doc.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BadEvent(f"wrong_type:{field}")
    return value


def _validated(doc: dict, user_field: str, ts_field: str, channel: str | None) -> Event:
    for field in ("event_id", "event_type", "product_id", "shade_id", user_field, ts_field):
        if doc.get(field) is None:
            raise BadEvent(f"missing_field:{field}")
    if not isinstance(doc["event_id"], str) or not doc["event_id"]:
        raise BadEvent("wrong_type:event_id")
    if doc["event_type"] not in EVENT_TYPES:
        raise BadEvent("unknown_event_type")
    product_id, shade_id, user_id, ts = (_int(doc, f) for f in ("product_id", "shade_id", user_field, ts_field))
    if product_id <= 0:
        raise BadEvent("out_of_range:product_id")
    rating = doc.get("rating")
    if doc["event_type"] == "review":
        if isinstance(rating, bool) or not isinstance(rating, int):
            raise BadEvent("wrong_type:rating")
        if not 1 <= rating <= 5:
            raise BadEvent("out_of_range:rating")
    else:
        rating = None
    return Event(doc["event_id"], doc["event_type"], product_id, shade_id, user_id, rating, ts, channel)


def _load(raw: bytes | None) -> dict:
    if raw is None:
        raise BadEvent("empty_payload")
    try:
        doc = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise BadEvent("invalid_json") from None
    if not isinstance(doc, dict):
        raise BadEvent("not_an_object")
    return doc


def decode_v1_strict(raw: bytes | None) -> Event:
    doc = _load(raw)
    if set(doc) != V1_FIELDS:
        raise BadEvent("schema_mismatch")
    return _validated(doc, "user_id", "produced_at_ms", None)


def decode(raw: bytes | None) -> Event:
    doc = _load(raw)
    version = doc.get("schema_version", 1)
    if version == 1:
        return _validated(doc, "user_id", "produced_at_ms", None)
    if version == 2:
        channel = doc.get("channel")
        if channel is not None and not isinstance(channel, str):
            raise BadEvent("wrong_type:channel")
        return _validated(doc, "customer_id", "occurred_at_ms", channel)
    raise BadEvent("unsupported_version")


DECODERS = {"strict-v1": decode_v1_strict, "versioned": decode}
