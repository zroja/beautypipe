import json
import random

import pytest

from beautypipe.events import BAD_KINDS, EventGenerator, corrupt
from beautypipe.producer import produce
from beautypipe.schema import BadEvent, decode, decode_v1_strict


def _event(channel=None):
    return EventGenerator(100, 6, seed=5).next(now_ms=1_700_000_000_000, channel=channel)


def test_v1_is_understood_by_both_decoders():
    e = _event()
    assert decode_v1_strict(e.to_json()) == e
    assert decode(e.to_json()) == e


def test_v2_is_understood_only_by_the_versioned_decoder():
    e = _event(channel="web")
    assert decode(e.to_json_v2()) == e
    with pytest.raises(BadEvent) as exc:
        decode_v1_strict(e.to_json_v2())
    assert exc.value.reason == "schema_mismatch"


def test_v2_renames_map_to_the_same_event():
    e = _event(channel="store")
    doc = json.loads(e.to_json_v2())
    assert doc["customer_id"] == e.user_id and doc["occurred_at_ms"] == e.produced_at_ms


@pytest.mark.parametrize("kind", BAD_KINDS)
def test_every_corruption_is_rejected_by_the_versioned_decoder(kind):
    payload = corrupt(_event().to_json(), kind)
    with pytest.raises(BadEvent):
        decode(payload)


def test_corruptions_have_distinct_reasons():
    reasons = {}
    for kind in BAD_KINDS:
        # review events are needed for the rating corruptions
        gen = EventGenerator(100, 6, seed=1)
        event = next(e for e in (gen.next() for _ in range(500)) if e.event_type == "review")
        with pytest.raises(BadEvent) as exc:
            decode(corrupt(event.to_json(), kind))
        reasons[kind] = exc.value.reason
    assert reasons["invalid_json"] == "invalid_json"
    assert reasons["unknown_event_type"] == "unknown_event_type"
    assert reasons["unsupported_version"] == "unsupported_version"
    assert len(set(reasons.values())) >= 5


@pytest.mark.parametrize("raw", [None, b"", b"[1,2]", b"\xff\xfe", b'{"event_id": 1}'])
def test_garbage_is_rejected_not_raised_as_other_exceptions(raw):
    with pytest.raises(BadEvent):
        decode(raw)


def test_rating_bounds():
    gen = EventGenerator(100, 6, seed=2)
    review = next(e for e in (gen.next() for _ in range(500)) if e.event_type == "review")
    doc = json.loads(review.to_json())
    doc["rating"] = 6
    with pytest.raises(BadEvent) as exc:
        decode(json.dumps(doc).encode())
    assert exc.value.reason == "out_of_range:rating"


def test_producer_stats_are_consistent(monkeypatch):
    sent = []

    class FakeProducer:
        def __init__(self, *_):
            pass

        def produce(self, topic, key=None, value=None):
            sent.append(value)

        def poll(self, _):
            pass

        def flush(self, _):
            pass

    monkeypatch.setattr("beautypipe.producer.Producer", FakeProducer)
    stats: dict = {}
    n = produce("t", rate=4000, duration=1.0, bad_rate=0.05, v2_after=0.4, stats=stats)
    assert n == len(sent) == stats["sent"]
    assert stats["valid"] + stats["bad"] == stats["sent"]
    assert stats["valid"] == stats["v1_valid"] + stats["v2"]
    assert stats["bad"] > 0 and stats["v2"] > 0
    ok = sum(1 for raw in sent if _decodes(raw))
    assert ok == stats["valid"]
    strict_ok = sum(1 for raw in sent if _decodes(raw, strict=True))
    assert strict_ok == stats["v1_valid"]


def _decodes(raw, strict=False):
    try:
        (decode_v1_strict if strict else decode)(raw)
        return True
    except BadEvent:
        return False
