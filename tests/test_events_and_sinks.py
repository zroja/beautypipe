import pytest

from beautypipe import db
from beautypipe.events import Event, EventGenerator
from beautypipe.sinks import PostgresBatchedSink, PostgresNaiveSink


def test_event_ids_are_deterministic():
    a = [EventGenerator(100, 6, seed=3).next(now_ms=1).event_id for _ in range(1)]
    b = [EventGenerator(100, 6, seed=3).next(now_ms=1).event_id for _ in range(1)]
    assert a == b
    gen = EventGenerator(100, 6, seed=3)
    ids = {gen.next().event_id for _ in range(1000)}
    assert len(ids) == 1000


def test_event_json_roundtrip():
    e = EventGenerator(100, 6).next()
    assert Event.from_json(e.to_json()) == e


@pytest.fixture
def clean_db():
    db.init_schema()
    db.reset_streaming_tables()


def _events(n):
    gen = EventGenerator(50, 6, seed=11)
    return [gen.next() for _ in range(n)]


@pytest.mark.integration
@pytest.mark.parametrize("sink_cls", [PostgresNaiveSink, PostgresBatchedSink])
def test_sink_is_idempotent_under_redelivery(clean_db, sink_cls):
    events = _events(300)
    sink = sink_cls()
    sink.write(events)
    sink.write(events[100:250])  # redelivered overlap
    sink.write(events)  # full replay
    sink.close()
    result = db.validate()
    assert result["events"] == 300
    assert result["stats_total"] == 300
    assert result["mismatched_products"] == 0
