import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

from beautypipe.events import EventGenerator
from beautypipe.parquet_sink import ParquetSink, compact, connect, read_events


def _events(n, seed=3):
    gen = EventGenerator(50, 6, seed=seed)
    return [gen.next(channel="web" if i % 2 else None) for i in range(n)]


def test_write_is_visible_and_redelivery_is_deduplicated_on_read(tmp_path):
    events = _events(300)
    sink = ParquetSink(tmp_path)
    sink.write(events[:200])
    sink.write(events[100:300])  # overlapping redelivery
    conn = connect()
    raw = conn.execute(f"SELECT count(*) FROM {read_events(tmp_path, dedupe=False)}").fetchone()[0]
    deduped = conn.execute(f"SELECT count(*) FROM {read_events(tmp_path)}").fetchone()[0]
    assert raw == 400  # at-least-once: the overlap is stored twice
    assert deduped == 300


def test_compaction_removes_duplicates_and_sorts(tmp_path):
    events = _events(500)
    sink = ParquetSink(tmp_path)
    for i in range(0, 500, 100):
        sink.write(events[i : i + 100])
    sink.write(events[50:250])
    result = compact(tmp_path)
    assert result["rows_before"] == 700 and result["distinct_before"] == 500 and result["rows_after"] == 500
    assert result["files_before"] > result["files_after"] == 1
    rows = connect().execute(f"SELECT product_id FROM {read_events(tmp_path, compacted=True, dedupe=False)}").fetchall()
    products = [r[0] for r in rows]
    assert products == sorted(products)


def test_values_round_trip(tmp_path):
    events = _events(20)
    ParquetSink(tmp_path).write(events)
    rows = connect().execute(
        f"SELECT event_id, rating, channel, epoch_ms(event_ts) FROM {read_events(tmp_path)} ORDER BY event_id"
    ).fetchall()
    expected = sorted((e.event_id, e.rating, e.channel, e.produced_at_ms) for e in events)
    assert sorted(rows, key=lambda r: r[0]) == expected
