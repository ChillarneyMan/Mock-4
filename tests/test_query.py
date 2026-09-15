import io
import os
import tempfile
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date, timezone
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.query import DuckDBQueryClient


def write_parquet(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    table = pa.table({
        "event_type": [r["event_type"] for r in rows],
        "source": [r["source"] for r in rows],
        "timestamp": [r["timestamp"] for r in rows],
        "payload": [r["payload"] for r in rows],
    })
    pq.write_table(table, path)


@pytest.fixture
def parquet_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        # page_view events on 2024-01-15
        write_parquet(
            f"{tmpdir}/event_type=page_view/date=2024-01-15/f1.parquet",
            [
                {"event_type": "page_view", "source": "web", "timestamp": "2024-01-15T10:00:00+00:00", "payload": "{}"},
                {"event_type": "page_view", "source": "web", "timestamp": "2024-01-15T11:00:00+00:00", "payload": "{}"},
                {"event_type": "page_view", "source": "mobile", "timestamp": "2024-01-15T12:00:00+00:00", "payload": "{}"},
            ],
        )
        # click events on 2024-01-15
        write_parquet(
            f"{tmpdir}/event_type=click/date=2024-01-15/f1.parquet",
            [
                {"event_type": "click", "source": "web", "timestamp": "2024-01-15T09:00:00+00:00", "payload": "{}"},
                {"event_type": "click", "source": "web", "timestamp": "2024-01-15T13:00:00+00:00", "payload": "{}"},
            ],
        )
        # page_view events on 2024-01-16
        write_parquet(
            f"{tmpdir}/event_type=page_view/date=2024-01-16/f1.parquet",
            [
                {"event_type": "page_view", "source": "web", "timestamp": "2024-01-16T08:00:00+00:00", "payload": "{}"},
            ],
        )
        yield tmpdir


@pytest.fixture
def client(parquet_dir):
    c = DuckDBQueryClient.__new__(DuckDBQueryClient)
    import duckdb
    c._con = duckdb.connect()
    return c, f"{parquet_dir}/**/*.parquet"


def test_filter_by_event_type(client):
    qc, glob = client
    rows = qc.get_events("page_view", None, None, None, 100, 0, parquet_glob=glob)
    assert all(r["event_type"] == "page_view" for r in rows)
    assert len(rows) == 4  # 3 on jan15 + 1 on jan16


def test_filter_by_source(client):
    qc, glob = client
    rows = qc.get_events(None, "mobile", None, None, 100, 0, parquet_glob=glob)
    assert all(r["source"] == "mobile" for r in rows)
    assert len(rows) == 1


def test_filter_by_time_range(client):
    qc, glob = client
    start = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    end = datetime(2024, 1, 15, 12, 30, tzinfo=timezone.utc)
    rows = qc.get_events(None, None, start, end, 100, 0, parquet_glob=glob)
    timestamps = [r["timestamp"] for r in rows]
    assert len(rows) == 2
    for ts in timestamps:
        assert "T10:00" not in ts  # 10:00 excluded (< start)
        assert "T13:00" not in ts  # 13:00 excluded (> end)


def test_pagination(client):
    qc, glob = client
    all_rows = qc.get_events(None, None, None, None, 100, 0, parquet_glob=glob)
    page1 = qc.get_events(None, None, None, None, 3, 0, parquet_glob=glob)
    page2 = qc.get_events(None, None, None, None, 3, 3, parquet_glob=glob)

    assert len(page1) == 3
    assert len(page2) == 3
    assert page1[0]["timestamp"] == all_rows[0]["timestamp"]
    assert page2[0]["timestamp"] == all_rows[3]["timestamp"]
    # No overlap
    assert set(r["timestamp"] for r in page1).isdisjoint(
        set(r["timestamp"] for r in page2)
    )


def test_stats_grouped_by_event_type_and_date(client):
    qc, glob = client
    stats = qc.get_event_stats(None, None, parquet_glob=glob)
    # Stats are now grouped by (event_type, date) — one row per combination.
    # page_view appears on jan15 (3 events) and jan16 (1 event).
    # click appears only on jan15 (2 events).
    # DuckDB returns the Hive partition 'date' column as datetime.date objects.
    by_key = {(r["event_type"], r["date"]): r["count"] for r in stats}
    assert by_key[("page_view", date(2024, 1, 15))] == 3
    assert by_key[("page_view", date(2024, 1, 16))] == 1
    assert by_key[("click", date(2024, 1, 15))] == 2


def test_stats_time_range_filter(client):
    qc, glob = client
    # Only jan 15
    start = datetime(2024, 1, 15, tzinfo=timezone.utc)
    end = datetime(2024, 1, 15, 23, 59, 59, tzinfo=timezone.utc)
    stats = qc.get_event_stats(start, end, parquet_glob=glob)
    by_key = {(r["event_type"], r["date"]): r["count"] for r in stats}
    assert by_key[("page_view", date(2024, 1, 15))] == 3  # jan16 excluded
    assert by_key[("click", date(2024, 1, 15))] == 2
    assert ("page_view", date(2024, 1, 16)) not in by_key


def test_no_data_returns_empty_list():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Empty dir — no parquet files
        qc = DuckDBQueryClient.__new__(DuckDBQueryClient)
        import duckdb
        qc._con = duckdb.connect()
        glob = f"{tmpdir}/**/*.parquet"
        rows = qc.get_events(None, None, None, None, 50, 0, parquet_glob=glob)
        assert rows == []


@pytest.mark.asyncio
async def test_invalid_limit_returns_400():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/events?limit=-1")
        assert resp.status_code == 400

        resp = await ac.get("/events?limit=501")
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_invalid_offset_returns_400():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/events?offset=-5")
        assert resp.status_code == 400
