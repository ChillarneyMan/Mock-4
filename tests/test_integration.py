"""
Integration tests: full pipeline POST → flush → GET, using a local fake SpacesClient
and a local DuckDB client pointed at a temp directory. No real network calls.
"""
import os
import tempfile
import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

import duckdb

from app.main import app
from app.buffer import buffer
from app.flush_worker import _flush_once, flush_status
from app.query import DuckDBQueryClient
from app.storage import SpacesUploadError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_EVENT = {
    "event_type": "page_view",
    "source": "web",
    "timestamp": "2024-03-01T10:00:00Z",
    "payload": {"url": "/home"},
}

VALID_EVENT_2 = {
    "event_type": "click",
    "source": "mobile",
    "timestamp": "2024-03-01T11:00:00Z",
    "payload": {"button": "signup"},
}

INVALID_EVENT = {
    "event_type": "",          # empty — should 400
    "source": "web",
    "timestamp": "2024-03-01T10:00:00Z",
    "payload": {},
}


class LocalSpacesClient:
    """Writes parquet bytes to a local temp dir instead of DO Spaces."""

    def __init__(self, tmpdir: str, fail: bool = False) -> None:
        self._tmpdir = tmpdir
        self._fail = fail
        self.upload_calls: list[str] = []

    async def upload_parquet_bytes(self, data: bytes, key: str) -> None:
        if self._fail:
            raise SpacesUploadError("simulated upload failure")
        path = os.path.join(self._tmpdir, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        self.upload_calls.append(key)


class LocalQueryClient(DuckDBQueryClient):
    """DuckDB client reading from a local temp dir instead of s3://."""

    def __init__(self, tmpdir: str) -> None:
        self._con = duckdb.connect()
        self._glob = f"{tmpdir}/events/**/*.parquet"

    def get_events(self, event_type, source, start, end, limit, offset, parquet_glob=None):
        return super().get_events(event_type, source, start, end, limit, offset, parquet_glob=self._glob)

    def get_event_stats(self, start, end, parquet_glob=None):
        return super().get_event_stats(start, end, parquet_glob=self._glob)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
async def reset_state():
    await buffer.snapshot_and_clear()
    flush_status.last_flush_at = None
    flush_status.last_flush_success = None
    flush_status.last_flush_event_count = 0
    flush_status.consecutive_failures = 0
    yield
    await buffer.snapshot_and_clear()


@pytest.fixture
def pipeline():
    """Yields (AsyncClient, LocalSpacesClient, LocalQueryClient, tmpdir)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        spaces = LocalSpacesClient(tmpdir)
        qclient = LocalQueryClient(tmpdir)
        yield spaces, qclient, tmpdir


@pytest.fixture
def failing_pipeline():
    """Like pipeline but the SpacesClient always fails on first use."""
    with tempfile.TemporaryDirectory() as tmpdir:
        spaces = LocalSpacesClient(tmpdir, fail=True)
        qclient = LocalQueryClient(tmpdir)
        yield spaces, qclient, tmpdir


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_post_flush_get_events(pipeline):
    spaces, qclient, tmpdir = pipeline
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # POST two events
        r1 = await ac.post("/events", json=VALID_EVENT)
        r2 = await ac.post("/events", json=VALID_EVENT_2)
        assert r1.status_code == 202
        assert r2.status_code == 202

        # Flush to local disk
        await _flush_once(spaces)

        # Query through patched local client
        with patch("app.routes.events.query_client", qclient):
            resp = await ac.get("/events")

    assert resp.status_code == 200
    data = resp.json()["data"]
    event_types = {r["event_type"] for r in data}
    assert event_types == {"page_view", "click"}
    assert len(data) == 2


@pytest.mark.asyncio
async def test_post_flush_get_stats(pipeline):
    spaces, qclient, tmpdir = pipeline
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        for _ in range(3):
            await ac.post("/events", json=VALID_EVENT)
        await ac.post("/events", json=VALID_EVENT_2)

        await _flush_once(spaces)

        with patch("app.routes.events.query_client", qclient):
            resp = await ac.get("/events/stats")

    assert resp.status_code == 200
    by_type = {r["event_type"]: r["count"] for r in resp.json()["data"]}
    assert by_type["page_view"] == 3
    assert by_type["click"] == 1


@pytest.mark.asyncio
async def test_event_absent_before_flush(pipeline):
    """Events in the buffer must NOT appear in GET /events until after flush."""
    spaces, qclient, tmpdir = pipeline
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/events", json=VALID_EVENT)

        # Query BEFORE flushing — temp dir is empty
        with patch("app.routes.events.query_client", qclient):
            resp = await ac.get("/events")

    assert resp.status_code == 200
    assert resp.json()["data"] == []


@pytest.mark.asyncio
async def test_mixed_valid_invalid_only_valid_after_flush(pipeline):
    spaces, qclient, tmpdir = pipeline
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        good = await ac.post("/events", json=VALID_EVENT)
        bad = await ac.post("/events", json=INVALID_EVENT)
        assert good.status_code == 202
        assert bad.status_code == 400

        await _flush_once(spaces)

        with patch("app.routes.events.query_client", qclient):
            resp = await ac.get("/events")

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert len(data) == 1
    assert data[0]["event_type"] == "page_view"


@pytest.mark.asyncio
async def test_retry_via_rebuffer(failing_pipeline):
    """Upload failure → events re-buffered → second flush succeeds."""
    failing_spaces, qclient, tmpdir = failing_pipeline

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/events", json=VALID_EVENT)

        # First flush: upload fails → events re-buffered
        await _flush_once(failing_spaces)
        assert buffer.current_size() == 1
        assert flush_status.consecutive_failures == 1

        # Second flush: use a working client
        working_spaces = LocalSpacesClient(tmpdir, fail=False)
        await _flush_once(working_spaces)
        assert buffer.current_size() == 0
        assert flush_status.consecutive_failures == 0

        with patch("app.routes.events.query_client", qclient):
            resp = await ac.get("/events")

    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 1
