import asyncio
import pytest
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.buffer import buffer


VALID_EVENT = {
    "event_type": "page_view",
    "source": "web",
    "timestamp": "2024-01-15T12:00:00Z",
    "payload": {"url": "/home"},
}


@pytest.fixture(autouse=True)
async def clear_buffer():
    await buffer.snapshot_and_clear()
    yield
    await buffer.snapshot_and_clear()


@pytest.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_valid_event_returns_202(client):
    resp = await client.post("/events", json=VALID_EVENT)
    assert resp.status_code == 202
    assert resp.json() == {"status": "accepted", "buffered": True}


@pytest.mark.asyncio
async def test_missing_required_field_returns_400(client):
    for field in ["event_type", "source", "timestamp", "payload"]:
        body = {k: v for k, v in VALID_EVENT.items() if k != field}
        resp = await client.post("/events", json=body)
        assert resp.status_code == 400
        fields_in_error = [e["field"] for e in resp.json()["detail"]]
        assert field in fields_in_error, f"Expected '{field}' in error detail"


@pytest.mark.asyncio
async def test_malformed_timestamp_returns_400(client):
    body = {**VALID_EVENT, "timestamp": "not-a-date"}
    resp = await client.post("/events", json=body)
    assert resp.status_code == 400
    fields_in_error = [e["field"] for e in resp.json()["detail"]]
    assert "timestamp" in fields_in_error


@pytest.mark.asyncio
async def test_payload_string_returns_400(client):
    body = {**VALID_EVENT, "payload": "a string"}
    resp = await client.post("/events", json=body)
    assert resp.status_code == 400
    fields_in_error = [e["field"] for e in resp.json()["detail"]]
    assert "payload" in fields_in_error


@pytest.mark.asyncio
async def test_payload_array_returns_400(client):
    body = {**VALID_EVENT, "payload": [1, 2, 3]}
    resp = await client.post("/events", json=body)
    assert resp.status_code == 400
    fields_in_error = [e["field"] for e in resp.json()["detail"]]
    assert "payload" in fields_in_error


@pytest.mark.asyncio
async def test_empty_event_type_returns_400(client):
    body = {**VALID_EVENT, "event_type": ""}
    resp = await client.post("/events", json=body)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_multiple_valid_events_buffer_size(client):
    for _ in range(5):
        await client.post("/events", json=VALID_EVENT)
    assert buffer.current_size() == 5


@pytest.mark.asyncio
async def test_snapshot_and_clear_empties_buffer(client):
    for _ in range(3):
        await client.post("/events", json=VALID_EVENT)

    snapshot = await buffer.snapshot_and_clear()
    assert len(snapshot) == 3
    assert buffer.current_size() == 0


@pytest.mark.asyncio
async def test_concurrent_appends_during_snapshot(client):
    async def post_event():
        await client.post("/events", json=VALID_EVENT)

    # Fire 10 concurrent appends alongside a snapshot
    results = await asyncio.gather(
        *[post_event() for _ in range(10)],
        buffer.snapshot_and_clear(),
        return_exceptions=True,
    )

    snapshot = results[-1]
    remaining = buffer.current_size()

    # All 10 events must be accounted for: in the snapshot or still in the buffer
    assert len(snapshot) + remaining == 10
