import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.models import Event
from app.buffer import buffer, EventBuffer
from app.flush_worker import _flush_once, flush_status
from app.storage import SpacesUploadError


def make_event(event_type="page_view", source="web", timestamp="2024-01-15T12:00:00Z", payload=None):
    return Event(
        event_type=event_type,
        source=source,
        timestamp=datetime.fromisoformat(timestamp.replace("Z", "+00:00")),
        payload=payload or {},
    )


def make_fake_client(fail=False):
    client = MagicMock()
    if fail:
        client.upload_parquet_bytes = AsyncMock(
            side_effect=SpacesUploadError("simulated failure")
        )
    else:
        client.upload_parquet_bytes = AsyncMock(return_value=None)
    return client


@pytest.fixture(autouse=True)
async def clear_buffer():
    await buffer.snapshot_and_clear()
    flush_status.consecutive_failures = 0
    flush_status.last_flush_success = None
    yield
    await buffer.snapshot_and_clear()


@pytest.mark.asyncio
async def test_single_partition_produces_one_upload():
    events = [make_event() for _ in range(3)]
    for e in events:
        await buffer.append(e)

    client = make_fake_client()
    await _flush_once(client)

    assert client.upload_parquet_bytes.call_count == 1
    key = client.upload_parquet_bytes.call_args[0][1]
    assert "event_type=page_view" in key
    assert "date=2024-01-15" in key
    assert key.endswith(".parquet")


@pytest.mark.asyncio
async def test_two_event_types_produces_two_uploads():
    await buffer.append(make_event(event_type="page_view"))
    await buffer.append(make_event(event_type="click"))

    client = make_fake_client()
    await _flush_once(client)

    assert client.upload_parquet_bytes.call_count == 2
    keys = [call[0][1] for call in client.upload_parquet_bytes.call_args_list]
    types_in_keys = {k.split("/")[1] for k in keys}
    assert types_in_keys == {"event_type=page_view", "event_type=click"}


@pytest.mark.asyncio
async def test_empty_buffer_produces_no_uploads():
    client = make_fake_client()
    await _flush_once(client)
    client.upload_parquet_bytes.assert_not_called()


@pytest.mark.asyncio
async def test_upload_failure_rebuffers_events():
    events = [make_event() for _ in range(4)]
    for e in events:
        await buffer.append(e)

    client = make_fake_client(fail=True)
    await _flush_once(client)

    # Events must be re-buffered, not lost
    assert buffer.current_size() == 4


@pytest.mark.asyncio
async def test_partition_path_uses_event_timestamp_not_flush_time():
    # Event timestamp is 2024-01-15; flush happens at a different wall-clock time
    event = make_event(timestamp="2024-01-15T23:59:00Z")
    await buffer.append(event)

    client = make_fake_client()

    # Patch datetime.now inside flush_worker to simulate flush at a different date
    fake_now = datetime(2024, 1, 16, 0, 5, 0, tzinfo=timezone.utc)
    with patch("app.flush_worker.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.fromisoformat = datetime.fromisoformat
        await _flush_once(client)

    key = client.upload_parquet_bytes.call_args[0][1]
    # Partition date must be the event's date (Jan 15), not flush date (Jan 16)
    assert "date=2024-01-15" in key
    assert "date=2024-01-16" not in key


@pytest.mark.asyncio
async def test_consecutive_failures_increment():
    await buffer.append(make_event())
    client = make_fake_client(fail=True)

    await _flush_once(client)
    assert flush_status.consecutive_failures == 1

    # Rebuffered events need to be there for a second flush
    await _flush_once(client)
    assert flush_status.consecutive_failures == 2
