import asyncio
import io
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from app.buffer import buffer
from app.config import settings
from app.models import Event
from app.storage import SpacesClient, SpacesUploadError, spaces_client

logger = structlog.get_logger(__name__)


class FlushStatus:
    def __init__(self) -> None:
        self.last_flush_at: Optional[datetime] = None
        self.last_flush_success: Optional[bool] = None
        self.last_flush_event_count: int = 0
        self.consecutive_failures: int = 0


flush_status = FlushStatus()


def _events_to_parquet_bytes(events: list[Event]) -> bytes:
    data = {
        "event_type": [e.event_type for e in events],
        "source": [e.source for e in events],
        "timestamp": [e.timestamp.isoformat() for e in events],
        "payload": [str(e.payload) for e in events],
    }
    table = pa.table(data)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _partition_key(event_type: str, date: str, flush_ts: str) -> str:
    file_id = uuid.uuid4()
    return f"events/event_type={event_type}/date={date}/{flush_ts}-{file_id}.parquet"


async def _flush_once(client: SpacesClient) -> None:
    snapshot = await buffer.snapshot_and_clear()
    if not snapshot:
        return

    flush_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Group by (event_type, date) using each event's own timestamp
    groups: dict[tuple[str, str], list[Event]] = defaultdict(list)
    for event in snapshot:
        date = event.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d")
        groups[(event.event_type, date)].append(event)

    failed_events: list[Event] = []

    for (event_type, date), group in groups.items():
        key = _partition_key(event_type, date, flush_ts)
        parquet_bytes = _events_to_parquet_bytes(group)
        try:
            await client.upload_parquet_bytes(parquet_bytes, key)
            logger.info(
                "flush_partition_uploaded",
                key=key,
                event_count=len(group),
            )
        except SpacesUploadError as exc:
            logger.error(
                "flush_partition_failed_rebuffering",
                key=key,
                event_count=len(group),
                error=str(exc),
            )
            failed_events.extend(group)

    total = len(snapshot)
    failed = len(failed_events)

    if failed_events:
        for event in failed_events:
            await buffer.append(event)
        flush_status.consecutive_failures += 1
        flush_status.last_flush_success = False
        if flush_status.consecutive_failures >= 5:
            logger.critical(
                "flush_consecutive_failures_alert",
                consecutive_failures=flush_status.consecutive_failures,
                rebuffered_count=failed,
            )
        else:
            logger.error(
                "flush_events_rebuffered",
                rebuffered_count=failed,
                consecutive_failures=flush_status.consecutive_failures,
            )
    else:
        flush_status.consecutive_failures = 0
        flush_status.last_flush_success = True

    flush_status.last_flush_at = datetime.now(timezone.utc)
    flush_status.last_flush_event_count = total - failed


async def flush_loop(client: SpacesClient | None = None) -> None:
    client = client or spaces_client
    while True:
        await asyncio.sleep(settings.BUFFER_FLUSH_INTERVAL_SECONDS)
        try:
            await _flush_once(client)
        except Exception as exc:
            logger.error("flush_loop_unexpected_error", error=str(exc))
