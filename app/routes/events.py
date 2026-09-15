import structlog
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.models import Event, EventAccepted
from app.buffer import buffer
from app.flush_worker import flush_status
from app.query import query_client

logger = structlog.get_logger(__name__)
router = APIRouter()

_MAX_LIMIT = 500


def _data_as_of() -> str | None:
    """Return the last successful flush timestamp as an ISO string, or None.

    Included in every query response so callers know exactly how stale the
    data might be. None means no flush has completed since the process started
    (data from previous runs may still be queryable via Spaces).
    """
    if flush_status.last_flush_at:
        return flush_status.last_flush_at.isoformat()
    return None


@router.post("/events", status_code=202, response_model=EventAccepted)
async def ingest_event(request: Request):
    """Ingest a single event into the in-memory buffer.

    Validates the request body synchronously against the Event Pydantic model.
    Invalid requests get an immediate 400 and never touch the buffer.
    Valid events are appended to the buffer and will be flushed to DO Spaces
    within BUFFER_FLUSH_INTERVAL_SECONDS seconds (default 30s).

    Returns 202 Accepted — not 200 — because the event is buffered but not yet
    durably stored. It will appear in GET /events only after the next flush.
    """
    try:
        body = await request.json()
        event = Event.model_validate(body)
    except ValidationError as exc:
        errors = [
            {"field": e["loc"][-1] if e["loc"] else "unknown", "message": e["msg"]}
            for e in exc.errors()
        ]
        logger.warning("event_validation_failed", errors=errors)
        return JSONResponse(status_code=400, content={"detail": errors})
    except Exception:
        return JSONResponse(
            status_code=400, content={"detail": "invalid JSON body"}
        )

    await buffer.append(event)
    logger.debug(
        "event_accepted",
        event_type=event.event_type,
        source=event.source,
        timestamp=event.timestamp.isoformat(),
    )
    return EventAccepted()


@router.get("/events")
async def get_events(
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    source: Optional[str] = Query(None, description="Filter by source"),
    start: Optional[datetime] = Query(None, description="Inclusive lower bound on event timestamp"),
    end: Optional[datetime] = Query(None, description="Inclusive upper bound on event timestamp"),
    limit: int = Query(50, description="Max results to return (max 500)"),
    offset: int = Query(0, description="Pagination offset"),
):
    """Return individual events from DO Spaces, filtered and paginated.

    Queries DuckDB directly against the Hive-partitioned Parquet files in Spaces.
    Events still in the 30s in-memory buffer are NOT visible here — the response
    includes 'data_as_of' showing when the data was last written to Spaces.

    All filter parameters are optional. Omitting a filter returns all values for
    that column. Results are ordered by event timestamp ascending.
    """
    if limit < 0 or offset < 0:
        return JSONResponse(
            status_code=400,
            content={"detail": "limit and offset must be non-negative"},
        )
    if limit > _MAX_LIMIT:
        return JSONResponse(
            status_code=400,
            content={"detail": f"limit must not exceed {_MAX_LIMIT}"},
        )

    rows = query_client.get_events(
        event_type=event_type,
        source=source,
        start=start,
        end=end,
        limit=limit,
        offset=offset,
    )
    return {
        "data": rows,
        "data_as_of": _data_as_of(),
        "note": "Results reflect only durably-flushed data. Events in the current buffer window may not appear yet.",
    }


@router.get("/events/stats")
async def get_events_stats(
    start: Optional[datetime] = Query(None, description="Inclusive lower bound on event timestamp"),
    end: Optional[datetime] = Query(None, description="Inclusive upper bound on event timestamp"),
):
    """Return event counts aggregated by event_type and date.

    Aggregation runs entirely in DuckDB SQL (GROUP BY event_type, date).
    Each row represents the count of a specific event type on a specific date.
    The optional start/end filters apply to the event's own timestamp before
    grouping, so they affect which events are counted.

    Like GET /events, results reflect only durably-flushed data.
    """
    rows = query_client.get_event_stats(start=start, end=end)
    return {
        "data": rows,
        "data_as_of": _data_as_of(),
        "note": "Results reflect only durably-flushed data. Events in the current buffer window may not appear yet.",
    }
