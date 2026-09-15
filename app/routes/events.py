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
    if flush_status.last_flush_at:
        return flush_status.last_flush_at.isoformat()
    return None


@router.post("/events", status_code=202, response_model=EventAccepted)
async def ingest_event(request: Request):
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
    event_type: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
):
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
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
):
    rows = query_client.get_event_stats(start=start, end=end)
    return {
        "data": rows,
        "data_as_of": _data_as_of(),
        "note": "Results reflect only durably-flushed data. Events in the current buffer window may not appear yet.",
    }
