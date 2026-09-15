from fastapi import APIRouter

from app.buffer import buffer
from app.flush_worker import flush_status

router = APIRouter()


@router.get("/health")
def health():
    """Health check endpoint used by App Platform's readiness probe.

    Returns the current operational status plus basic observability fields:
    - buffer_size: events waiting to be flushed
    - last_flush_at: when the last flush cycle completed
    - last_flush_success: whether the last flush uploaded without error
    - last_flush_event_count: how many events were written in the last flush
    - consecutive_flush_failures: number of back-to-back failed flush cycles
      (a CRITICAL log is emitted at 5, which is the on-call alert hook)
    """
    return {
        "status": "ok",
        "buffer_size": buffer.current_size(),
        "last_flush_at": flush_status.last_flush_at.isoformat() if flush_status.last_flush_at else None,
        "last_flush_success": flush_status.last_flush_success,
        "last_flush_event_count": flush_status.last_flush_event_count,
        "consecutive_flush_failures": flush_status.consecutive_failures,
    }
