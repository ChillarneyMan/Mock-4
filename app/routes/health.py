from fastapi import APIRouter

from app.buffer import buffer
from app.flush_worker import flush_status

router = APIRouter()


@router.get("/health")
def health():
    return {
        "status": "ok",
        "buffer_size": buffer.current_size(),
        "last_flush_at": flush_status.last_flush_at.isoformat() if flush_status.last_flush_at else None,
        "last_flush_success": flush_status.last_flush_success,
        "last_flush_event_count": flush_status.last_flush_event_count,
        "consecutive_flush_failures": flush_status.consecutive_failures,
    }
