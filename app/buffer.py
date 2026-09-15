import asyncio
import structlog

from app.models import Event

logger = structlog.get_logger(__name__)


class EventBuffer:
    def __init__(self, max_size: int) -> None:
        self._max_size = max_size
        self._buffer: list[Event] = []
        self._lock = asyncio.Lock()

    async def append(self, event: Event) -> None:
        async with self._lock:
            if len(self._buffer) >= self._max_size:
                logger.warning(
                    "buffer_max_size_exceeded",
                    max_size=self._max_size,
                    current_size=len(self._buffer),
                )
            self._buffer.append(event)

    async def snapshot_and_clear(self) -> list[Event]:
        async with self._lock:
            snapshot = self._buffer
            self._buffer = []
            return snapshot

    def current_size(self) -> int:
        return len(self._buffer)


from app.config import settings  # noqa: E402

buffer = EventBuffer(max_size=settings.BUFFER_MAX_SIZE)
