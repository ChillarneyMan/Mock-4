import asyncio
import structlog

from app.models import Event

logger = structlog.get_logger(__name__)


class EventBuffer:
    """
    Thread-safe in-memory buffer for incoming events.

    Acts as a lightweight queue between the HTTP ingest path and the background
    flush worker. Uses an asyncio.Lock so concurrent requests and the flush
    worker can't race on the underlying list.
    """

    def __init__(self, max_size: int) -> None:
        self._max_size = max_size
        self._buffer: list[Event] = []
        self._lock = asyncio.Lock()

    async def append(self, event: Event) -> None:
        """Add a validated event to the buffer.

        Logs a warning if the buffer has hit its max size — this means the flush
        interval is too long for the incoming event rate. Does NOT drop the event;
        it still gets appended so nothing is silently lost.
        """
        async with self._lock:
            if len(self._buffer) >= self._max_size:
                logger.warning(
                    "buffer_max_size_exceeded",
                    max_size=self._max_size,
                    current_size=len(self._buffer),
                )
            self._buffer.append(event)

    async def snapshot_and_clear(self) -> list[Event]:
        """Atomically swap out the current buffer and return it.

        The swap happens under the lock so no event can appear in both the
        returned snapshot and the new live buffer. The flush worker calls this
        at the start of each cycle; any events that arrive during the upload
        go into the new buffer and are picked up on the next cycle.
        """
        async with self._lock:
            snapshot = self._buffer
            self._buffer = []
            return snapshot

    def current_size(self) -> int:
        """Return the number of events currently waiting to be flushed.

        Read without acquiring the lock — used only for observability (health
        endpoint), so a slightly stale value is acceptable.
        """
        return len(self._buffer)


from app.config import settings  # noqa: E402

# Module-level singleton shared by the ingest route and the flush worker.
buffer = EventBuffer(max_size=settings.BUFFER_MAX_SIZE)
