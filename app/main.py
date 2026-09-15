import asyncio
import logging
import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.config import settings
from app.routes import health, events


def configure_logging() -> None:
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    from app.flush_worker import flush_loop
    task = asyncio.create_task(flush_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="event-service", lifespan=lifespan)

app.include_router(health.router)
app.include_router(events.router)
