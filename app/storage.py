import asyncio
import structlog
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from app.config import settings

logger = structlog.get_logger(__name__)


class SpacesUploadError(Exception):
    """Raised when a Parquet upload to DO Spaces fails after all retries."""
    pass


class SpacesClient:
    """Thin wrapper around boto3's S3 client configured for DO Spaces.

    DO Spaces is S3-API-compatible, so boto3 works as-is with a custom
    endpoint_url pointing at the Spaces region endpoint.
    """

    def __init__(self) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.DO_SPACES_ENDPOINT,
            aws_access_key_id=settings.DO_SPACES_KEY,
            aws_secret_access_key=settings.DO_SPACES_SECRET,
            region_name=settings.DO_SPACES_REGION,
        )

    async def upload_parquet_bytes(self, data: bytes, key: str) -> None:
        """Upload raw Parquet bytes to DO Spaces at the given key.

        Retries up to MAX_FLUSH_RETRIES times with exponential backoff
        (1s, 2s, 4s, ...). Raises SpacesUploadError only after all retries
        are exhausted — the flush worker catches this and re-buffers the
        affected events rather than dropping them.

        The boto3 call is offloaded to a thread executor because boto3 is
        synchronous and would block the event loop otherwise.
        """
        last_exc: Exception | None = None
        for attempt in range(1, settings.MAX_FLUSH_RETRIES + 1):
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._client.put_object(
                        Bucket=settings.DO_SPACES_BUCKET,
                        Key=key,
                        Body=data,
                        ContentType="application/octet-stream",
                    ),
                )
                return
            except (BotoCoreError, ClientError, Exception) as exc:
                last_exc = exc
                logger.warning(
                    "spaces_upload_retry",
                    attempt=attempt,
                    max_retries=settings.MAX_FLUSH_RETRIES,
                    key=key,
                    error=str(exc),
                )
                if attempt < settings.MAX_FLUSH_RETRIES:
                    await asyncio.sleep(2 ** (attempt - 1))

        raise SpacesUploadError(
            f"Upload failed after {settings.MAX_FLUSH_RETRIES} attempts: {last_exc}"
        )


# Module-level singleton used by the flush worker.
spaces_client = SpacesClient()
