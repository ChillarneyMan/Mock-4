import asyncio
import structlog
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from app.config import settings

logger = structlog.get_logger(__name__)


class SpacesUploadError(Exception):
    pass


class SpacesClient:
    def __init__(self) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.DO_SPACES_ENDPOINT,
            aws_access_key_id=settings.DO_SPACES_KEY,
            aws_secret_access_key=settings.DO_SPACES_SECRET,
            region_name=settings.DO_SPACES_REGION,
        )

    async def upload_parquet_bytes(self, data: bytes, key: str) -> None:
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


spaces_client = SpacesClient()
