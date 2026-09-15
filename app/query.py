from datetime import datetime
from typing import Any

import duckdb
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

_PARQUET_GLOB = f"s3://{settings.DO_SPACES_BUCKET}/events/*/*/*.parquet"


class DuckDBQueryClient:
    def __init__(self) -> None:
        self._con = duckdb.connect()
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        self._con.execute(f"SET s3_endpoint='{settings.DO_SPACES_ENDPOINT.removeprefix('https://')}';")
        self._con.execute(f"SET s3_access_key_id='{settings.DO_SPACES_KEY}';")
        self._con.execute(f"SET s3_secret_access_key='{settings.DO_SPACES_SECRET}';")
        self._con.execute(f"SET s3_region='{settings.DO_SPACES_REGION}';")
        self._con.execute("SET s3_url_style='path';")

    def _run(self, sql: str, params: list[Any]) -> list[dict]:
        try:
            result = self._con.execute(sql, params)
            cols = [d[0] for d in result.description]
            return [dict(zip(cols, row)) for row in result.fetchall()]
        except duckdb.IOException:
            # No parquet files exist yet (fresh deployment)
            return []
        except Exception as exc:
            logger.error("duckdb_query_error", error=str(exc))
            return []

    def get_events(
        self,
        event_type: str | None,
        source: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
        parquet_glob: str | None = None,
    ) -> list[dict]:
        glob = parquet_glob or _PARQUET_GLOB
        clauses: list[str] = []
        params: list[Any] = []

        if event_type is not None:
            clauses.append("event_type = ?")
            params.append(event_type)
        if source is not None:
            clauses.append("source = ?")
            params.append(source)
        if start is not None:
            clauses.append("timestamp >= ?")
            params.append(start.isoformat())
        if end is not None:
            clauses.append("timestamp <= ?")
            params.append(end.isoformat())

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT event_type, source, timestamp, payload
            FROM read_parquet('{glob}', hive_partitioning=true)
            {where}
            ORDER BY timestamp
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        return self._run(sql, params)

    def get_event_stats(
        self,
        start: datetime | None,
        end: datetime | None,
        parquet_glob: str | None = None,
    ) -> list[dict]:
        glob = parquet_glob or _PARQUET_GLOB
        clauses: list[str] = []
        params: list[Any] = []

        if start is not None:
            clauses.append("timestamp >= ?")
            params.append(start.isoformat())
        if end is not None:
            clauses.append("timestamp <= ?")
            params.append(end.isoformat())

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT event_type, COUNT(*) AS count
            FROM read_parquet('{glob}', hive_partitioning=true)
            {where}
            GROUP BY event_type
            ORDER BY event_type
        """
        return self._run(sql, params)


query_client = DuckDBQueryClient()
