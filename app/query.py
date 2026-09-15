from datetime import datetime
from typing import Any

import duckdb
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

# Glob pattern that covers all Hive-partitioned Parquet files in the bucket.
# DuckDB reads this with hive_partitioning=true so it can prune by event_type/date.
_PARQUET_GLOB = f"s3://{settings.DO_SPACES_BUCKET}/events/*/*/*.parquet"


class DuckDBQueryClient:
    """Query layer that reads Parquet files from DO Spaces via DuckDB's httpfs extension.

    All filtering and aggregation is pushed into SQL — no Python-side processing
    of rows. Uses parameterised queries (? placeholders) for all user-supplied
    values so there is no risk of SQL injection.
    """

    def __init__(self) -> None:
        """Open a DuckDB connection and configure the S3/httpfs settings for DO Spaces.

        DO Spaces uses path-style URLs (s3_url_style='path'), which differs from
        AWS S3's default virtual-hosted style.
        """
        self._con = duckdb.connect()
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        self._con.execute(f"SET s3_endpoint='{settings.DO_SPACES_ENDPOINT.removeprefix('https://')}';")
        self._con.execute(f"SET s3_access_key_id='{settings.DO_SPACES_KEY}';")
        self._con.execute(f"SET s3_secret_access_key='{settings.DO_SPACES_SECRET}';")
        self._con.execute(f"SET s3_region='{settings.DO_SPACES_REGION}';")
        self._con.execute("SET s3_url_style='path';")

    def _run(self, sql: str, params: list[Any]) -> list[dict]:
        """Execute a parameterised SQL query and return rows as dicts.

        Returns an empty list if no Parquet files exist yet (fresh deployment)
        rather than raising — DuckDB throws an IOException when the glob matches
        nothing. Any other unexpected errors are logged and also return empty.
        """
        try:
            result = self._con.execute(sql, params)
            cols = [d[0] for d in result.description]
            return [dict(zip(cols, row)) for row in result.fetchall()]
        except duckdb.IOException:
            # No parquet files exist yet — return empty rather than 500.
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
        """Return individual events matching the given filters, ordered by timestamp.

        Only adds WHERE clauses for filters that were actually provided — omitting
        a filter means no restriction on that column. Results are paginated via
        LIMIT/OFFSET.

        parquet_glob overrides the default S3 path and is used by tests to point
        DuckDB at a local temp directory instead of real Spaces.
        """
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
        """Return event counts grouped by event_type and date.

        Aggregation runs entirely in DuckDB SQL. The optional start/end filters
        apply to the event's own timestamp before grouping.

        Returns rows of the form: {event_type, date, count}.

        parquet_glob overrides the default S3 path for tests.
        """
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
            SELECT event_type, date, COUNT(*) AS count
            FROM read_parquet('{glob}', hive_partitioning=true)
            {where}
            GROUP BY event_type, date
            ORDER BY event_type, date
        """
        return self._run(sql, params)


# Module-level singleton used by the route handlers.
query_client = DuckDBQueryClient()
