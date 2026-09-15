# event-service

A REST API that ingests application events and makes them queryable for analytics,
built entirely on DigitalOcean App Platform. Events are validated synchronously,
buffered in memory, converted to Parquet every 30 seconds, and uploaded to DO Spaces
using Hive-style partitioning. Queries are served by DuckDB reading those Parquet
files directly from Spaces via the httpfs extension, with aggregation pushed into SQL.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full pipeline diagram and data flow detail.

---

## API Reference

### `POST /events`

Ingest a single event. Validates synchronously — bad requests get an immediate 400,
never touch the buffer.

**Request body**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `event_type` | string | yes | non-empty |
| `source` | string | yes | non-empty |
| `timestamp` | string (ISO 8601) | yes | must parse as a datetime |
| `payload` | object | yes | must be a JSON object, not a string or array |

**Example request**
```bash
curl -X POST https://<your-app>/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "page_view",
    "source": "web",
    "timestamp": "2024-03-01T10:00:00Z",
    "payload": {"url": "/home", "referrer": "google.com"}
  }'
```

**202 Accepted** — event is buffered (not yet durably stored)
```json
{"status": "accepted", "buffered": true}
```

**400 Bad Request** — validation failed
```json
{
  "detail": [
    {"field": "event_type", "message": "Value error, event_type must be a non-empty string"}
  ]
}
```

---

### `GET /events`

Query flushed events. Results reflect only durably-stored data — events still in the
30s buffer will not appear yet (`data_as_of` shows when data was last written).

**Query params**

| Param | Type | Default | Notes |
|-------|------|---------|-------|
| `event_type` | string | — | filter by event type |
| `source` | string | — | filter by source |
| `start` | ISO 8601 datetime | — | inclusive lower bound on timestamp |
| `end` | ISO 8601 datetime | — | inclusive upper bound on timestamp |
| `limit` | int | 50 | max 500 |
| `offset` | int | 0 | for pagination |

**Example**
```bash
curl "https://<your-app>/events?event_type=page_view&limit=10"
```

**200 OK**
```json
{
  "data": [
    {
      "event_type": "page_view",
      "source": "web",
      "timestamp": "2024-03-01T10:00:00+00:00",
      "payload": "{'url': '/home'}"
    }
  ],
  "data_as_of": "2024-03-01T10:00:35.123456+00:00",
  "note": "Results reflect only durably-flushed data. Events in the current buffer window may not appear yet."
}
```

**400 Bad Request** — invalid limit or offset
```json
{"detail": "limit must not exceed 500"}
```

---

### `GET /events/stats`

Aggregate event counts grouped by `event_type`. Aggregation runs in DuckDB SQL, not Python.

**Query params**

| Param | Type | Notes |
|-------|------|-------|
| `start` | ISO 8601 datetime | optional time range filter |
| `end` | ISO 8601 datetime | optional time range filter |

**Example**
```bash
curl "https://<your-app>/events/stats?start=2024-03-01T00:00:00Z"
```

**200 OK**
```json
{
  "data": [
    {"event_type": "click", "count": 42},
    {"event_type": "page_view", "count": 198}
  ],
  "data_as_of": "2024-03-01T10:00:35.123456+00:00",
  "note": "Results reflect only durably-flushed data. Events in the current buffer window may not appear yet."
}
```

---

### `GET /health`

Health check used by App Platform. Returns operational status and basic observability.

**200 OK**
```json
{
  "status": "ok",
  "buffer_size": 12,
  "last_flush_at": "2024-03-01T10:00:35.123456+00:00",
  "last_flush_success": true,
  "last_flush_event_count": 47,
  "consecutive_flush_failures": 0
}
```

---

## Design Decisions & Trade-offs

### DuckDB + Spaces over Postgres

At telemetry scale, a managed Postgres instance becomes expensive fast — storage costs
grow linearly, analytical queries (aggregations over millions of rows) are slow without
heavy indexing, and you're paying for always-on compute. Parquet on object storage is
cheap at any volume, and DuckDB can run complex GROUP BY queries over hundreds of
Parquet files in milliseconds by pushing predicate evaluation down into the column reader.
The trade-off is no row-level writes — this is append-only, batch-oriented storage, not
a transactional database. It's the right call for event analytics; it would be wrong for
anything requiring immediate consistency or updates.

### Accepted staleness window

`GET /events` queries DuckDB only. Events still in the 30s in-memory buffer are invisible
to reads. This is deliberate: merging the live buffer into every read would mean holding
the buffer lock for the duration of a potentially slow DuckDB query, creating contention
with the flush worker. The 30s window is explicit in the API response (`data_as_of`,
staleness note) so callers know exactly what they're getting. For most analytics use cases
this is fine; for real-time monitoring you'd want a separate hot path.

### Flush failure and retry via rebuffer

When a Spaces upload fails after retries, the affected events are re-appended to the live
buffer rather than dropped. This means they'll be retried on the next flush cycle.
The current limitation is that this retry state lives only in memory — if the process
crashes between a failed flush and the next cycle, those events are lost. A real
production system would put events into an actual message queue (SQS, Kafka, Redis
Streams) before acknowledging the write, so a DLQ could catch unprocessable events and a
crash doesn't lose data. That's the first architectural change to make before taking real
user traffic.

### What changes at 10x / 100x scale

**10x** (~10k events/min): the 30s buffer might hold 5,000+ events before flush. Reduce
`BUFFER_FLUSH_INTERVAL_SECONDS`, add horizontal App Platform instances, but note that
multiple instances each have their own buffer — Parquet files from different instances
land in the same Spaces bucket and DuckDB reads all of them, so reads stay correct. The
asyncio lock in the buffer becomes the main bottleneck; move to a separate process or a
real queue.

**100x** (~100k events/min): in-process buffering breaks down entirely. Replace the
in-memory buffer with Kafka or SQS. Move the flush worker to a separate consumer service.
Consider Parquet compaction (many small files → fewer large files) to keep DuckDB query
performance consistent as the file count in Spaces grows.

---

## Running Locally

```bash
# 1. Copy env template and fill in your DO Spaces credentials
cp .env.example .env

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the dev server
uvicorn app.main:app --reload

# API available at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

## Running Tests

```bash
make test
```

All 29 tests run fully offline — no real Spaces or network calls are made.
Coverage: 87% across the `app/` package.

## Deploying

See [DEPLOY.md](DEPLOY.md) for step-by-step `doctl` commands.

Deployed URL: _to be added once live_
