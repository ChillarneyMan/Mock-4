# Architecture

## Overview

event-service is a write-optimised event ingestion pipeline with a queryable analytics layer,
built entirely on DigitalOcean infrastructure. The core design trades a short, explicit
staleness window (≤30s) for cost efficiency and simplicity — no managed database, no
message queue, no streaming infrastructure.

## Diagram

```
                         ┌─────────────────────────────────┐
         ┌──────────────►│        App Platform             │
         │               │         (FastAPI)               │
         │               └──────────────┬──────────────────┘
         │                              │
         │                      POST /events
         │                              │
         │                    ┌─────────▼──────────┐
         │                    │  Sync Validation    │
         │                    │  (Pydantic model)   │
         │                    │  400 on bad input   │
         │                    └─────────┬───────────┘
         │                             │ valid
         │                    ┌────────▼────────────┐
         │                    │  In-Memory Buffer   │◄──────────────┐
         │                    │  (asyncio.Lock)     │               │
         │                    │  Acts like SQS but  │               │ re-append on
         │                    │  in-process memory  │               │ upload failure
         │                    └────────┬────────────┘               │
         │                             │ every 30s                  │
         │                    ┌────────▼────────────┐               │
         │                    │   Flush Worker      │               │
         │                    │  snapshot_and_clear │               │
         │                    │  group by           │               │
         │                    │  (event_type, date) │───────────────┘
         │                    │  → Parquet (memory) │  SpacesUploadError
         │                    └────────┬────────────┘  (retried next cycle)
         │                             │
         │               partition: events/event_type={t}/date={d}/{ts}-{uuid}.parquet
         │               date = event's OWN timestamp, not flush wall-clock time
         │                             │
         │                    ┌────────▼────────────┐
         │                    │    DO Spaces (S3)   │
         │                    │    Hive-partitioned │
         │                    │    Parquet files    │
         │                    └────────┬────────────┘
         │                             │
         │    ┌────────────────────────▼──────────────────────┐
         │    │              DuckDB (httpfs)                   │
         │    │  read_parquet('s3://...', hive_partitioning=1) │
         │    │  Parameterised SQL — aggregation in SQL        │
         │    └──────────────────┬────────────────────────────┘
         │                       │
         │    ┌──────────────────▼────────────────────────────┐
         └────┤          FastAPI Handler                       │
              │  GET /events         — filtered, paginated     │
              │  GET /events/stats   — GROUP BY in SQL         │
              │  GET /health         — buffer + flush status   │
              └───────────────────────────────────────────────┘

  ⚠ Staleness note: GET /events queries DuckDB only.
    Events still in the 30s buffer are NOT visible yet.
    Response includes "data_as_of" showing last successful flush timestamp.
```

## Data Flow Detail

### Write path
1. `POST /events` receives a JSON body and validates it synchronously against the `Event`
   Pydantic model. Invalid requests get an immediate `400` — they never touch the buffer.
2. Valid events are appended to the in-memory `EventBuffer` under an `asyncio.Lock`.
3. Every `BUFFER_FLUSH_INTERVAL_SECONDS` (default 30s), the flush worker atomically swaps
   the buffer for a new empty list (`snapshot_and_clear`), groups the snapshot by
   `(event_type, date)` — using the **event's own `timestamp`**, not wall-clock flush time
   — and serialises each group to Parquet in memory via PyArrow.
4. Each Parquet file is uploaded to DO Spaces at a Hive-partitioned path:
   `events/event_type={t}/date={d}/{flush_ts}-{uuid}.parquet`
5. On upload failure, events are re-appended to the live buffer for retry on the next cycle.

### Read path
1. `GET /events` and `GET /events/stats` build parameterised SQL queries against
   `read_parquet('s3://...', hive_partitioning=true)` via DuckDB's httpfs extension.
2. Filtering and aggregation (`GROUP BY`, `COUNT`) are pushed into SQL, not Python.
3. Every response includes `data_as_of` (last successful flush timestamp) and a staleness note.
