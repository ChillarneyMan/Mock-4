# event-service

A REST API that ingests application events and makes them queryable for analytics,
built to run entirely on DigitalOcean App Platform.

## Architecture

- **POST /events** — validates and buffers events in memory (immediate 400 on bad input, 202 on success)
- **Background flush worker** — every 30s, converts the buffer to Parquet and uploads to DO Spaces, partitioned by `event_type` and date (using the event's own timestamp, not wall-clock flush time)
- **GET /events / GET /events/stats** — DuckDB queries Parquet files directly from Spaces via httpfs; results reflect only durably-flushed data (accepted 30s staleness window)
- **GET /health** — returns buffer size, last flush status, and consecutive failure count

## Stack

| Component | Technology |
|-----------|-----------|
| API | FastAPI + uvicorn |
| Buffer | asyncio in-memory |
| Object storage | DigitalOcean Spaces (S3-compatible) |
| File format | Apache Parquet (PyArrow) |
| Query engine | DuckDB + httpfs |

## Running Locally

```bash
cp .env.example .env
# fill in your DO Spaces credentials in .env

pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Running Tests

```bash
make test
```

All tests run fully offline — no real Spaces or network calls.

## Deploying

See [DEPLOY.md](DEPLOY.md) for step-by-step `doctl` commands to deploy to DigitalOcean App Platform.

Deployed URL: _to be added once live_
