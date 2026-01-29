import os
from typing import Any, Dict, List, Optional

import psycopg
from fastapi import FastAPI, Query
from psycopg.rows import dict_row

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://platform:platform@localhost:5432/platform")

app = FastAPI(title="Data Pipeline Platform - Serving API", version="0.1.0")


def db() -> psycopg.Connection:
    return psycopg.connect(POSTGRES_DSN, row_factory=dict_row)


@app.get("/health")
def health() -> Dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok;")
            row = cur.fetchone()
    return {"status": "ok", "db": row["ok"]}


@app.get("/stats/event-types")
def stats_event_types(minutes: int = Query(60, ge=1, le=1440)) -> List[Dict[str, Any]]:
    sql = """
      SELECT event_type, COUNT(*)::bigint AS event_count
      FROM raw_events
      WHERE occurred_at >= NOW() - (%s || ' minutes')::interval
      GROUP BY event_type
      ORDER BY event_count DESC;
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (minutes,))
            return cur.fetchall()


@app.get("/stats/timeseries")
def stats_timeseries(
    minutes: int = Query(60, ge=1, le=1440),
    bucket_seconds: int = Query(60, ge=10, le=3600),
    event_type: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    # Bucket timestamps using epoch math (simple + fast for demo)
    sql = """
      SELECT
        to_timestamp(floor(extract(epoch from occurred_at) / %s) * %s) AT TIME ZONE 'UTC' AS bucket_utc,
        event_type,
        COUNT(*)::bigint AS event_count
      FROM raw_events
      WHERE occurred_at >= NOW() - (%s || ' minutes')::interval
        AND (%s::text IS NULL OR event_type = %s)
      GROUP BY bucket_utc, event_type
      ORDER BY bucket_utc DESC, event_count DESC
      LIMIT 5000;
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (bucket_seconds, bucket_seconds, minutes, event_type, event_type))
            return cur.fetchall()


@app.get("/stats/ingestion-lag")
def ingestion_lag(minutes: int = Query(60, ge=1, le=1440)) -> Dict[str, Any]:
    sql = """
      SELECT
        COUNT(*)::bigint AS n,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))) AS p50_s,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))) AS p95_s,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))) AS p99_s
      FROM raw_events
      WHERE occurred_at >= NOW() - (%s || ' minutes')::interval;
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (minutes,))
            row = cur.fetchone()
            return dict(row)
