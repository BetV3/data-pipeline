CREATE OR REPLACE VIEW event_counts_by_type_60m AS
SELECT
  event_type,
  COUNT(*) AS event_count
FROM raw_events
WHERE occurred_at >= NOW() - INTERVAL '60 minutes'
GROUP BY event_type
ORDER BY event_count DESC;

CREATE OR REPLACE VIEW event_counts_per_minute_60m AS
SELECT
  date_trunc('minute', occurred_at) AS minute_bucket,
  event_type,
  COUNT(*) AS event_count
FROM raw_events
WHERE occurred_at >= NOW() - INTERVAL '60 minutes'
GROUP BY minute_bucket, event_type
ORDER BY minute_bucket DESC, event_count DESC;

-- Ingestion lag (seconds) stats over last 60 minutes
CREATE OR REPLACE VIEW ingestion_lag_stats_60m AS
SELECT
  COUNT(*) AS n,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))) AS p50_s,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))) AS p95_s,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))) AS p99_s
FROM raw_events
WHERE occurred_at >= NOW() - INTERVAL '60 minutes';
