-- Initial database schema for T0

CREATE TABLE IF NOT EXISTS raw_events (
  id            BIGSERIAL PRIMARY KEY,
  event_id      TEXT NOT NULL,
  event_type    TEXT NOT NULL,
  source        TEXT NOT NULL,
  occurred_at   TIMESTAMPTZ NOT NULL,
  payload_json  JSONB NOT NULL,
  ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_occurred_at ON raw_events(occurred_at);