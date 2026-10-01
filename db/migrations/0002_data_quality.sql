-- depends: 0001_sensor_readings
-- Day 2: data-quality handling and the state behind /health.

-- Stale or null Luchtmeetnet readings are kept but flagged, never dropped: a gap in the time
-- series is worse than a flagged value for this use case.
ALTER TABLE sensor_readings ADD COLUMN is_flagged BOOLEAN NOT NULL DEFAULT FALSE;

-- One row per ingestion source. The ingestion containers are short-lived cron jobs, so this is
-- the only place their last-successful-fetch and bad-data counter can live; /health reads it.
CREATE TABLE ingestion_status (
    source                 VARCHAR(20)  PRIMARY KEY,   -- 'luchtmeetnet' | 'ndw'
    last_successful_fetch  TIMESTAMPTZ,
    last_attempt           TIMESTAMPTZ,
    last_error             TEXT,
    bad_data_count         INTEGER      NOT NULL DEFAULT 0
);

-- Every DATA_QUALITY_ERROR event, so "more than 10 in the last hour" is a query, not a guess.
CREATE TABLE bad_data_events (
    id          BIGSERIAL     PRIMARY KEY,
    source      VARCHAR(20)   NOT NULL,
    location    VARCHAR(40),              -- station id or NDW site id
    field       VARCHAR(20),              -- 'NO2' | 'speed'
    value       TEXT,
    reason      VARCHAR(40),
    detected_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX bad_data_events_src_ts_idx ON bad_data_events (source, detected_at DESC);
