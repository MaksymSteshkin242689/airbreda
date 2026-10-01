-- AirBreda relational schema (PostgreSQL). Applied idempotently by common.ensure_schema()
-- on every ingestion run, so a fresh database needs no manual setup.

-- Parsed hourly air-quality readings from Luchtmeetnet. One row per (station, hour, component).
-- The composite primary key is what makes ingestion idempotent: re-fetching the same hour
-- hits ON CONFLICT DO NOTHING instead of creating a duplicate (at-least-once + idempotent write).
CREATE TABLE IF NOT EXISTS sensor_readings (
    station_id  VARCHAR(20)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    component   VARCHAR(10)   NOT NULL,
    value       FLOAT,
    is_flagged  BOOLEAN       NOT NULL DEFAULT FALSE,   -- stale/null reading kept but flagged (Day 2)
    PRIMARY KEY (station_id, timestamp, component)
);
ALTER TABLE sensor_readings ADD COLUMN IF NOT EXISTS is_flagged BOOLEAN NOT NULL DEFAULT FALSE;

-- Parsed per-minute NDW traffic snapshots for the four A27 sites. Rows with speed=-1 never land
-- here (dropped and logged); the raw file in object storage keeps the sentinel for the audit trail.
CREATE TABLE IF NOT EXISTS traffic_readings (
    site_id               VARCHAR(5)    NOT NULL,   -- hrl | hrr | vwd | vwa
    ndw_site_id           VARCHAR(40)   NOT NULL,   -- RWS01_MONIBAS_...
    timestamp             TIMESTAMPTZ   NOT NULL,   -- measurement period from the feed, not fetch time
    intensity_veh_per_hr  INTEGER       NOT NULL,   -- sum of lane flows
    speed_kmh             FLOAT,                    -- flow-weighted mean of lane speeds
    lanes                 SMALLINT      NOT NULL,
    PRIMARY KEY (site_id, timestamp)
);
CREATE INDEX IF NOT EXISTS traffic_readings_ts_idx ON traffic_readings (timestamp DESC);

-- One row per ingestion source. The ingestion containers are short-lived cron jobs, so this table
-- is the only place their "last successful fetch" and bad-data counters can survive; the
-- dashboard's /health reads it.
CREATE TABLE IF NOT EXISTS ingestion_status (
    source                 VARCHAR(20)  PRIMARY KEY,  -- 'luchtmeetnet' | 'ndw'
    last_successful_fetch  TIMESTAMPTZ,
    last_attempt           TIMESTAMPTZ,
    last_error             TEXT,
    bad_data_count         INTEGER      NOT NULL DEFAULT 0
);

-- Every DATA_QUALITY_ERROR event, so "more than 10 in the last hour" is a query, not a guess.
CREATE TABLE IF NOT EXISTS bad_data_events (
    id          BIGSERIAL     PRIMARY KEY,
    source      VARCHAR(20)   NOT NULL,
    location    VARCHAR(40),
    field       VARCHAR(20),
    value       TEXT,
    reason      VARCHAR(40),
    detected_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS bad_data_events_src_ts_idx ON bad_data_events (source, detected_at DESC);
