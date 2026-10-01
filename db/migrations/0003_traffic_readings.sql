-- depends: 0002_data_quality
-- Day 3: parsed per-minute NDW traffic snapshots for the four A27 sites, for serving and for
-- building the training set. Rows carrying the speed=-1 sentinel never land here (dropped and
-- logged); the raw CSV in object storage keeps them for the audit trail.
CREATE TABLE traffic_readings (
    site_id               VARCHAR(5)    NOT NULL,   -- hrl | hrr | vwd | vwa
    ndw_site_id           VARCHAR(40)   NOT NULL,   -- RWS01_MONIBAS_...
    timestamp             TIMESTAMPTZ   NOT NULL,   -- measurement minute from the feed, not fetch time
    intensity_veh_per_hr  INTEGER       NOT NULL,   -- sum of lane flows
    speed_kmh             FLOAT,                    -- flow-weighted mean of lane speeds
    lanes                 SMALLINT      NOT NULL,
    PRIMARY KEY (site_id, timestamp)
);
CREATE INDEX traffic_readings_ts_idx ON traffic_readings (timestamp DESC);
