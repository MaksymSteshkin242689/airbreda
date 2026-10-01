-- Day 1: parsed hourly air-quality readings from Luchtmeetnet (station NL10240).
-- The composite primary key is what makes ingestion idempotent: re-fetching the same hour hits
-- ON CONFLICT DO NOTHING instead of creating a duplicate (at-least-once delivery + idempotent write).
CREATE TABLE sensor_readings (
    station_id  VARCHAR(20)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,   -- end of the measurement hour, as published by the API
    component   VARCHAR(10)   NOT NULL,   -- 'NO2'
    value       FLOAT,                    -- NULL when the station reported no value
    PRIMARY KEY (station_id, timestamp, component)
);
