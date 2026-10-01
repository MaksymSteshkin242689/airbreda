-- depends: 0003_traffic_readings
-- Count each bad reading once: a feed that stops updating is re-read every five minutes, and
-- without the measurement timestamp on the event the same speed=-1 would be counted every run.
ALTER TABLE bad_data_events ADD COLUMN reading_ts TIMESTAMPTZ;
CREATE INDEX bad_data_events_reading_idx ON bad_data_events (source, location, field, reading_ts);
