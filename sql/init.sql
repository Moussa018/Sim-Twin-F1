CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw telemetry samples
CREATE TABLE IF NOT EXISTS telemetry (
    time      TIMESTAMPTZ NOT NULL,
    driver    TEXT        NOT NULL,
    speed     DOUBLE PRECISION,
    throttle  DOUBLE PRECISION,
    brake     DOUBLE PRECISION,
    gear      INT,
    rpm       DOUBLE PRECISION,
    tyre_temp DOUBLE PRECISION,
    tyre_wear DOUBLE PRECISION
);
SELECT create_hypertable('telemetry', 'time', if_not_exists => TRUE);

-- Windowed KPIs produced by Kafka Streams
CREATE TABLE IF NOT EXISTS kpi_window (
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    driver       TEXT        NOT NULL,
    avg_speed    DOUBLE PRECISION,
    max_speed    DOUBLE PRECISION,
    avg_throttle DOUBLE PRECISION,
    brake_pct    DOUBLE PRECISION,
    avg_tyre_temp DOUBLE PRECISION
);
SELECT create_hypertable('kpi_window', 'window_start', if_not_exists => TRUE);

-- Model predictions
CREATE TABLE IF NOT EXISTS prediction (
    time      TIMESTAMPTZ NOT NULL,
    driver    TEXT        NOT NULL,
    metric    TEXT        NOT NULL,
    value     DOUBLE PRECISION
);
SELECT create_hypertable('prediction', 'time', if_not_exists => TRUE);
