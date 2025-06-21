CREATE TABLE IF NOT EXISTS iot_metrics (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    city TEXT,
    avg_temperature DOUBLE PRECISION,
    avg_humidity    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS iot_batch_metrics (
    city TEXT,
    avg_temperature DOUBLE PRECISION,
    avg_humidity    DOUBLE PRECISION
);