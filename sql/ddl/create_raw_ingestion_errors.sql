CREATE TABLE raw_ingestion_errors (
    id BIGSERIAL PRIMARY KEY,
    consumer_name TEXT,
    topic TEXT,
    kafka_partition INT,
    kafka_offset BIGINT,
    error_message TEXT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);