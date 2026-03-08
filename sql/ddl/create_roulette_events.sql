CREATE TABLE IF NOT EXISTS roulette_events (
    event_id TEXT PRIMARY KEY,
    event_name TEXT NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,

    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,

    table_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    bet_id TEXT NOT NULL,

    bet_type TEXT NOT NULL,
    selection INT NULL,
    stake NUMERIC(12, 2) NOT NULL,

    currency TEXT NOT NULL,
    country TEXT NOT NULL,
    device TEXT NOT NULL,

    kafka_partition INT,
    kafka_offset BIGINT,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);