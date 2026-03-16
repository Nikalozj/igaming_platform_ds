CREATE TABLE IF NOT EXISTS dag_test_table (
    bet_id TEXT PRIMARY KEY,

    user_id INT NOT NULL,
    game_id INT NOT NULL,
    provider_id INT NOT NULL,

    table_id TEXT,
    round_id TEXT,

    bet_type TEXT,              -- player, banker, red, black, etc
    stake_amount NUMERIC(12,2) NOT NULL,
    win_amount NUMERIC(12,2) NOT NULL,

    currency TEXT NOT NULL,
    bet_status TEXT NOT NULL,

    device_type TEXT,
    country_code TEXT,

    dealer_id TEXT,

    event_time TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);