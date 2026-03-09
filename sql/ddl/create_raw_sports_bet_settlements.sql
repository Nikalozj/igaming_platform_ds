CREATE TABLE IF NOT EXISTS raw_sports_bet_settlements (
    settlement_id TEXT PRIMARY KEY,

    bet_id TEXT NOT NULL,

    result_status TEXT NOT NULL,       -- won, lost, void, cancelled, cashout
    win_amount NUMERIC(12,2) DEFAULT 0,

    settled_time TIMESTAMP NOT NULL,

    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);