CREATE TABLE IF NOT EXISTS raw_sports_bets (
    bet_id TEXT PRIMARY KEY,

    user_id INT NOT NULL,

    ticket_id TEXT,                -- multiple bets can belong to one ticket
    event_id INT NOT NULL,         -- match id
    market_id INT NOT NULL,        -- market (winner, over/under, etc.)
    selection_id INT NOT NULL,     -- chosen outcome

    odds NUMERIC(8,3) NOT NULL,

    stake_amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,

    bet_status TEXT NOT NULL,

    is_live BOOLEAN,

    device_type TEXT,
    country_code TEXT,

    event_time TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);