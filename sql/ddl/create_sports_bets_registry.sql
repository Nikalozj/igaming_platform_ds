CREATE TABLE IF NOT EXISTS sports_bets_registry (
    bet_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',   -- open, processing, settled
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    picked_at TIMESTAMP,
    settled_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sports_bets_registry_status_created
    ON sports_bets_registry (status, created_at);