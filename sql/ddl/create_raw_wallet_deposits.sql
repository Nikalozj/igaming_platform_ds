CREATE TABLE IF NOT EXISTS raw_wallet_deposits (
    deposit_id TEXT PRIMARY KEY,
    user_id INT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    provider_name TEXT,
    provider_txn_id TEXT,
    deposit_status TEXT NOT NULL,
    country_code TEXT,
    device_type TEXT,
    event_time TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);