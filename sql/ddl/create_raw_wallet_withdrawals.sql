CREATE TABLE IF NOT EXISTS raw_wallet_withdrawals (
    withdrawal_id TEXT PRIMARY KEY,
    user_id INT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,
    payout_method TEXT NOT NULL,
    provider_name TEXT,
    provider_txn_id TEXT,
    withdrawal_status TEXT NOT NULL,
    country_code TEXT,
    device_type TEXT,
    event_time TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);