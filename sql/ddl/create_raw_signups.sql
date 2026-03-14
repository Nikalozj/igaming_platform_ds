CREATE TABLE IF NOT EXISTS raw_user_signups (
    signup_id TEXT PRIMARY KEY,
    user_id INT NOT NULL,
    username TEXT NOT NULL,
    email TEXT,
    registration_method TEXT NOT NULL,
    country_code TEXT,
    device_type TEXT,
    is_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);