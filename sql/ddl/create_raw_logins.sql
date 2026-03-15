CREATE TABLE IF NOT EXISTS raw_user_logins (
    login_id TEXT PRIMARY KEY,

    user_id INT NOT NULL,

    session_id TEXT,
    login_status TEXT NOT NULL,          -- success, failed
    failure_reason TEXT,                 -- invalid_password, blocked_user, otp_failed, etc.

    login_method TEXT,                   -- password, google, apple, facebook, etc.
    auth_type TEXT,                      -- password_only, 2fa, biometric

    ip_address INET,
    user_agent TEXT,

    device_type TEXT,                    -- mobile, desktop, tablet
    os_name TEXT,                        -- windows, ios, android, macos, linux
    browser_name TEXT,                   -- chrome, safari, firefox, edge

    country_code TEXT,
    city TEXT,

    event_time TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);