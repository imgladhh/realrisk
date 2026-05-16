CREATE TABLE IF NOT EXISTS alert_log (
    alert_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    reason_summary TEXT NOT NULL,
    channels_notified TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alert_log_user_created
    ON alert_log (user_id, created_at);
