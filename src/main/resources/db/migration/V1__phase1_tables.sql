CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    amount_cents BIGINT NOT NULL,
    currency TEXT NOT NULL,
    ip_address TEXT,
    device_fp TEXT,
    merchant_id TEXT,
    counterparty TEXT,
    source TEXT NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_user_time
    ON raw_events (user_id, occurred_at);

CREATE TABLE IF NOT EXISTS risk_decisions (
    decision_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    risk_score INTEGER NOT NULL,
    reasons TEXT[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_risk_decisions_user_created
    ON risk_decisions (user_id, created_at);

CREATE TABLE IF NOT EXISTS audit_bans (
    id BIGSERIAL PRIMARY KEY,
    decision_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    severity INTEGER NOT NULL,
    expires_at TIMESTAMPTZ,
    cleared_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_bans_expiring
    ON audit_bans (expires_at)
    WHERE cleared_at IS NULL;
