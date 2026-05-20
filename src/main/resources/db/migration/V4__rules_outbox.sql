CREATE TABLE IF NOT EXISTS rules (
  rule_id text PRIMARY KEY,
  rule_type text NOT NULL,
  parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
  enabled boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rule_outbox (
  id bigserial PRIMARY KEY,
  rule_id text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_rule_outbox_unpublished
  ON rule_outbox (published_at, id);
