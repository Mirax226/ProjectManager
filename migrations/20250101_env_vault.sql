-- Env Vault + Cron job links + Telegram bot setup

CREATE TABLE IF NOT EXISTS env_var_sets (
  id UUID PRIMARY KEY,
  project_id TEXT NOT NULL,
  name TEXT NOT NULL,
  is_default BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS project_env_vars (
  id UUID PRIMARY KEY,
  project_id TEXT NOT NULL,
  env_set_id UUID NULL,
  key TEXT NOT NULL,
  value_enc TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(project_id, env_set_id, key)
);

CREATE TABLE IF NOT EXISTS cron_job_links (
  cron_job_id TEXT PRIMARY KEY,
  project_id TEXT NULL,
  label TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS project_telegram_bots (
  project_id TEXT PRIMARY KEY,
  bot_token_enc TEXT NULL,
  webhook_url TEXT NULL,
  webhook_path TEXT NULL,
  last_set_at TIMESTAMPTZ NULL,
  last_test_at TIMESTAMPTZ NULL,
  last_test_status TEXT NULL,
  enabled BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Note: project_type and default_env_set_id are stored in the projects JSON payload
-- inside path_config (no dedicated projects table).
