CREATE TABLE IF NOT EXISTS project_log_settings (
  project_id TEXT PRIMARY KEY,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  levels TEXT[] NOT NULL DEFAULT ARRAY['error']::TEXT[],
  destination_chat_id TEXT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS project_recent_logs (
  id UUID PRIMARY KEY,
  project_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  level TEXT NOT NULL,
  service TEXT NOT NULL,
  env TEXT NOT NULL,
  timestamp TIMESTAMPTZ NULL,
  message TEXT NOT NULL,
  stack TEXT NULL,
  context JSONB NULL
);

CREATE INDEX IF NOT EXISTS project_recent_logs_project_id_idx
  ON project_recent_logs (project_id, created_at DESC);
