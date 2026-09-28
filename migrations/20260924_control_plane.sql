-- Control-plane-only state. Existing PostgreSQL/config DB tables remain owned by the legacy Node runtime.
CREATE TABLE IF NOT EXISTS cp_projects (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, environment TEXT NOT NULL,
  runner_profile TEXT, capabilities_json TEXT NOT NULL DEFAULT '[]', updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_events (
  event_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL DEFAULT 1, project TEXT NOT NULL, environment TEXT NOT NULL,
  severity TEXT NOT NULL, category TEXT NOT NULL, component TEXT NOT NULL DEFAULT '', timestamp TEXT NOT NULL,
  message TEXT NOT NULL, context_json TEXT NOT NULL DEFAULT '{}', source TEXT NOT NULL,
  correlation_id TEXT, received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_events_project_time ON cp_events(project, timestamp DESC);
CREATE TABLE IF NOT EXISTS cp_jobs (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL, type TEXT NOT NULL, risk TEXT NOT NULL,
  status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  requested_by TEXT NOT NULL, payload_json TEXT NOT NULL DEFAULT '{}', attempt_count INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT, lease_expires_at TEXT, result_json TEXT, error TEXT, idempotency_key TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS cp_jobs_idempotency ON cp_jobs(project_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS cp_jobs_claim ON cp_jobs(status, created_at);
CREATE TABLE IF NOT EXISTS cp_runners (
  runner_id TEXT PRIMARY KEY, project_id TEXT, status TEXT NOT NULL,
  last_seen_at TEXT NOT NULL, version TEXT, host_label TEXT, capabilities_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS cp_alerts (
  id TEXT PRIMARY KEY, alert_key TEXT UNIQUE NOT NULL, project TEXT NOT NULL,
  category TEXT NOT NULL, environment TEXT NOT NULL, message TEXT NOT NULL,
  event_id TEXT NOT NULL, created_at TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0
);
