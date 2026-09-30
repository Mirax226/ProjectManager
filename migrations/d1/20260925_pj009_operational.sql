-- PJ-009 durable operational state. Additive and rerun-safe.
CREATE TABLE IF NOT EXISTS cp_operational_config (
  id TEXT PRIMARY KEY, config_json TEXT NOT NULL, updated_at TEXT NOT NULL,
  updated_by TEXT NOT NULL, operation_key TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS cp_config_audit (
  audit_id TEXT PRIMARY KEY, operation_key TEXT UNIQUE, project_id TEXT,
  actor TEXT NOT NULL, changed_at TEXT NOT NULL, setting_names_json TEXT NOT NULL,
  context_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS cp_project_config (
  project_id TEXT PRIMARY KEY, project_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_desktop_profiles (
  profile_id TEXT PRIMARY KEY, project_id TEXT, profile_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_excel_assets (
  asset_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, asset_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS cp_excel_assets_project_asset ON cp_excel_assets(project_id, asset_id);
CREATE TABLE IF NOT EXISTS cp_provider_ops (
  provider_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, provider_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS cp_provider_ops_project_provider ON cp_provider_ops(project_id, provider_id);
CREATE TABLE IF NOT EXISTS cp_archives (
  archive_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, operation_key TEXT UNIQUE,
  archive_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_archives_project_time ON cp_archives(project_id, updated_at DESC);
CREATE TABLE IF NOT EXISTS cp_backups (
  backup_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, operation_key TEXT UNIQUE,
  backup_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_backups_project_time ON cp_backups(project_id, updated_at DESC);
CREATE TABLE IF NOT EXISTS cp_retention_candidates (
  candidate_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, candidate_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_retention_candidates_project ON cp_retention_candidates(project_id, updated_at DESC);
CREATE TABLE IF NOT EXISTS cp_reconciliation_results (
  work_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, result_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
