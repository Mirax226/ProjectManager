-- Operational delivery identifiers only; no raw updates or business data.
CREATE TABLE IF NOT EXISTS cp_telegram_updates (
  update_id INTEGER PRIMARY KEY, status TEXT NOT NULL,
  lease_until INTEGER NOT NULL DEFAULT 0, owner TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_telegram_updates_created ON cp_telegram_updates(created_at);

ALTER TABLE cp_jobs ADD COLUMN result_runner_id TEXT;
