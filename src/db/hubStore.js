const crypto = require('crypto');
const { getConfigDbPool } = require('../../configDb');
const { encryptSecret, decryptSecret, getKmsKey } = require('./crypto');
const { normalizePostgresDsn, validatePostgresDsn, fingerprintPostgresDsn, maskPostgresDsn } = require('./dsn');

async function ensureDbHubSchema() {
  const db = await getConfigDbPool();
  if (!db) return false;
  await db.query(`
    CREATE TABLE IF NOT EXISTS pm_project_db_connections (
      id uuid PRIMARY KEY,
      project_id text NOT NULL,
      label text NOT NULL,
      role text NOT NULL,
      dsn_encrypted text NOT NULL,
      dsn_fingerprint text NOT NULL,
      ssl_mode text,
      enabled boolean NOT NULL DEFAULT true,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS pm_project_db_connections_project_fingerprint_uidx
      ON pm_project_db_connections(project_id, dsn_fingerprint);
    CREATE UNIQUE INDEX IF NOT EXISTS pm_project_db_connections_primary_secondary_uidx
      ON pm_project_db_connections(project_id, role)
      WHERE role IN ('primary', 'secondary');

    CREATE TABLE IF NOT EXISTS pm_db_sync_state (
      project_id text PRIMARY KEY,
      dual_mode_enabled boolean NOT NULL DEFAULT false,
      preferred_read text NOT NULL DEFAULT 'auto',
      last_sync_at timestamptz,
      last_sync_status text,
      last_error text
    );

    CREATE TABLE IF NOT EXISTS pm_db_migration_jobs (
      id uuid PRIMARY KEY,
      project_id text NOT NULL,
      source_conn_id uuid NOT NULL,
      target_conn_id uuid NOT NULL,
      mode text NOT NULL,
      selected_tables jsonb,
      status text NOT NULL,
      progress int NOT NULL DEFAULT 0,
      current_step text,
      log text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS pm_db_events (
      id bigserial PRIMARY KEY,
      project_id text NOT NULL,
      event_type text NOT NULL,
      payload jsonb,
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS pm_db_write_queue (
      id bigserial PRIMARY KEY,
      project_id text NOT NULL,
      target_role text NOT NULL,
      sql_text text NOT NULL,
      params jsonb,
      status text NOT NULL DEFAULT 'queued',
      attempts int NOT NULL DEFAULT 0,
      last_error text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );
  `);
  return true;
}

async function upsertProjectConnection({ projectId, label, role, dsn, sslMode, enabled = true }) {
  if (!getKmsKey()) {
    return { ok: false, code: 'PM_KMS_KEY_MISSING', error: 'PM_KMS_KEY missing. Cannot store DSN.' };
  }
  const valid = validatePostgresDsn(dsn);
  if (!valid.ok) return { ok: false, code: 'INVALID_DSN', error: valid.errors.join('; ') };
  await ensureDbHubSchema();
  const db = await getConfigDbPool();
  const normalized = normalizePostgresDsn(dsn).dsn;
  const encrypted = encryptSecret(normalized);
  const fingerprint = fingerprintPostgresDsn(normalized);
  const id = crypto.randomUUID();
  const query = `
    INSERT INTO pm_project_db_connections (id, project_id, label, role, dsn_encrypted, dsn_fingerprint, ssl_mode, enabled)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (project_id, role) WHERE role IN ('primary', 'secondary')
    DO UPDATE SET label=EXCLUDED.label, dsn_encrypted=EXCLUDED.dsn_encrypted,
      dsn_fingerprint=EXCLUDED.dsn_fingerprint, ssl_mode=EXCLUDED.ssl_mode, enabled=EXCLUDED.enabled,
      updated_at=now()
    RETURNING id, project_id, label, role, ssl_mode, enabled, created_at, updated_at;
  `;
  const result = await db.query(query, [id, projectId, label, role, encrypted, fingerprint, sslMode || null, Boolean(enabled)]);
  return { ok: true, connection: result.rows[0], warnings: valid.warnings };
}

async function listProjectConnections(projectId, { includeDsn = false } = {}) {
  await ensureDbHubSchema();
  const db = await getConfigDbPool();
  const result = await db.query(
    `SELECT id, project_id, label, role, dsn_encrypted, ssl_mode, enabled, created_at, updated_at
     FROM pm_project_db_connections WHERE project_id=$1 ORDER BY role='primary' DESC, role='secondary' DESC, created_at ASC`,
    [projectId],
  );
  return result.rows.map((row) => ({
    id: row.id,
    projectId: row.project_id,
    label: row.label,
    role: row.role,
    sslMode: row.ssl_mode,
    enabled: row.enabled,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    dsnMasked: (() => {
      try { return maskPostgresDsn(decryptSecret(row.dsn_encrypted)); } catch (_error) { return 'postgresql://***'; }
    })(),
    dsn: includeDsn ? decryptSecret(row.dsn_encrypted) : undefined,
  }));
}

module.exports = {
  ensureDbHubSchema,
  upsertProjectConnection,
  listProjectConnections,
};
