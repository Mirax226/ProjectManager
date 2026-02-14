const crypto = require('crypto');
const { getConfigDbPool } = require('../../configDb');
const { ensureDbHubSchema } = require('./hubStore');

async function createMigrationJob(payload) {
  await ensureDbHubSchema();
  const db = await getConfigDbPool();
  const id = crypto.randomUUID();
  await db.query(
    `INSERT INTO pm_db_migration_jobs(id, project_id, source_conn_id, target_conn_id, mode, selected_tables, status, progress, current_step, log)
     VALUES($1,$2,$3,$4,$5,$6,'queued',0,'queued','')`,
    [id, payload.projectId, payload.sourceConnId, payload.targetConnId, payload.mode || 'schema_and_data', payload.selectedTables ? JSON.stringify(payload.selectedTables) : null],
  );
  return { id };
}

async function updateMigrationJob(jobId, patch) {
  await ensureDbHubSchema();
  const db = await getConfigDbPool();
  const fields = [];
  const values = [];
  const add = (key, value) => { fields.push(`${key}=$${values.length + 1}`); values.push(value); };
  if (patch.status) add('status', patch.status);
  if (Number.isFinite(patch.progress)) add('progress', patch.progress);
  if (patch.currentStep) add('current_step', patch.currentStep);
  if (!fields.length) return;
  values.push(jobId);
  const query = `UPDATE pm_db_migration_jobs SET ${fields.join(', ')}, updated_at=now() WHERE id=$${values.length}`;
  await db.query(query, values);
}

async function getMigrationJob(jobId) {
  await ensureDbHubSchema();
  const db = await getConfigDbPool();
  const res = await db.query('SELECT * FROM pm_db_migration_jobs WHERE id=$1', [jobId]);
  return res.rows[0] || null;
}

module.exports = { createMigrationJob, updateMigrationJob, getMigrationJob };
