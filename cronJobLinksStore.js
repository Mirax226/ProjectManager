const { getConfigDbPool } = require('./configDb');
const { forwardSelfLog } = require('./logger');

let tablesReady = false;
const memory = {
  links: [],
};

async function ensureCronJobLinksTable(db) {
  if (!db || tablesReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS cron_job_links (
      cron_job_id TEXT PRIMARY KEY,
      project_id TEXT NULL,
      label TEXT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  tablesReady = true;
}

async function getDb() {
  const db = await getConfigDbPool();
  if (db) {
    await ensureCronJobLinksTable(db);
  }
  return db;
}

async function listCronJobLinks() {
  const db = await getDb();
  if (!db) {
    return [...memory.links];
  }
  const { rows } = await db.query(
    'SELECT cron_job_id, project_id, label FROM cron_job_links ORDER BY updated_at DESC',
  );
  return rows.map((row) => ({
    cronJobId: row.cron_job_id,
    projectId: row.project_id,
    label: row.label,
  }));
}

async function getCronJobLink(cronJobId) {
  const db = await getDb();
  if (!db) {
    return memory.links.find((link) => link.cronJobId === cronJobId) || null;
  }
  const { rows } = await db.query(
    'SELECT cron_job_id, project_id, label FROM cron_job_links WHERE cron_job_id = $1 LIMIT 1',
    [String(cronJobId)],
  );
  if (!rows.length) return null;
  return {
    cronJobId: rows[0].cron_job_id,
    projectId: rows[0].project_id,
    label: rows[0].label,
  };
}

async function upsertCronJobLink(cronJobId, projectId, label) {
  const db = await getDb();
  if (!db) {
    const existingIndex = memory.links.findIndex((link) => link.cronJobId === cronJobId);
    const entry = {
      cronJobId: String(cronJobId),
      projectId: projectId || null,
      label: label || null,
    };
    if (existingIndex === -1) {
      memory.links.push(entry);
    } else {
      memory.links[existingIndex] = entry;
    }
    return entry;
  }
  try {
    await db.query(
      `
        INSERT INTO cron_job_links (cron_job_id, project_id, label)
        VALUES ($1, $2, $3)
        ON CONFLICT (cron_job_id)
        DO UPDATE SET project_id = EXCLUDED.project_id, label = EXCLUDED.label, updated_at = NOW()
      `,
      [String(cronJobId), projectId || null, label || null],
    );
  } catch (error) {
    console.error('[cronJobLinksStore] Failed to save cron job link', error);
    await forwardSelfLog('error', 'Failed to save cron job link', {
      stack: error?.stack,
      context: { error: error?.message, cronJobId },
    });
    throw error;
  }
  return getCronJobLink(cronJobId);
}

async function deleteCronJobLink(cronJobId) {
  const db = await getDb();
  if (!db) {
    const before = memory.links.length;
    memory.links = memory.links.filter((link) => link.cronJobId !== cronJobId);
    return memory.links.length !== before;
  }
  const result = await db.query('DELETE FROM cron_job_links WHERE cron_job_id = $1', [String(cronJobId)]);
  return result.rowCount > 0;
}

module.exports = {
  listCronJobLinks,
  getCronJobLink,
  upsertCronJobLink,
  deleteCronJobLink,
};
