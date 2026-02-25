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
      provider_job_id TEXT NULL,
      job_key TEXT NULL UNIQUE,
      project_id TEXT NULL,
      label TEXT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      schedule_normalized TEXT NULL,
      target_normalized TEXT NULL,
      fingerprint TEXT NULL,
      provider TEXT NULL,
      display_name TEXT NULL,
      last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS provider_job_id TEXT NULL');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS job_key TEXT NULL');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS schedule_normalized TEXT NULL');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS target_normalized TEXT NULL');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS fingerprint TEXT NULL');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS provider TEXT NULL');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS display_name TEXT NULL');
  await db.query('ALTER TABLE cron_job_links ADD COLUMN IF NOT EXISTS last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()');
  await db.query('CREATE UNIQUE INDEX IF NOT EXISTS cron_job_links_job_key_uidx ON cron_job_links (job_key) WHERE job_key IS NOT NULL');
  await db.query('CREATE INDEX IF NOT EXISTS cron_job_links_fingerprint_idx ON cron_job_links (fingerprint) WHERE fingerprint IS NOT NULL');
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
    `SELECT cron_job_id, provider_job_id, job_key, project_id, label, enabled,
            schedule_normalized, target_normalized, fingerprint, provider, display_name, last_updated_at
       FROM cron_job_links
      ORDER BY updated_at DESC`,
  );
  return rows.map((row) => ({
    cronJobId: row.cron_job_id,
    providerJobId: row.provider_job_id || row.cron_job_id,
    jobKey: row.job_key,
    projectId: row.project_id,
    label: row.label,
    enabled: row.enabled !== false,
    scheduleNormalized: row.schedule_normalized,
    targetNormalized: row.target_normalized,
    fingerprint: row.fingerprint || null,
    provider: row.provider || 'cronjob_org',
    displayName: row.display_name || row.label || null,
    lastUpdatedAt: row.last_updated_at ? new Date(row.last_updated_at).toISOString() : null,
  }));
}

async function getCronJobLink(cronJobId) {
  const db = await getDb();
  if (!db) {
    return memory.links.find((link) => link.cronJobId === cronJobId) || null;
  }
  const { rows } = await db.query(
    `SELECT cron_job_id, provider_job_id, job_key, project_id, label, enabled,
            schedule_normalized, target_normalized, fingerprint, provider, display_name, last_updated_at
       FROM cron_job_links
      WHERE cron_job_id = $1
      LIMIT 1`,
    [String(cronJobId)],
  );
  if (!rows.length) return null;
  return {
    cronJobId: rows[0].cron_job_id,
    providerJobId: rows[0].provider_job_id || rows[0].cron_job_id,
    jobKey: rows[0].job_key,
    projectId: rows[0].project_id,
    label: rows[0].label,
    enabled: rows[0].enabled !== false,
    scheduleNormalized: rows[0].schedule_normalized,
    targetNormalized: rows[0].target_normalized,
    fingerprint: rows[0].fingerprint || null,
    provider: rows[0].provider || 'cronjob_org',
    displayName: rows[0].display_name || rows[0].label || null,
    lastUpdatedAt: rows[0].last_updated_at ? new Date(rows[0].last_updated_at).toISOString() : null,
  };
}

async function getCronJobLinkByJobKey(jobKey) {
  if (!jobKey) return null;
  const db = await getDb();
  if (!db) {
    return memory.links.find((link) => link.jobKey === jobKey) || null;
  }
  const { rows } = await db.query(
    `SELECT cron_job_id, provider_job_id, job_key, project_id, label, enabled,
            schedule_normalized, target_normalized, fingerprint, provider, display_name, last_updated_at
       FROM cron_job_links
      WHERE job_key = $1
      LIMIT 1`,
    [String(jobKey)],
  );
  if (!rows.length) return null;
  return {
    cronJobId: rows[0].cron_job_id,
    providerJobId: rows[0].provider_job_id || rows[0].cron_job_id,
    jobKey: rows[0].job_key,
    projectId: rows[0].project_id,
    label: rows[0].label,
    enabled: rows[0].enabled !== false,
    scheduleNormalized: rows[0].schedule_normalized,
    targetNormalized: rows[0].target_normalized,
    fingerprint: rows[0].fingerprint || null,
    provider: rows[0].provider || 'cronjob_org',
    displayName: rows[0].display_name || rows[0].label || null,
    lastUpdatedAt: rows[0].last_updated_at ? new Date(rows[0].last_updated_at).toISOString() : null,
  };
}

async function upsertCronJobLink(cronJobId, projectId, label, metadata = {}) {
  const db = await getDb();
  const nowIso = new Date().toISOString();
  const entry = {
    cronJobId: String(cronJobId),
    providerJobId: metadata.providerJobId ? String(metadata.providerJobId) : String(cronJobId),
    jobKey: metadata.jobKey || null,
    projectId: projectId || null,
    label: label || null,
    enabled: metadata.enabled !== false,
    scheduleNormalized: metadata.scheduleNormalized || null,
    targetNormalized: metadata.targetNormalized || null,
    fingerprint: metadata.fingerprint || null,
    provider: metadata.provider || 'cronjob_org',
    displayName: metadata.displayName || label || null,
    lastUpdatedAt: metadata.lastUpdatedAt || nowIso,
  };
  if (!db) {
    const existingIndex = memory.links.findIndex((link) => link.cronJobId === cronJobId);
    if (existingIndex === -1) {
      const byJobKey = entry.jobKey ? memory.links.findIndex((link) => link.jobKey === entry.jobKey) : -1;
      if (byJobKey >= 0) {
        memory.links[byJobKey] = entry;
      } else {
        memory.links.push(entry);
      }
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
    if (entry.jobKey) {
      await db.query(
        `UPDATE cron_job_links
            SET cron_job_id = $1,
                provider_job_id = $2,
                project_id = $3,
                label = $4,
                job_key = $5,
                enabled = $6,
                schedule_normalized = $7,
                target_normalized = $8,
                fingerprint = $9,
                provider = $10,
                display_name = $11,
                last_updated_at = $12,
                updated_at = NOW()
          WHERE job_key = $5`,
        [entry.cronJobId, entry.providerJobId, entry.projectId, entry.label, entry.jobKey, entry.enabled, entry.scheduleNormalized, entry.targetNormalized, entry.fingerprint, entry.provider, entry.displayName, entry.lastUpdatedAt],
      );
    }
    await db.query(
      `UPDATE cron_job_links
          SET provider_job_id = $2,
              job_key = COALESCE($3, job_key),
              enabled = $4,
              schedule_normalized = COALESCE($5, schedule_normalized),
              target_normalized = COALESCE($6, target_normalized),
              fingerprint = COALESCE($7, fingerprint),
              provider = COALESCE($8, provider),
              display_name = COALESCE($9, display_name),
              last_updated_at = $10,
              updated_at = NOW()
        WHERE cron_job_id = $1`,
      [entry.cronJobId, entry.providerJobId, entry.jobKey, entry.enabled, entry.scheduleNormalized, entry.targetNormalized, entry.fingerprint, entry.provider, entry.displayName, entry.lastUpdatedAt],
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

async function renameCronJobLinkProjectId(oldProjectId, newProjectId) {
  const db = await getDb();
  if (!db) {
    memory.links = memory.links.map((link) =>
      link.projectId === oldProjectId ? { ...link, projectId: newProjectId } : link,
    );
    return true;
  }
  try {
    await db.query('UPDATE cron_job_links SET project_id = $1 WHERE project_id = $2', [
      newProjectId,
      oldProjectId,
    ]);
    return true;
  } catch (error) {
    console.error('[cronJobLinksStore] Failed to rename cron job link projectId', error);
    await forwardSelfLog('error', 'Failed to rename cron job link projectId', {
      stack: error?.stack,
      context: { error: error?.message, oldProjectId, newProjectId },
    });
    throw error;
  }
}


async function listCronJobLinksByFingerprint(fingerprint) {
  if (!fingerprint) return [];
  const links = await listCronJobLinks();
  return links.filter((item) => item.fingerprint === fingerprint);
}

module.exports = {
  listCronJobLinks,
  getCronJobLink,
  getCronJobLinkByJobKey,
  listCronJobLinksByFingerprint,
  upsertCronJobLink,
  deleteCronJobLink,
  renameCronJobLinkProjectId,
};
