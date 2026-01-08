const crypto = require('crypto');
const { getConfigDbPool } = require('./configDb');
const { encryptSecret, decryptSecret } = require('./envVaultCrypto');
const { forwardSelfLog } = require('./logger');

let tablesReady = false;

const memory = {
  envVarSets: [],
  envVars: [],
};

async function ensureEnvVaultTables(db) {
  if (!db || tablesReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS env_var_sets (
      id UUID PRIMARY KEY,
      project_id TEXT NOT NULL,
      name TEXT NOT NULL,
      is_default BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await db.query(`
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
  `);
  tablesReady = true;
}

async function getDb() {
  const db = await getConfigDbPool();
  if (db) {
    await ensureEnvVaultTables(db);
  }
  return db;
}

function findDefaultSetInMemory(projectId) {
  return memory.envVarSets.find((set) => set.projectId === projectId && set.isDefault);
}

async function ensureDefaultEnvVarSet(projectId, name = 'default') {
  const db = await getDb();
  if (!db) {
    let existing = findDefaultSetInMemory(projectId);
    if (!existing) {
      existing = {
        id: crypto.randomUUID(),
        projectId,
        name,
        isDefault: true,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      memory.envVarSets.push(existing);
    }
    return existing.id;
  }

  const { rows } = await db.query(
    'SELECT id FROM env_var_sets WHERE project_id = $1 AND is_default = true ORDER BY created_at ASC LIMIT 1',
    [projectId],
  );
  if (rows.length) {
    return rows[0].id;
  }

  const id = crypto.randomUUID();
  await db.query(
    `
      INSERT INTO env_var_sets (id, project_id, name, is_default)
      VALUES ($1, $2, $3, true)
    `,
    [id, projectId, name],
  );
  return id;
}

function normalizeEnvKey(key) {
  return String(key || '').trim();
}

async function listEnvVarKeys(projectId, envSetId) {
  const db = await getDb();
  if (!db) {
    const entries = memory.envVars.filter(
      (entry) => entry.projectId === projectId && entry.envSetId === envSetId,
    );
    return entries.map((entry) => entry.key).sort();
  }
  const { rows } = await db.query(
    'SELECT key FROM project_env_vars WHERE project_id = $1 AND env_set_id IS NOT DISTINCT FROM $2 ORDER BY key ASC',
    [projectId, envSetId || null],
  );
  return rows.map((row) => row.key);
}

async function listEnvVars(projectId, envSetId) {
  const db = await getDb();
  if (!db) {
    return memory.envVars.filter(
      (entry) => entry.projectId === projectId && entry.envSetId === envSetId,
    );
  }
  const { rows } = await db.query(
    'SELECT id, key, value_enc FROM project_env_vars WHERE project_id = $1 AND env_set_id IS NOT DISTINCT FROM $2 ORDER BY key ASC',
    [projectId, envSetId || null],
  );
  return rows.map((row) => ({
    id: row.id,
    projectId,
    envSetId: envSetId || null,
    key: row.key,
    valueEnc: row.value_enc,
  }));
}

async function getEnvVarRecord(projectId, key, envSetId) {
  const normalizedKey = normalizeEnvKey(key);
  const db = await getDb();
  if (!db) {
    return memory.envVars.find(
      (entry) =>
        entry.projectId === projectId && entry.envSetId === envSetId && entry.key === normalizedKey,
    );
  }
  const { rows } = await db.query(
    'SELECT id, key, value_enc FROM project_env_vars WHERE project_id = $1 AND env_set_id IS NOT DISTINCT FROM $2 AND key = $3 LIMIT 1',
    [projectId, envSetId || null, normalizedKey],
  );
  if (!rows.length) return null;
  return {
    id: rows[0].id,
    projectId,
    envSetId: envSetId || null,
    key: rows[0].key,
    valueEnc: rows[0].value_enc,
  };
}

async function upsertEnvVar(projectId, key, value, envSetId) {
  const normalizedKey = normalizeEnvKey(key);
  const encrypted = encryptSecret(value);
  const db = await getDb();
  if (!db) {
    const existingIndex = memory.envVars.findIndex(
      (entry) =>
        entry.projectId === projectId && entry.envSetId === envSetId && entry.key === normalizedKey,
    );
    const payload = {
      id: existingIndex === -1 ? crypto.randomUUID() : memory.envVars[existingIndex].id,
      projectId,
      envSetId: envSetId || null,
      key: normalizedKey,
      valueEnc: encrypted,
      updatedAt: new Date().toISOString(),
    };
    if (existingIndex === -1) {
      memory.envVars.push(payload);
    } else {
      memory.envVars[existingIndex] = { ...memory.envVars[existingIndex], ...payload };
    }
    return payload;
  }

  const id = crypto.randomUUID();
  try {
    await db.query(
      `
        INSERT INTO project_env_vars (id, project_id, env_set_id, key, value_enc)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (project_id, env_set_id, key)
        DO UPDATE SET value_enc = EXCLUDED.value_enc, updated_at = NOW()
      `,
      [id, projectId, envSetId || null, normalizedKey, encrypted],
    );
  } catch (error) {
    console.error('[envVaultStore] Failed to save env var', error);
    await forwardSelfLog('error', 'Failed to save env var', {
      stack: error?.stack,
      context: { error: error?.message, projectId, key: normalizedKey },
    });
    throw error;
  }

  return getEnvVarRecord(projectId, normalizedKey, envSetId);
}

async function deleteEnvVar(projectId, key, envSetId) {
  const normalizedKey = normalizeEnvKey(key);
  const db = await getDb();
  if (!db) {
    const before = memory.envVars.length;
    memory.envVars = memory.envVars.filter(
      (entry) =>
        !(entry.projectId === projectId && entry.envSetId === envSetId && entry.key === normalizedKey),
    );
    return memory.envVars.length !== before;
  }
  const result = await db.query(
    'DELETE FROM project_env_vars WHERE project_id = $1 AND env_set_id IS NOT DISTINCT FROM $2 AND key = $3',
    [projectId, envSetId || null, normalizedKey],
  );
  return result.rowCount > 0;
}

async function getEnvVarValue(projectId, key, envSetId) {
  const record = await getEnvVarRecord(projectId, key, envSetId);
  if (!record) return null;
  return decryptSecret(record.valueEnc);
}

module.exports = {
  ensureDefaultEnvVarSet,
  listEnvVarKeys,
  listEnvVars,
  getEnvVarRecord,
  getEnvVarValue,
  upsertEnvVar,
  deleteEnvVar,
};
