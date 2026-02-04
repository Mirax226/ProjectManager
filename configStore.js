// Persistent storage for projects & globalSettings in Postgres (JSONB)

const { getConfigDbPool } = require('./configDb');
const { classifyDbError, sanitizeDbErrorMessage } = require('./configDbErrors');
const { forwardSelfLog } = require('./logger');
const { appState, recordDbError } = require('./appState');

const DB_OPERATION_TIMEOUT_MS = 5000;
let tableReady = false;

// Simple in-memory cache as fallback
const memory = {
  projects: [],
  globalSettings: null,
};

async function getPool() {
  return getConfigDbPool();
}

async function withDbTimeout(promise, context) {
  if (!promise || typeof promise.then !== 'function') return promise;
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error('DB_TIMEOUT');
      error.code = 'DB_TIMEOUT';
      error.context = context;
      reject(error);
    }, DB_OPERATION_TIMEOUT_MS);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

async function ensureConfigTable(db) {
  if (!db || tableReady) return;
  await withDbTimeout(
    db.query(`
      CREATE TABLE IF NOT EXISTS path_config (
        key        TEXT PRIMARY KEY,
        data       JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `),
    'pool_init',
  );
  tableReady = true;
}

async function getDb() {
  if (!appState.dbReady || appState.degradedMode) {
    return null;
  }
  const db = await getPool();
  if (db) {
    await ensureConfigTable(db);
  }
  return db;
}

async function loadJson(key) {
  const db = await getDb();
  if (!db) {
    return memory[key] ?? null;
  }

  let res;
  try {
    res = await withDbTimeout(
      db.query('SELECT data FROM path_config WHERE key = $1', [key]),
      `load:${key}`,
    );
  } catch (err) {
    const category = classifyDbError(err);
    const message = sanitizeDbErrorMessage(err?.message);
    recordDbError(category, message);
    console.error(`[configStore] Failed to load ${key} from DB`, {
      category,
      message,
    });
    return memory[key] ?? null;
  }

  if (res.rowCount === 0) return null;

  try {
    const raw = res.rows[0].data;
    const value = typeof raw === 'string' ? JSON.parse(raw) : raw;
    memory[key] = value;
    return value;
  } catch (err) {
    console.error('[configStore] Failed to parse JSON for key', key, err);
    return null;
  }
}

async function saveJson(key, value) {
  memory[key] = value;

  if (!appState.dbReady || appState.degradedMode) return;

  const db = await getDb();
  if (!db) return;

  const jsonText = JSON.stringify(value);

  try {
    await withDbTimeout(
      db.query(
        `
        INSERT INTO path_config (key, data)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (key)
        DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
      `,
        [key, jsonText],
      ),
      `save:${key}`,
    );
  } catch (err) {
    const category = classifyDbError(err);
    const message = sanitizeDbErrorMessage(err?.message);
    recordDbError(category, message);
    console.error(`[configStore] Failed to save ${key} to DB`, {
      category,
      message,
    });
  }
}

// Public API

async function loadProjects() {
  const projects = await loadJson('projects');
  if (!projects || !Array.isArray(projects)) return [];
  return projects;
}

async function saveProjects(projects) {
  if (!Array.isArray(projects)) throw new Error('projects must be an array');
  try {
    await saveJson('projects', projects);
  } catch (err) {
    const message = sanitizeDbErrorMessage(err?.message);
    console.error('[configStore] Failed to save projects', {
      message,
    });
    await forwardSelfLog('error', 'Failed to save projects', {
      stack: err?.stack,
      context: { error: message },
    });
  }
}

async function loadGlobalSettings() {
  const settings = await loadJson('globalSettings');
  if (!settings || typeof settings !== 'object') return null;
  return settings;
}

async function saveGlobalSettings(settings) {
  try {
    await saveJson('globalSettings', settings || {});
  } catch (err) {
    const message = sanitizeDbErrorMessage(err?.message);
    console.error('[configStore] Failed to save globalSettings', {
      message,
    });
    await forwardSelfLog('error', 'Failed to save global settings', {
      stack: err?.stack,
      context: { error: message },
    });
  }
}

module.exports = {
  classifyDbError,
  ensureConfigTable,
  loadJson,
  saveJson,
  loadProjects,
  saveProjects,
  loadGlobalSettings,
  saveGlobalSettings,
};
