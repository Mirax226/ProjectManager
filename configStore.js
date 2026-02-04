// Persistent storage for projects & globalSettings in Postgres (JSONB)

const { Pool } = require('pg');
const { forwardSelfLog } = require('./logger');
const { recordDbError, setDbReady } = require('./appState');

let pool = null;
let sslWarningEmitted = false;

const DB_POOL_MAX = 5;
const DB_IDLE_TIMEOUT_MS = 30_000;
const DB_CONNECTION_TIMEOUT_MS = 10_000;
const DB_STATEMENT_TIMEOUT_MS = 10_000;
const DB_CONNECT_RETRIES = [500, 1000, 2000, 4000, 8000];

// Simple in-memory cache as fallback
const memory = {
  projects: [],
  globalSettings: null,
};

async function getPool() {
  const dsn = process.env.PATH_APPLIER_CONFIG_DSN;
  if (!dsn) {
    console.warn(
      '[configStore] PATH_APPLIER_CONFIG_DSN is not set; using in-memory config only.',
    );
    return null;
  }

  if (!pool) {
    if (!sslWarningEmitted) {
      const sslMode = `${dsn} ${process.env.DATABASE_URL || ''}`.toLowerCase();
      if (sslMode.includes('sslmode=require') && !sslMode.includes('uselibpqcompat=true')) {
        console.warn(
          '[configStore] SSL warning: add uselibpqcompat=true or use direct 5432 Supabase host to avoid SSL chain errors.',
        );
        sslWarningEmitted = true;
      }
    }
    pool = new Pool({
      connectionString: dsn,
      max: DB_POOL_MAX,
      idleTimeoutMillis: DB_IDLE_TIMEOUT_MS,
      connectionTimeoutMillis: DB_CONNECTION_TIMEOUT_MS,
      options: `-c statement_timeout=${DB_STATEMENT_TIMEOUT_MS}`,
    });

    try {
      await testPoolConnection(pool);
      setDbReady(true);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS path_config (
          key        TEXT PRIMARY KEY,
          data       JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);
    } catch (error) {
      const category = classifyDbError(error);
      recordDbError(category);
      console.error('[configStore] Failed to initialize config DB connection', {
        category: category || 'unknown',
        message: error?.message,
      });
      pool = null;
      return null;
    }
  }

  return pool;
}

function classifyDbError(error) {
  const code = error?.code;
  const message = String(error?.message || '');
  if (code === 'SELF_SIGNED_CERT_IN_CHAIN' || message.includes('SELF_SIGNED_CERT_IN_CHAIN')) {
    return 'SELF_SIGNED_CERT_IN_CHAIN';
  }
  if (code === 'ETIMEDOUT' || message.includes('ETIMEDOUT')) {
    return 'ETIMEDOUT';
  }
  if (code === 'ECONNRESET' || message.includes('ECONNRESET')) {
    return 'ECONNRESET';
  }
  if (message.toLowerCase().includes('connection terminated unexpectedly')) {
    return 'CONNECTION_TERMINATED_UNEXPECTEDLY';
  }
  return null;
}

async function testPoolConnection(poolInstance) {
  let lastError;
  for (let attempt = 0; attempt < DB_CONNECT_RETRIES.length; attempt += 1) {
    try {
      await poolInstance.query('SELECT 1');
      return true;
    } catch (error) {
      lastError = error;
      const delay = DB_CONNECT_RETRIES[attempt];
      if (delay) {
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

async function loadJson(key) {
  const db = await getPool();
  if (!db) {
    return memory[key] ?? null;
  }

  let res;
  try {
    res = await db.query('SELECT data FROM path_config WHERE key = $1', [key]);
  } catch (err) {
    const category = classifyDbError(err);
    recordDbError(category);
    console.error(`[configStore] Failed to load ${key} from DB`, err);
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

  const db = await getPool();
  if (!db) return;

  const jsonText = JSON.stringify(value);

  try {
    await db.query(
      `
        INSERT INTO path_config (key, data)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (key)
        DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
      `,
      [key, jsonText],
    );
    setDbReady(true);
  } catch (err) {
    const category = classifyDbError(err);
    recordDbError(category);
    console.error(`[configStore] Failed to save ${key} to DB`, err);
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
    console.error('[configStore] Failed to save projects', err);
    await forwardSelfLog('error', 'Failed to save projects', {
      stack: err?.stack,
      context: { error: err?.message },
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
    console.error('[configStore] Failed to save globalSettings', err);
    await forwardSelfLog('error', 'Failed to save global settings', {
      stack: err?.stack,
      context: { error: err?.message },
    });
  }
}

module.exports = {
  classifyDbError,
  loadJson,
  saveJson,
  loadProjects,
  saveProjects,
  loadGlobalSettings,
  saveGlobalSettings,
};
