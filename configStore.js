// Persistent storage for projects & globalSettings in Postgres (JSONB)

const { Pool } = require('pg');

let pool = null;

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
    pool = new Pool({
      connectionString: dsn,
      max: 3,
      idleTimeoutMillis: 30_000,
    });

    await pool.query(`
      CREATE TABLE IF NOT EXISTS path_config (
        key        TEXT PRIMARY KEY,
        data       JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
  }

  return pool;
}

async function loadJson(key, fallbackValue) {
  const db = await getPool();
  if (!db) {
    return memory[key] ?? fallbackValue;
  }

  try {
    const { rows } = await db.query(
      'SELECT data FROM path_config WHERE key = $1',
      [key],
    );
    if (rows.length === 0) return fallbackValue;

    const value = rows[0].data; // pg jsonb → JS object
    memory[key] = value;
    return value;
  } catch (err) {
    console.error(`[configStore] Failed to load ${key} from DB`, err);
    return memory[key] ?? fallbackValue;
  }
}

async function saveJson(key, value) {
  memory[key] = value;

  const db = await getPool();
  if (!db) return;

  try {
    await db.query(
      `
        INSERT INTO path_config (key, data)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (key) DO UPDATE
        SET data = EXCLUDED.data,
            updated_at = NOW();
      `,
      // IMPORTANT: pass value directly; DO NOT JSON.stringify here.
      [key, value],
    );
  } catch (err) {
    console.error(`[configStore] Failed to save ${key} to DB`, err);
  }
}

// Public API

async function loadProjects() {
  return loadJson('projects', []);
}

async function saveProjects(projects) {
  await saveJson('projects', projects || []);
}

async function loadGlobalSettings() {
  return loadJson('globalSettings', null);
}

async function saveGlobalSettings(settings) {
  await saveJson('globalSettings', settings || {});
}

module.exports = {
  loadProjects,
  saveProjects,
  loadGlobalSettings,
  saveGlobalSettings,
};
