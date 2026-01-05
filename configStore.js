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

async function loadJson(key) {
  const db = await getPool();
  if (!db) {
    return memory[key] ?? null;
  }

  let res;
  try {
    res = await db.query('SELECT data FROM path_config WHERE key = $1', [key]);
  } catch (err) {
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

  await db.query(
    `
      INSERT INTO path_config (key, data)
      VALUES ($1, $2::jsonb)
      ON CONFLICT (key)
      DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
    `,
    [key, jsonText],
  );
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
  }
}

module.exports = {
  loadJson,
  saveJson,
  loadProjects,
  saveProjects,
  loadGlobalSettings,
  saveGlobalSettings,
};
