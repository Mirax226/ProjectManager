const { Pool } = require('pg');

const CONFIG_DSN = process.env.PATH_APPLIER_CONFIG_DSN;

let pool;
let isReady = false;

async function getPool() {
  if (!CONFIG_DSN) {
    return null;
  }
  if (!pool) {
    pool = new Pool({ connectionString: CONFIG_DSN });
  }
  if (!isReady) {
    await pool.query(
      `CREATE TABLE IF NOT EXISTS path_config (
        id          SERIAL PRIMARY KEY,
        key         TEXT UNIQUE NOT NULL,
        data        JSONB NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );`,
    );
    isReady = true;
  }
  return pool;
}

async function loadProjects() {
  try {
    const db = await getPool();
    if (!db) {
      console.warn('PATH_APPLIER_CONFIG_DSN not set, skipping DB load for projects.');
      return [];
    }
    const result = await db.query('SELECT data FROM path_config WHERE key = $1', ['projects']);
    if (!result.rows.length) {
      return [];
    }
    return result.rows[0].data || [];
  } catch (error) {
    console.error('Failed to load projects from DB', error);
    return [];
  }
}

async function saveProjects(projects) {
  try {
    const db = await getPool();
    if (!db) {
      console.warn('PATH_APPLIER_CONFIG_DSN not set, skipping DB save for projects.');
      return;
    }
    await db.query(
      `INSERT INTO path_config (key, data)
       VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE
       SET data = EXCLUDED.data, updated_at = NOW()`,
      ['projects', projects],
    );
  } catch (error) {
    console.error('Failed to save projects to DB', error);
  }
}

async function loadGlobalSettings() {
  try {
    const db = await getPool();
    if (!db) {
      console.warn('PATH_APPLIER_CONFIG_DSN not set, skipping DB load for global settings.');
      return null;
    }
    const result = await db.query('SELECT data FROM path_config WHERE key = $1', ['globalSettings']);
    if (!result.rows.length) {
      return null;
    }
    return result.rows[0].data || null;
  } catch (error) {
    console.error('Failed to load global settings from DB', error);
    return null;
  }
}

async function saveGlobalSettings(settings) {
  try {
    const db = await getPool();
    if (!db) {
      console.warn('PATH_APPLIER_CONFIG_DSN not set, skipping DB save for global settings.');
      return;
    }
    await db.query(
      `INSERT INTO path_config (key, data)
       VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE
       SET data = EXCLUDED.data, updated_at = NOW()`,
      ['globalSettings', settings],
    );
  } catch (error) {
    console.error('Failed to save global settings to DB', error);
  }
}

module.exports = {
  loadProjects,
  saveProjects,
  loadGlobalSettings,
  saveGlobalSettings,
};
