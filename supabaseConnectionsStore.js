const fs = require('fs/promises');
const path = require('path');
const { Pool } = require('pg');

const CONNECTIONS_FILE = path.join(__dirname, 'supabaseConnections.json');
const CONFIG_DSN = process.env.PATH_APPLIER_CONFIG_DSN;
let cachedConnections;
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

async function saveSupabaseConnections(connections) {
  const db = await getPool();
  if (!db) {
    return;
  }
  await db.query(
    `INSERT INTO path_config (key, data)
     VALUES ($1, $2)
     ON CONFLICT (key) DO UPDATE
     SET data = EXCLUDED.data, updated_at = NOW()`,
    ['supabaseConnections', connections],
  );
}

async function loadSupabaseConnections() {
  if (cachedConnections) {
    return cachedConnections;
  }
  try {
    const db = await getPool();
    if (db) {
      const result = await db.query('SELECT data FROM path_config WHERE key = $1', ['supabaseConnections']);
      if (result.rows.length) {
        cachedConnections = result.rows[0].data || [];
        return cachedConnections;
      }
    }
  } catch (error) {
    console.error('Failed to load supabase connections', error);
  }
  try {
    const raw = await fs.readFile(CONNECTIONS_FILE, 'utf-8');
    cachedConnections = JSON.parse(raw);
    try {
      if (cachedConnections.length) {
        await saveSupabaseConnections(cachedConnections);
      }
    } catch (error) {
      console.error('Failed to save supabase connections', error);
    }
    return cachedConnections;
  } catch (error) {
    console.error('Failed to load supabaseConnections.json', error);
    cachedConnections = [];
    return cachedConnections;
  }
}

async function findSupabaseConnection(id) {
  const connections = await loadSupabaseConnections();
  return connections.find((conn) => conn.id === id);
}

module.exports = {
  loadSupabaseConnections,
  findSupabaseConnection,
};
