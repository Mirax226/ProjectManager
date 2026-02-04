const crypto = require('crypto');
const { getConfigDbPool } = require('./configDb');
const { appState } = require('./appState');

let tableReady = false;
const memory = [];
const DB_OPERATION_TIMEOUT_MS = 5000;

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

async function ensureLogTable(db) {
  if (!db || tableReady) return;
  await withDbTimeout(
    db.query(`
    CREATE TABLE IF NOT EXISTS self_recent_logs (
      id UUID PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      level TEXT NOT NULL,
      message TEXT NOT NULL,
      stack TEXT NULL,
      context JSONB NULL
    );
  `),
    'self_log_table',
  );
  tableReady = true;
}

async function getDb() {
  if (!appState.dbReady || appState.degradedMode) {
    return null;
  }
  const db = await getConfigDbPool();
  if (db) {
    await ensureLogTable(db);
  }
  return db;
}

async function addSelfLog(entry) {
  const logEntry = {
    id: entry.id || crypto.randomUUID(),
    createdAt: entry.createdAt || new Date().toISOString(),
    level: entry.level || 'error',
    message: entry.message || '(no message)',
    stack: entry.stack || null,
    context: entry.context ?? null,
  };
  const db = await getDb();
  if (!db) {
    memory.unshift(logEntry);
    if (memory.length > 50) {
      memory.length = 50;
    }
    return logEntry;
  }
  try {
    await withDbTimeout(
      db.query(
        `
        INSERT INTO self_recent_logs (id, created_at, level, message, stack, context)
        VALUES ($1, $2, $3, $4, $5, $6)
      `,
        [
          logEntry.id,
          logEntry.createdAt,
          logEntry.level,
          logEntry.message,
          logEntry.stack,
          logEntry.context,
        ],
      ),
      'self_log_insert',
    );
    await withDbTimeout(
      db.query(
        `
        DELETE FROM self_recent_logs
        WHERE id IN (
          SELECT id FROM self_recent_logs
          ORDER BY created_at DESC
          OFFSET 50
        )
      `,
      ),
      'self_log_trim',
    );
  } catch (error) {
    console.error('[self-logs] Failed to store self log', error);
  }
  return logEntry;
}

async function listSelfLogs(limit = 50, offset = 0) {
  const db = await getDb();
  if (!db) {
    return memory.slice(offset, offset + limit);
  }
  try {
    const { rows } = await withDbTimeout(
      db.query(
        `
        SELECT id, created_at, level, message, stack, context
        FROM self_recent_logs
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
      `,
        [limit, offset],
      ),
      'self_log_list',
    );
    return rows.map((row) => ({
      id: row.id,
      createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
      level: row.level,
      message: row.message,
      stack: row.stack,
      context: row.context,
    }));
  } catch (error) {
    console.error('[self-logs] Failed to list self logs', error);
    return memory.slice(offset, offset + limit);
  }
}

async function getSelfLogById(logId) {
  const db = await getDb();
  if (!db) {
    return memory.find((log) => log.id === logId) || null;
  }
  try {
    const { rows } = await withDbTimeout(
      db.query(
        `
        SELECT id, created_at, level, message, stack, context
        FROM self_recent_logs
        WHERE id = $1
        LIMIT 1
      `,
        [logId],
      ),
      'self_log_get',
    );
    if (!rows.length) return null;
    const row = rows[0];
    return {
      id: row.id,
      createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
      level: row.level,
      message: row.message,
      stack: row.stack,
      context: row.context,
    };
  } catch (error) {
    console.error('[self-logs] Failed to load self log', error);
    return memory.find((log) => log.id === logId) || null;
  }
}

module.exports = {
  addSelfLog,
  listSelfLogs,
  getSelfLogById,
};
