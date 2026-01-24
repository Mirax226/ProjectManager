const crypto = require('crypto');
const { getConfigDbPool } = require('./configDb');

let tableReady = false;
const memory = [];

async function ensureLogTable(db) {
  if (!db || tableReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS self_recent_logs (
      id UUID PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      level TEXT NOT NULL,
      message TEXT NOT NULL,
      stack TEXT NULL,
      context JSONB NULL
    );
  `);
  tableReady = true;
}

async function getDb() {
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
    await db.query(
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
    );
    await db.query(
      `
        DELETE FROM self_recent_logs
        WHERE id IN (
          SELECT id FROM self_recent_logs
          ORDER BY created_at DESC
          OFFSET 50
        )
      `,
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
  const { rows } = await db.query(
    `
      SELECT id, created_at, level, message, stack, context
      FROM self_recent_logs
      ORDER BY created_at DESC
      LIMIT $1 OFFSET $2
    `,
    [limit, offset],
  );
  return rows.map((row) => ({
    id: row.id,
    createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
    level: row.level,
    message: row.message,
    stack: row.stack,
    context: row.context,
  }));
}

async function getSelfLogById(logId) {
  const db = await getDb();
  if (!db) {
    return memory.find((log) => log.id === logId) || null;
  }
  const { rows } = await db.query(
    `
      SELECT id, created_at, level, message, stack, context
      FROM self_recent_logs
      WHERE id = $1
      LIMIT 1
    `,
    [logId],
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
}

module.exports = {
  addSelfLog,
  listSelfLogs,
  getSelfLogById,
};
