const { getConfigDbPool } = require('./configDb');
const { forwardSelfLog } = require('./logger');

let tablesReady = false;
const memory = {
  settings: new Map(),
  logs: new Map(),
};

const DEFAULT_SETTINGS = {
  enabled: true,
  levels: ['error'],
  destinationChatId: null,
};

async function ensureLogTables(db) {
  if (!db || tablesReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS project_log_settings (
      project_id TEXT PRIMARY KEY,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      levels TEXT[] NOT NULL DEFAULT ARRAY['error']::TEXT[],
      destination_chat_id TEXT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS project_recent_logs (
      id UUID PRIMARY KEY,
      project_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      level TEXT NOT NULL,
      service TEXT NOT NULL,
      env TEXT NOT NULL,
      timestamp TIMESTAMPTZ NULL,
      message TEXT NOT NULL,
      stack TEXT NULL,
      context JSONB NULL
    );
  `);
  await db.query(
    'CREATE INDEX IF NOT EXISTS project_recent_logs_project_id_idx ON project_recent_logs (project_id, created_at DESC)'
  );
  tablesReady = true;
}

async function getDb() {
  const db = await getConfigDbPool();
  if (db) {
    await ensureLogTables(db);
  }
  return db;
}

function normalizeLevels(levels) {
  if (!Array.isArray(levels)) return [...DEFAULT_SETTINGS.levels];
  const normalized = levels.map((level) => String(level).toLowerCase()).filter(Boolean);
  return normalized.length ? normalized : [...DEFAULT_SETTINGS.levels];
}

function normalizeSettings(settings) {
  const payload = settings || {};
  return {
    enabled: typeof payload.enabled === 'boolean' ? payload.enabled : DEFAULT_SETTINGS.enabled,
    levels: normalizeLevels(payload.levels),
    destinationChatId: payload.destinationChatId ? String(payload.destinationChatId) : null,
  };
}

function getMemoryLogs(projectId) {
  if (!memory.logs.has(projectId)) {
    memory.logs.set(projectId, []);
  }
  return memory.logs.get(projectId);
}

async function getProjectLogSettings(projectId) {
  const db = await getDb();
  if (!db) {
    return memory.settings.get(projectId) || null;
  }
  const { rows } = await db.query(
    'SELECT enabled, levels, destination_chat_id FROM project_log_settings WHERE project_id = $1 LIMIT 1',
    [projectId],
  );
  if (!rows.length) return null;
  return normalizeSettings({
    enabled: rows[0].enabled,
    levels: rows[0].levels,
    destinationChatId: rows[0].destination_chat_id,
  });
}

async function upsertProjectLogSettings(projectId, settings) {
  const normalized = normalizeSettings(settings);
  const db = await getDb();
  if (!db) {
    memory.settings.set(projectId, normalized);
    return normalized;
  }
  try {
    await db.query(
      `
        INSERT INTO project_log_settings (project_id, enabled, levels, destination_chat_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (project_id)
        DO UPDATE SET
          enabled = EXCLUDED.enabled,
          levels = EXCLUDED.levels,
          destination_chat_id = EXCLUDED.destination_chat_id,
          updated_at = NOW()
      `,
      [projectId, normalized.enabled, normalized.levels, normalized.destinationChatId],
    );
  } catch (error) {
    console.error('[log-settings] Failed to upsert log settings', error);
    await forwardSelfLog('error', 'Failed to save log settings', {
      stack: error?.stack,
      context: { error: error?.message, projectId },
    });
    throw error;
  }
  return normalized;
}

function coerceTimestamp(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

async function addRecentLog(projectId, entry) {
  const logEntry = {
    id: entry.id,
    projectId,
    createdAt: entry.createdAt || new Date().toISOString(),
    level: entry.level,
    service: entry.service,
    env: entry.env,
    timestamp: coerceTimestamp(entry.timestamp),
    message: entry.message,
    stack: entry.stack || null,
    context: entry.context ?? null,
  };
  const db = await getDb();
  if (!db) {
    const logs = getMemoryLogs(projectId);
    logs.unshift(logEntry);
    memory.logs.set(projectId, logs.slice(0, 50));
    return logEntry;
  }
  try {
    await db.query(
      `
        INSERT INTO project_recent_logs
          (id, project_id, created_at, level, service, env, timestamp, message, stack, context)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `,
      [
        logEntry.id,
        projectId,
        logEntry.createdAt,
        logEntry.level,
        logEntry.service,
        logEntry.env,
        logEntry.timestamp,
        logEntry.message,
        logEntry.stack,
        logEntry.context,
      ],
    );

    await db.query(
      `
        DELETE FROM project_recent_logs
        WHERE id IN (
          SELECT id FROM project_recent_logs
          WHERE project_id = $1
          ORDER BY created_at DESC
          OFFSET 50
        )
      `,
      [projectId],
    );
  } catch (error) {
    console.error('[log-settings] Failed to add recent log', error);
    await forwardSelfLog('error', 'Failed to store recent log', {
      stack: error?.stack,
      context: { error: error?.message, projectId },
    });
    throw error;
  }
  return logEntry;
}

async function listRecentLogs(projectId, limit = 50, offset = 0) {
  const db = await getDb();
  if (!db) {
    const logs = getMemoryLogs(projectId);
    return logs.slice(offset, offset + limit);
  }
  const { rows } = await db.query(
    `
      SELECT id, project_id, created_at, level, service, env, timestamp, message, stack, context
      FROM project_recent_logs
      WHERE project_id = $1
      ORDER BY created_at DESC
      LIMIT $2 OFFSET $3
    `,
    [projectId, limit, offset],
  );
  return rows.map((row) => ({
    id: row.id,
    projectId: row.project_id,
    createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
    level: row.level,
    service: row.service,
    env: row.env,
    timestamp: row.timestamp instanceof Date ? row.timestamp.toISOString() : row.timestamp,
    message: row.message,
    stack: row.stack,
    context: row.context,
  }));
}

async function getRecentLogById(projectId, logId) {
  const db = await getDb();
  if (!db) {
    const logs = getMemoryLogs(projectId);
    return logs.find((log) => log.id === logId) || null;
  }
  const { rows } = await db.query(
    `
      SELECT id, project_id, created_at, level, service, env, timestamp, message, stack, context
      FROM project_recent_logs
      WHERE project_id = $1 AND id = $2
      LIMIT 1
    `,
    [projectId, logId],
  );
  if (!rows.length) return null;
  const row = rows[0];
  return {
    id: row.id,
    projectId: row.project_id,
    createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
    level: row.level,
    service: row.service,
    env: row.env,
    timestamp: row.timestamp instanceof Date ? row.timestamp.toISOString() : row.timestamp,
    message: row.message,
    stack: row.stack,
    context: row.context,
  };
}

module.exports = {
  DEFAULT_SETTINGS,
  getProjectLogSettings,
  upsertProjectLogSettings,
  addRecentLog,
  listRecentLogs,
  getRecentLogById,
};
