const { Pool } = require('pg');
const { classifyDbError, sanitizeDbErrorMessage } = require('./configDbErrors');

let pool = null;
let sslWarningEmitted = false;
const DB_POOL_MAX = 3;
const DB_IDLE_TIMEOUT_MS = 30_000;
const DB_CONNECTION_TIMEOUT_MS = 15_000;
const DB_STATEMENT_TIMEOUT_MS = 5000;
const DB_OPERATION_TIMEOUT_MS = 5000;

async function getConfigDbPool() {
  const dsn = process.env.PATH_APPLIER_CONFIG_DSN;
  if (!dsn) {
    console.warn('[configDb] PATH_APPLIER_CONFIG_DSN is not set; using in-memory config only.');
    return null;
  }

  if (!pool) {
    if (!sslWarningEmitted) {
      const sslMode = `${dsn} ${process.env.DATABASE_URL || ''}`.toLowerCase();
      if (sslMode.includes('sslmode=require') && !sslMode.includes('uselibpqcompat=true')) {
        console.warn(
          '[configDb] SSL warning: add uselibpqcompat=true or use direct 5432 Supabase host to avoid SSL chain errors.',
        );
        sslWarningEmitted = true;
      }
    }
    const sslMode = `${dsn} ${process.env.DATABASE_URL || ''}`.toLowerCase();
    const ssl =
      sslMode.includes('sslmode=require') || sslMode.includes('ssl=true')
        ? { rejectUnauthorized: false }
        : undefined;
    pool = new Pool({
      connectionString: dsn,
      max: DB_POOL_MAX,
      idleTimeoutMillis: DB_IDLE_TIMEOUT_MS,
      connectionTimeoutMillis: DB_CONNECTION_TIMEOUT_MS,
      options: `-c statement_timeout=${DB_STATEMENT_TIMEOUT_MS}`,
      keepAlive: true,
      ssl,
    });
  }

  return pool;
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

async function testConfigDbConnection() {
  const db = await getConfigDbPool();
  if (!db) {
    return { ok: false, category: 'UNKNOWN_DB_ERROR', message: 'not configured', configured: false };
  }
  try {
    await withDbTimeout(db.query('SELECT 1'), 'config_db_test');
    return { ok: true, configured: true };
  } catch (error) {
    const category = classifyDbError(error);
    const message = sanitizeDbErrorMessage(error?.message) || 'connection failed';
    return { ok: false, category, message, configured: true };
  }
}

module.exports = {
  getConfigDbPool,
  testConfigDbConnection,
};
