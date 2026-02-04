const { Pool } = require('pg');

let pool = null;
const DB_CONNECTION_TIMEOUT_MS = 5000;
const DB_STATEMENT_TIMEOUT_MS = 5000;

async function getConfigDbPool() {
  const dsn = process.env.PATH_APPLIER_CONFIG_DSN;
  if (!dsn) {
    console.warn('[configDb] PATH_APPLIER_CONFIG_DSN is not set; using in-memory config only.');
    return null;
  }

  if (!pool) {
    pool = new Pool({
      connectionString: dsn,
      max: 3,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: DB_CONNECTION_TIMEOUT_MS,
      options: `-c statement_timeout=${DB_STATEMENT_TIMEOUT_MS}`,
    });
  }

  return pool;
}

module.exports = {
  getConfigDbPool,
};
