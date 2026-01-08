const { Pool } = require('pg');

let pool = null;

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
    });
  }

  return pool;
}

module.exports = {
  getConfigDbPool,
};
