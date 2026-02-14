const { Pool } = require('pg');
const { getConfigDbPool } = require('../../configDb');
const { ensureDbHubSchema, listProjectConnections } = require('./hubStore');

const poolCache = new Map();

function getPool(dsn) {
  if (!poolCache.has(dsn)) {
    poolCache.set(dsn, new Pool({ connectionString: dsn, max: 2, idleTimeoutMillis: 15000 }));
  }
  return poolCache.get(dsn);
}

async function queueWriteRetry({ projectId, targetRole, sqlText, params, error }) {
  const db = await getConfigDbPool();
  if (!db) return;
  await ensureDbHubSchema();
  await db.query(
    `INSERT INTO pm_db_write_queue(project_id,target_role,sql_text,params,status,last_error)
     VALUES($1,$2,$3,$4,'queued',$5)`,
    [projectId, targetRole, sqlText, JSON.stringify(params || []), String(error?.message || 'unknown')],
  );
}

async function getDbClient(projectId, opType = 'read') {
  const connections = await listProjectConnections(projectId, { includeDsn: true });
  const primary = connections.find((c) => c.role === 'primary' && c.enabled);
  const secondary = connections.find((c) => c.role === 'secondary' && c.enabled);
  if (opType === 'read') {
    if (primary) return { mode: 'single', role: 'primary', client: getPool(primary.dsn) };
    if (secondary) return { mode: 'single', role: 'secondary', client: getPool(secondary.dsn) };
    return null;
  }
  return {
    mode: 'dual',
    primary: primary ? getPool(primary.dsn) : null,
    secondary: secondary ? getPool(secondary.dsn) : null,
    async write(sqlText, params = []) {
      const tasks = [
        primary ? primary.role : null,
        secondary ? secondary.role : null,
      ].filter(Boolean).map(async (role) => {
        const pool = role === 'primary' ? getPool(primary.dsn) : getPool(secondary.dsn);
        try {
          await Promise.race([
            pool.query(sqlText, params),
            new Promise((_, reject) => setTimeout(() => reject(new Error('WRITE_TIMEOUT')), 3000)),
          ]);
          return { role, ok: true };
        } catch (error) {
          await queueWriteRetry({ projectId, targetRole: role, sqlText, params, error });
          return { role, ok: false, error: error.message };
        }
      });
      return Promise.all(tasks);
    },
  };
}

module.exports = { getDbClient };
