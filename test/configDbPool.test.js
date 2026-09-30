const test = require('node:test');
const assert = require('node:assert/strict');
const pg = require('pg');

async function fixture(dsn, run) {
  const previous = process.env.DATABASE_URL_PM;
  const previousFallback = process.env.PATH_APPLIER_CONFIG_DSN;
  const originalPool = pg.Pool;
  const loggerPath = require.resolve('../logger');
  const originalLogger = require.cache[loggerPath];
  const modulePath = require.resolve('../configDb');
  let constructed = 0;
  let connectionString;
  const db = { query: async () => ({ rows: [{ value: 1 }] }) };
  pg.Pool = class { constructor(options) { constructed++; connectionString = options.connectionString; return db; } };
  require.cache[loggerPath] = { id: loggerPath, filename: loggerPath, loaded: true, exports: { forwardSelfLog: async () => {} } };
  process.env.DATABASE_URL_PM = dsn;
  delete process.env.PATH_APPLIER_CONFIG_DSN;
  delete require.cache[modulePath];
  try { await run(require('../configDb'), () => ({ constructed, connectionString }), db); }
  finally {
    pg.Pool = originalPool;
    delete require.cache[modulePath];
    if (originalLogger) require.cache[loggerPath] = originalLogger; else delete require.cache[loggerPath];
    if (previous === undefined) delete process.env.DATABASE_URL_PM; else process.env.DATABASE_URL_PM = previous;
    if (previousFallback === undefined) delete process.env.PATH_APPLIER_CONFIG_DSN; else process.env.PATH_APPLIER_CONFIG_DSN = previousFallback;
  }
}

test('Config DB malformed/structurally invalid DSNs fail before any Postgres client construction', async () => {
  for (const [dsn, category] of [['invalid', 'CONFIG_DB_DSN_INVALID'], ['postgres://user:secret@bad_host/db', 'CONFIG_DB_HOST_INVALID']]) {
    await fixture(dsn, async (config, state) => {
      const result = await config.testConfigDbConnection();
      assert.equal(result.category, category);
      assert.equal(state().constructed, 0);
      assert.equal(result.message.includes('secret'), false);
    });
  }
});

test('Config DB repeated pool reads retain credential-only repair without mutating ENV', async () => {
  const raw = 'postgres://postgres.tenant:pa?ss@db.example.com/db';
  await fixture(raw, async (config, state) => {
    const first = await config.getConfigDbPool();
    const second = await config.getConfigDbPool();
    assert.equal(first, second);
    assert.equal(state().constructed, 1);
    assert.equal(new URL(state().connectionString).hostname, 'db.example.com');
    assert.equal(process.env.DATABASE_URL_PM, raw);
  });
});

test('Config DB probe preserves actual DNS and provider tenant failure evidence', async () => {
  await fixture('postgres://postgres.tenant:secret@db.example.com/db', async (config, state, db) => {
    db.query = async () => { throw Object.assign(new Error('tenant/user postgres.tenant not found'), { code: 'XX000' }); };
    assert.equal((await config.testConfigDbConnection()).category, 'CONFIG_DB_AUTH_FAILED');
    db.query = async () => { throw Object.assign(new Error('getaddrinfo ENOTFOUND db.example.com'), { code: 'ENOTFOUND' }); };
    assert.equal((await config.testConfigDbConnection()).category, 'CONFIG_DB_DNS_FAILED');
  });
});
