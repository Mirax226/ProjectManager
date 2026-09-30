const test = require('node:test');
const assert = require('node:assert/strict');

const { tryFixPostgresDsn, maskDsn, inspectConfigDbDsn, getConfigDbEnvSource } = require('../configDb');
const { sanitizeDbErrorMessage, classifyConfigDbError } = require('../configDbErrors');

test('tryFixPostgresDsn encodes userinfo only and preserves host/query', () => {
  const input = 'postgres://my user:pa?ss@db.example.com:5432/app?sslmode=require&application_name=pm';
  const result = tryFixPostgresDsn(input);
  assert.equal(result.fixed, true);
  assert.equal(result.dsn.includes('db.example.com:5432/app?sslmode=require&application_name=pm'), true);
  assert.equal(result.dsn.includes('%3F'), true);
  assert.equal(result.dsn.includes('%20'), true);
  assert.equal(result.dsn.includes('sslmode=require'), true);
});

test('sanitizeDbErrorMessage redacts DSN/password/token content', () => {
  const raw = 'connect failed postgres://user:supersecret@host/db password=hunter2 token=abc123';
  const sanitized = sanitizeDbErrorMessage(raw);
  assert.equal(sanitized.includes('supersecret'), false);
  assert.equal(sanitized.includes('hunter2'), false);
  assert.equal(sanitized.includes('abc123'), false);
  assert.equal(maskDsn('postgres://user:supersecret@host/db').includes('supersecret'), false);
});

test('DSN preflight separates postgres tenant username from hostname', () => {
  const result = inspectConfigDbDsn('postgresql://postgres.bqvyprmlqcbrwepnrwoc:secret@db.example.com:5432/project?sslmode=require');
  assert.equal(result.category, null);
  assert.equal(result.hostname, 'db.example.com');
  assert.equal(result.usernamePresent, true);
  assert.equal(result.port, 5432);
  assert.equal(result.databaseNamePresent, true);
});

test('DSN preflight rejects malformed URL and invalid host without network access', () => {
  assert.equal(inspectConfigDbDsn('postgres://user:secret@').category, 'CONFIG_DB_DSN_INVALID');
  assert.equal(inspectConfigDbDsn('postgres://user:secret@bad_host/db').category, 'CONFIG_DB_HOST_INVALID');
});

test('ENOTFOUND is classified at the known DNS stage', () => {
  const category = classifyConfigDbError(Object.assign(new Error('getaddrinfo ENOTFOUND db.example.com'), { code: 'ENOTFOUND' }));
  assert.equal(category, 'CONFIG_DB_DNS_FAILED');
});

test('Config DB source precedence is explicit and metadata-only', () => {
  const previousPrimary = process.env.DATABASE_URL_PM;
  const previousFallback = process.env.PATH_APPLIER_CONFIG_DSN;
  try {
    delete process.env.DATABASE_URL_PM;
    process.env.PATH_APPLIER_CONFIG_DSN = 'postgres://user:secret@db.example.com/app';
    assert.deepEqual(getConfigDbEnvSource(), { envVar: 'PATH_APPLIER_CONFIG_DSN', dsn: process.env.PATH_APPLIER_CONFIG_DSN });
    process.env.DATABASE_URL_PM = 'postgres://primary:secret@db.example.com/app';
    assert.equal(getConfigDbEnvSource().envVar, 'DATABASE_URL_PM');
  } finally {
    if (previousPrimary === undefined) delete process.env.DATABASE_URL_PM; else process.env.DATABASE_URL_PM = previousPrimary;
    if (previousFallback === undefined) delete process.env.PATH_APPLIER_CONFIG_DSN; else process.env.PATH_APPLIER_CONFIG_DSN = previousFallback;
  }
});
