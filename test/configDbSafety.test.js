const test = require('node:test');
const assert = require('node:assert/strict');

const { tryFixPostgresDsn, maskDsn } = require('../configDb');
const { sanitizeDbErrorMessage } = require('../configDbErrors');

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
