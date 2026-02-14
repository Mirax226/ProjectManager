const test = require('node:test');
const assert = require('node:assert/strict');
const {
  parsePostgresDsn,
  normalizePostgresDsn,
  validatePostgresDsn,
  maskPostgresDsn,
} = require('../src/db/dsn');

test('normalize + mask postgres dsn', () => {
  const input = 'postgres://user:pa ss@localhost:5432/app_db?sslmode=require';
  const normalized = normalizePostgresDsn(input);
  assert.equal(normalized.dsn.startsWith('postgresql://'), true);
  assert.equal(normalized.dsn.includes('pa%20ss'), true);
  const masked = maskPostgresDsn(normalized.dsn);
  assert.equal(masked.includes('***@localhost:5432/app_db'), true);
});

test('parse postgres dsn', () => {
  const parsed = parsePostgresDsn('postgresql://john:secret@db.example.com:5433/mydb?sslmode=require');
  assert.equal(parsed.user, 'john');
  assert.equal(parsed.password, 'secret');
  assert.equal(parsed.host, 'db.example.com');
  assert.equal(parsed.db, 'mydb');
  assert.equal(parsed.params.sslmode, 'require');
});

test('validate postgres dsn', () => {
  const invalid = validatePostgresDsn('postgresql://user@localhost');
  assert.equal(invalid.ok, false);
  const valid = validatePostgresDsn('postgresql://user:pass@localhost:5432/main');
  assert.equal(valid.ok, true);
});
