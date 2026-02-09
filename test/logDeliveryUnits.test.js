const test = require('node:test');
const assert = require('node:assert/strict');

const { createRateLimiter, shouldRouteBySeverity } = require('../logIngestService');
const { maskSecrets, computeDestinations } = require('../opsReliability');

test('rate limiter blocks over burst', () => {
  const limiter = createRateLimiter({ limitPerMinute: 1, burst: 2 });
  const now = Date.now();
  assert.equal(limiter('p1', now), true);
  assert.equal(limiter('p1', now), true);
  assert.equal(limiter('p1', now), false);
});

test('routing decision obeys threshold and mute categories', () => {
  assert.equal(shouldRouteBySeverity({ threshold: 'warn', level: 'info' }), false);
  assert.equal(shouldRouteBySeverity({ threshold: 'warn', level: 'error' }), true);
  assert.equal(shouldRouteBySeverity({ threshold: 'info', level: 'error', muteCategories: ['TEST'], category: 'TEST' }), false);
});

test('masking removes secrets from reports', () => {
  const masked = maskSecrets('token=abcdef password=1234 postgres://user:secret@host/db');
  assert.doesNotMatch(masked, /secret@/i);
  assert.doesNotMatch(masked, /password=1234/i);
});

test('destinations include room/rob only for high severity', () => {
  const prefs = { destinations: { admin_inbox: true, admin_room: true, admin_rob: true } };
  const warn = computeDestinations(prefs, 'warn');
  assert.equal(warn.admin_room, false);
  const err = computeDestinations(prefs, 'error');
  assert.equal(err.admin_room, true);
});
