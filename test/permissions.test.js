const test = require('node:test');
const assert = require('node:assert/strict');
const { resolveRole, canAccess } = require('../src/permissions');

test('guest is read only', () => {
  const role = resolveRole('30', { ownerId: '1', adminIds: ['2'], guestIds: ['30'] });
  assert.equal(role, 'guest');
  assert.equal(canAccess(role, 'logs'), true);
  assert.equal(canAccess(role, 'settings'), true);
});


test('guest can read global sections but cannot mutate globally', () => {
  const role = resolveRole('30', { ownerId: '1', adminIds: ['2'], guestIds: ['30'] });
  assert.equal(canAccess(role, 'database'), true);
  assert.equal(canAccess(role, 'cronjobs'), true);
  assert.equal(canAccess(role, 'deploy'), true);
  assert.equal(canAccess(role, 'mutate:database'), false);
  assert.equal(canAccess(role, 'write:cronjobs'), false);
});
