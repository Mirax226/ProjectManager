const test = require('node:test');
const assert = require('node:assert/strict');
const { resolveRole, canAccess } = require('../src/permissions');

test('guest is read only', () => {
  const role = resolveRole('30', { ownerId: '1', adminIds: ['2'], guestIds: ['30'] });
  assert.equal(role, 'guest');
  assert.equal(canAccess(role, 'logs'), true);
  assert.equal(canAccess(role, 'settings'), false);
});
