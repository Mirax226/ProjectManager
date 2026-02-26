const assert = require('node:assert/strict');
const test = require('node:test');

const { parseDeploy } = require('../src/renderClient');

test('parseDeploy extracts normalized fields from render payload', () => {
  const parsed = parseDeploy({
    id: 'dep-12345',
    status: 'live',
    updatedAt: '2026-01-01T00:00:00.000Z',
    commit: { id: 'abc123def456' },
    trigger: { type: 'new_commit' },
    image: 'render/image:latest',
  });

  assert.equal(parsed.deployId, 'dep-12345');
  assert.equal(parsed.status, 'live');
  assert.equal(parsed.updatedAt, '2026-01-01T00:00:00.000Z');
  assert.equal(parsed.commitId, 'abc123def456');
  assert.equal(parsed.trigger, 'new_commit');
  assert.equal(parsed.image, 'render/image:latest');
});

test('parseDeploy handles empty payload safely', () => {
  const parsed = parseDeploy(null);
  assert.equal(parsed.deployId, null);
  assert.equal(parsed.status, null);
  assert.equal(parsed.updatedAt, null);
});
