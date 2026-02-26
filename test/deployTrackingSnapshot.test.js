const assert = require('node:assert/strict');
const test = require('node:test');

const { __test } = require('../bot');

test('deploy snapshot renders populated fields when deploy exists', () => {
  const text = __test.buildDeployTrackingSnapshotMessage(
    { id: 'pm', name: 'Project Manager' },
    { render: { serviceId: 'srv-1', serviceName: 'pm-service' } },
    {
      deployId: 'dep-123',
      status: 'live',
      updatedAt: '2026-01-01T00:00:00.000Z',
      commitId: 'abc123',
      trigger: 'new_commit',
      image: 'render/image',
    },
    { endpoint: '/services/srv-1/deploys?limit=1', httpStatus: 200, durationMs: 50, returnedCount: 1 },
  );

  assert.match(text, /DeployId: dep-123/);
  assert.match(text, /Status: live/);
  assert.match(text, /Updated: 2026-01-01T00:00:00.000Z/);
  assert.match(text, /Commit: abc123/);
});

test('deploy snapshot renders explicit no-deploy message', () => {
  const text = __test.buildDeployTrackingSnapshotMessage(
    { id: 'pm', name: 'Project Manager' },
    { render: { serviceId: 'srv-1', serviceName: null } },
    null,
    { endpoint: '/services/srv-1/deploys?limit=1', httpStatus: 200, durationMs: 50, returnedCount: 0 },
  );

  assert.match(text, /No deploys found for this service yet\./);
  assert.doesNotMatch(text, /DeployId: -/);
  assert.doesNotMatch(text, /Status: -/);
});
