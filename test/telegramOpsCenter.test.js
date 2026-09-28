const test = require('node:test');
const assert = require('node:assert/strict');
const {
  SAFE_ACTIONS,
  buildIncidentCenterModel,
  buildIncidentDetailModel,
  buildRunnerDashboardModel,
  isSafeOpsAction,
} = require('../src/telegramOpsCenter');

test('incident center renders operational fields and sanitizes messages', () => {
  const model = buildIncidentCenterModel([{
    id: 'inc-1', projectId: 'daily-system', environment: 'prod', level: 'error', category: 'D1_ERROR',
    meta_json: { component: 'control-plane' }, first_seen_at: '2026-09-25T10:00:00.000Z', last_seen_at: '2026-09-25T10:05:00.000Z',
    occurrence_count: 3, message_short: 'password=super-secret database unavailable', status: 'open',
  }], 'open');
  assert.match(model.text, /daily-system/);
  assert.match(model.text, /control-plane/);
  assert.match(model.text, /Occurrences: 3/);
  assert.match(model.text, /password=\[REDACTED\]/);
  assert.doesNotMatch(model.text, /super-secret/);
  const detail = buildIncidentDetailModel({ id: 'inc-1', projectId: 'daily-system', message_short: 'token=raw-value postgres://user:db-password@host/db' });
  assert.doesNotMatch(detail.text, /raw-value/);
  assert.doesNotMatch(detail.text, /db-password/);
});

test('operational actions require admin or owner and exclude unsafe actions', () => {
  for (const action of ['details', 'health', 'timeline', 'acknowledge', 'codex_task']) {
    assert.equal(isSafeOpsAction('admin', action), true);
    assert.equal(isSafeOpsAction('guest', action), false);
  }
  assert.equal(isSafeOpsAction('admin', 'shell'), false);
  assert.equal(isSafeOpsAction('admin', 'delete'), false);
  assert.equal(isSafeOpsAction('admin', 'secret'), false);
  assert.equal(SAFE_ACTIONS.includes('deploy'), false);
});

test('runner dashboard renders identity, state, heartbeat, project, and recent jobs', () => {
  const model = buildRunnerDashboardModel([
    { runnerId: 'windows-1', status: 'ONLINE', lastSeenAt: '2026-09-25T10:00:00.000Z', projectId: 'daily-system' },
  ], [
    { projectId: 'daily-system', type: 'HEALTHCHECK', status: 'SUCCEEDED' },
  ]);
  assert.match(model.text, /windows-1/);
  assert.match(model.text, /ONLINE/);
  assert.match(model.text, /daily-system/);
  assert.match(model.text, /HEALTHCHECK:SUCCEEDED/);
});
