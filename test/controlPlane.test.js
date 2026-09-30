const test = require('node:test');
const assert = require('node:assert/strict');
const { ControlPlaneStore } = require('../src/controlPlane/store');
const { createAuthenticator } = require('../src/controlPlane/auth');
const { createControlPlaneHandler } = require('../src/controlPlane/handler');

function setup() {
  let now = Date.parse('2026-09-24T10:00:00.000Z');
  const store = new ControlPlaneStore({ now: () => now, runnerStaleMs: 1000, runnerOfflineMs: 3000, projects: [
    { id: 'daily-system', key: 'daily-system', name: 'DailySystem', environment: 'dev' },
    { id: 'other-project', key: 'other-project', name: 'Other Project', environment: 'dev' },
  ] });
  const handler = createControlPlaneHandler({ store, authenticator: createAuthenticator({ adminToken: 'admin-secret', runnerTokens: { r1: { token: 'runner-secret', project: 'daily-system' } }, projectTokens: { 'daily-system': 'project-secret' } }) });
  return { store, handler, advance: (ms) => { now += ms; } };
}
async function call(handler, path, options = {}) {
  return handler(new Request(`https://control.test${path}`, { ...options, headers: { 'content-type': 'application/json', ...(options.headers || {}) } }));
}
async function body(response) { return response.json(); }

test('operational event auth, validation, redaction, and idempotency', async () => {
  const { handler } = setup();
  const event = { schemaVersion: 1, eventId: 'evt-1', project: 'daily-system', environment: 'dev', severity: 'ERROR', category: 'D1_ERROR', timestamp: '2026-09-24T09:59:00.000Z', message: 'D1 failed', context: { password: 'do-not-store', query: 'select 1', correlationId: 'corr-1' }, source: 'daily-system' };
  assert.equal((await call(handler, '/api/v1/ops/events', { method: 'POST', body: JSON.stringify(event) })).status, 401);
  const missingSchema = { ...event, schemaVersion: undefined, eventId: 'evt-missing-schema' };
  assert.equal((await call(handler, '/api/v1/ops/events', { method: 'POST', headers: { authorization: 'Bearer project-secret' }, body: JSON.stringify(missingSchema) })).status, 400);
  const first = await call(handler, '/api/v1/ops/events', { method: 'POST', headers: { authorization: 'Bearer project-secret' }, body: JSON.stringify(event) });
  assert.equal(first.status, 200); const firstBody = await body(first); assert.equal(firstBody.event.schemaVersion, 1); assert.equal(firstBody.event.correlationId, 'corr-1'); assert.equal(firstBody.event.context.password, '[REDACTED]');
  const duplicate = await call(handler, '/api/v1/ops/events', { method: 'POST', headers: { authorization: 'Bearer project-secret' }, body: JSON.stringify(event) });
  assert.equal((await body(duplicate)).duplicate, true);
});

test('job creation, project scope, lease ownership, and duplicate results', async () => {
  const { handler } = setup();
  const denied = await call(handler, '/api/v1/jobs', { method: 'POST', headers: { authorization: 'Bearer project-secret' }, body: JSON.stringify({ projectId: 'other', type: 'GIT_STATUS' }) });
  assert.equal(denied.status, 403);
  const created = await call(handler, '/api/v1/jobs', { method: 'POST', headers: { authorization: 'Bearer admin-secret' }, body: JSON.stringify({ projectId: 'daily-system', type: 'GIT_STATUS', idempotencyKey: 'git-1' }) });
  const job = (await body(created)).job; assert.equal(job.status, 'PENDING'); assert.equal(job.risk, 'READ_ONLY');
  const duplicate = await call(handler, '/api/v1/jobs', { method: 'POST', headers: { authorization: 'Bearer admin-secret' }, body: JSON.stringify({ projectId: 'daily-system', type: 'GIT_STATUS', idempotencyKey: 'git-1' }) });
  assert.equal((await body(duplicate)).duplicate, true);
  const claim = await call(handler, '/api/v1/jobs/claim', { method: 'POST', headers: { authorization: 'Bearer runner-secret' }, body: JSON.stringify({ projectId: 'daily-system' }) });
  const claimed = (await body(claim)).job; assert.equal(claimed.leaseOwner, 'r1');
  const wrong = await call(handler, `/api/v1/jobs/${claimed.id}/result`, { method: 'POST', headers: { authorization: 'Bearer runner-secret' }, body: JSON.stringify({ ok: true, attemptCount: claimed.attemptCount, result: { status: 'clean' } }) });
  assert.equal(wrong.status, 200);
  const duplicateResult = await call(handler, `/api/v1/jobs/${claimed.id}/result`, { method: 'POST', headers: { authorization: 'Bearer runner-secret' }, body: JSON.stringify({ ok: true, attemptCount: claimed.attemptCount }) });
  assert.equal((await body(duplicateResult)).duplicate, true);
});

test('runner heartbeat transitions offline and recovers without duplicate claims', async () => {
  const { handler, store, advance } = setup();
  const heartbeat = await call(handler, '/api/v1/runners/heartbeat', { method: 'POST', headers: { authorization: 'Bearer runner-secret' }, body: JSON.stringify({ projectId: 'daily-system', capabilities: ['git'] }) });
  assert.equal((await body(heartbeat)).runner.status, 'ONLINE');
  advance(4000); store.refreshRunnerStates(); assert.equal(store.status().runners[0].status, 'OFFLINE');
  const recovered = await call(handler, '/api/v1/runners/heartbeat', { method: 'POST', headers: { authorization: 'Bearer runner-secret' }, body: JSON.stringify({ projectId: 'daily-system' }) });
  assert.equal((await body(recovered)).runner.status, 'ONLINE');
  assert.equal(store.status().latestEvents.filter((event) => event.category === 'RUNNER_RECOVERED').length, 1);
});

test('runner identity cannot cross its project boundary', async () => {
  const { handler } = setup();
  const heartbeat = await call(handler, '/api/v1/runners/heartbeat', { method: 'POST', headers: { authorization: 'Bearer runner-secret' }, body: JSON.stringify({ projectId: 'other-project' }) });
  assert.equal(heartbeat.status, 403);
  const created = await call(handler, '/api/v1/jobs', { method: 'POST', headers: { authorization: 'Bearer admin-secret' }, body: JSON.stringify({ projectId: 'other-project', type: 'GIT_STATUS' }) });
  assert.equal(created.status, 200);
  const claim = await call(handler, '/api/v1/jobs/claim', { method: 'POST', headers: { authorization: 'Bearer runner-secret' }, body: JSON.stringify({ projectId: 'other-project' }) });
  assert.equal((await body(claim)).job, null);
});
