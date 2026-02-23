const test = require('node:test');
const assert = require('node:assert/strict');
const { runDeploy } = require('../src/deploy/workflow');
const { listDeployEvents, setDeploySettings } = require('../src/deploy/deployStore');
const { saveJson } = require('../configStore');
const { createOpsTimelineStore } = require('../src/opsTimeline');
const { listLogs } = require('../src/logsHubStore');

test('deploy workflow creates event with refId and timeline entry', async () => {
  await saveJson('deploy_events_v1', []);
  await saveJson('deploy_settings_v1', {});
  await saveJson('ops_event_log', []);
  const timeline = createOpsTimelineStore();
  await setDeploySettings('p1', { provider: 'render', healthMode: 'off' });
  const result = await runDeploy({
    project: { id: 'p1', deployHookUrl: 'https://render/hook' },
    timeline,
    deps: { requestUrl: async () => ({ status: 200, body: 'ok' }) },
  });
  assert.equal(result.ok, true);
  assert.match(result.deploy.refId, /^DEPLOY-/);
  const events = await listDeployEvents('p1');
  assert.equal(events.length, 1);
  const t = timeline.query({ scope: 'project', projectId: 'p1' });
  assert.equal(t.items[0].type, 'DEPLOY_LIVE');
});

test('deploy workflow marks failed on health timeout and emits DEPLOY_ERROR log', async () => {
  await saveJson('deploy_events_v1', []);
  await saveJson('deploy_settings_v1', {});
  await saveJson('ops_event_log', []);
  await setDeploySettings('p2', { provider: 'render', healthMode: 'path', healthTimeoutSec: 1, healthPollIntervalSec: 1, healthPath: '/healthz' });
  const result = await runDeploy({
    project: { id: 'p2', deployHookUrl: 'https://render/hook', renderServiceUrl: 'https://svc.example.com' },
    deps: { requestUrl: async (method) => (method === 'POST' ? { status: 200, body: 'ok' } : { status: 500, body: 'bad' }) },
  });
  assert.equal(result.ok, false);
  assert.equal(result.deploy.status, 'failed');
  const logs = await listLogs({ projectId: 'p2', category: 'DEPLOY_ERROR' });
  assert.equal(logs.length, 1);
});
