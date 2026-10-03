const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { ControlPlaneStore } = require('../src/controlPlane/store');
const { executeJob, runCommand } = require('../src/runner/jobExecutor');
const { repoStatus, releaseEvidence, stagingHealth, ACCEPTED_SCRIPTS, acceptedScripts, validationReport } = require('../src/runner/zjOperations');
const { readiness } = require('../src/controlPlane/zjReadiness');
const { createControlPlaneHandler } = require('../src/controlPlane/handler');
const { dispatchTelegramUpdate } = require('../src/telegramApplication');

test('project registry isolates DailySystem and ZJ jobs and capabilities', () => {
  const store = new ControlPlaneStore();
  assert.equal(store.getProject('daily-system').allowedJobTypes.includes('EXCEL_HEALTHCHECK'), true);
  assert.equal(store.getProject('zj').allowedJobTypes.includes('EXCEL_HEALTHCHECK'), false);
  assert.equal(store.createJob({ projectId: 'zj', type: 'EXCEL_HEALTHCHECK' }).error, 'job_not_allowed');
  assert.equal(store.createJob({ projectId: 'daily-system', type: 'ZJ_REPO_STATUS' }).error, 'job_not_allowed');
  assert.equal(store.createJob({ projectId: 'unknown', type: 'ZJ_REPO_STATUS' }).ok, false);
  assert.equal(store.createJob({ projectId: 'constructor', type: 'ZJ_REPO_STATUS' }).ok, false);
  assert.equal(store.createJob({ projectId: 'zj', type: 'ZJ_PRODUCTION_DEPLOY' }).ok, false);
  assert.equal(store.createJob({ projectId: 'zj', type: 'ZJ_REPO_STATUS', payload: { repoPath: 'C:\\' } }).ok, false);
  assert.equal(store.createJob({ projectId: 'zj', type: 'ZJ_STAGING_HEALTHCHECK', payload: { url: 'https://example.com/' } }).ok, false);
});

test('accepted validation scripts reject production commands and npm lifecycle injection', () => {
  assert.equal(acceptedScripts({ scripts: { ...ACCEPTED_SCRIPTS } }), true);
  for (const command of ['wrangler deploy --name zj', 'wrangler d1 execute zj-db --command DELETE', 'curl production/webhook', 'powershell whoami']) {
    assert.equal(acceptedScripts({ scripts: { ...ACCEPTED_SCRIPTS, build: command } }), false);
  }
  assert.equal(acceptedScripts({ scripts: { ...ACCEPTED_SCRIPTS, pretest: 'wrangler deploy' } }), false);
});

test('project bearer tokens cannot submit or read another project data', async () => {
  const store = new ControlPlaneStore();
  store.heartbeat({ runnerId: 'ds', projectId: 'daily-system' }); store.heartbeat({ runnerId: 'zj', projectId: 'zj' });
  const handler = createControlPlaneHandler({ store, tokens: { projectTokens: { 'daily-system': 'fixture-daily', zj: 'fixture-zj' } } });
  for (const [token, projectId, type] of [['fixture-daily', 'zj', 'ZJ_REPO_STATUS'], ['fixture-zj', 'daily-system', 'GIT_STATUS']]) {
    assert.equal((await handler(new Request('https://pj.test/api/v1/jobs', { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify({ projectId, type }) }))).status, 403);
  }
  const status = await (await handler(new Request('https://pj.test/api/v1/status', { headers: { authorization: 'Bearer fixture-zj' } }))).json();
  assert.deepEqual(status.runners.map((row) => row.projectId), ['zj']);
  assert.deepEqual(status.projects.map((row) => row.id), ['zj']);
});

test('readiness does not reuse validation from another commit or hide a pending run', () => {
  const jobs = [{ projectId: 'zj', type: 'ZJ_REPO_STATUS', status: 'SUCCEEDED', updatedAt: '2026-10-03', result: { state: 'CLEAN_SYNCED', head: 'new' } }, { projectId: 'zj', type: 'ZJ_LOCAL_VALIDATION', status: 'SUCCEEDED', updatedAt: '2026-10-02', result: { commit: 'old', commands: [{ commandId: 'tests', status: 'PASS' }] } }];
  assert.equal(readiness(jobs).gates.LOCAL_TESTS, 'UNKNOWN');
  jobs.push({ projectId: 'zj', type: 'ZJ_LOCAL_VALIDATION', status: 'PENDING', updatedAt: '2026-10-04' });
  assert.equal(readiness(jobs).gates.LOCAL_TESTS, 'PENDING');
});

test('validation result model reports exact totals while retaining unknown domain attribution', () => {
  const model = validationReport({ success: true, numTotalTests: 10, numPassedTests: 10, numFailedTests: 0, testResults: [{}, {}] }, 'fixture', 100);
  assert.equal(model.testFileCount, 2); assert.equal(model.testCount, 10); assert.equal(model.status, 'PASS');
  assert.equal(model.rcMatrix.every((row) => row.status === 'UNKNOWN'), true);
  assert.equal(validationReport(null, 'fixture', 0).status, 'UNKNOWN');
});

test('cross-project result, stale attempt, and wrong runner are rejected', () => {
  const store = new ControlPlaneStore();
  const created = store.createJob({ projectId: 'zj', type: 'ZJ_REPO_STATUS' });
  const claimed = store.claimJob('zj-runner', 'zj');
  assert.equal(claimed.id, created.job.id);
  assert.equal(store.resultJob(claimed.id, 'zj-runner', { projectId: 'daily-system', attemptCount: 1 }).status, 403);
  assert.equal(store.resultJob(claimed.id, 'zj-runner', { attemptCount: 1 }).status, 403);
  assert.equal(store.resultJob(claimed.id, 'zj-runner', { projectId: 'zj', attemptCount: 1, result: { projectId: 'daily-system' } }).status, 403);
  assert.equal(store.resultJob(claimed.id, 'other-runner', { projectId: 'zj', attemptCount: 1 }).status, 409);
  assert.equal(store.resultJob(claimed.id, 'zj-runner', { projectId: 'zj', attemptCount: 0 }).status, 409);
  assert.equal(store.resultJob(claimed.id, 'zj-runner', { projectId: 'zj', attemptCount: 1, result: { state: 'CLEAN_SYNCED' } }).ok, true);
  assert.equal(store.resultJob(claimed.id, 'other-runner', { projectId: 'zj', attemptCount: 1 }).status, 409);
});

test('unknown and disabled projects cannot receive runner heartbeats or jobs', () => {
  const store = new ControlPlaneStore({ zjEnabled: false });
  assert.equal(store.heartbeat({ runnerId: 'unknown-runner', projectId: 'unknown' }).error, 'unknown projectId');
  assert.equal(store.heartbeat({ runnerId: 'zj-runner', projectId: 'zj' }).error, 'project_disabled');
  assert.equal(store.createJob({ projectId: 'zj', type: 'ZJ_REPO_STATUS' }).error, 'project_disabled');
  assert.equal(store.claimJob('zj-runner', 'zj'), null);
});

test('ZJ runner refuses arbitrary payload and cross-project jobs before execution', async () => {
  let calls = 0; const runCommand = async () => { calls += 1; return { ok: true, stdout: '' }; };
  const profile = { id: 'zj', projectId: 'zj', repoPath: 'C:\\Users\\Amir\\Documents\\GitHub\\ZJ' };
  assert.equal((await executeJob({ type: 'ZJ_REPO_STATUS', projectId: 'daily-system', payload: {} }, { profile, runCommand })).ok, false);
  assert.equal((await executeJob({ type: 'ZJ_REPO_STATUS', projectId: 'zj', payload: { command: 'whoami' } }, { profile, runCommand })).ok, false);
  assert.equal(calls, 0);
});

test('missing repo and Plans return bounded unknown evidence', async () => {
  const missing = path.join(os.tmpdir(), `pj-zj-missing-${Date.now()}`);
  assert.equal((await repoStatus(async () => { throw new Error('should not run'); }, { repoPath: missing })).diagnosticCode, 'REPO_NOT_FOUND');
  assert.equal(releaseEvidence({ plansPath: missing }).diagnosticCode, 'PLANS_NOT_FOUND');
});

test('repository status distinguishes dirty, detached and tracking mismatch', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-zj-status-')); fs.mkdirSync(path.join(root, '.git'));
  const fake = (overrides = {}) => async (_file, args) => {
    const key = args.join(' ');
    const value = { 'status --porcelain=v1': '', 'branch --show-current': 'main', 'rev-parse HEAD': 'a'.repeat(40), 'rev-parse @{upstream}': 'a'.repeat(40), 'rev-list --left-right --count HEAD...@{upstream}': '0 0', 'diff --check': '', 'log -5 --format=%h %s': '' , ...overrides }[key];
    return value == null ? { ok: false, stdout: '' } : { ok: true, stdout: value };
  };
  try {
    assert.equal((await repoStatus(fake({ 'status --porcelain=v1': ' M file' }), { repoPath: root })).result.state, 'DIRTY');
    assert.equal((await repoStatus(fake({ 'branch --show-current': '' }), { repoPath: root })).result.state, 'DETACHED');
    assert.equal((await repoStatus(fake({ 'rev-parse @{upstream}': null }), { repoPath: root })).result.state, 'REMOTE_MISMATCH');
  } finally { if (path.dirname(root) === path.resolve(os.tmpdir()) && path.basename(root).startsWith('pj-zj-status-')) fs.rmSync(root, { recursive: true, force: true }); }
});

test('staging check rejects production and arbitrary URLs without fetching', async () => {
  let calls = 0; const fetchImpl = async () => { calls += 1; throw new Error('unexpected'); };
  for (const url of ['https://zj.example.workers.dev/', 'https://example.com/', 'http://zj-staging.example.workers.dev/', 'https://zj-staging.example.workers.dev/?x=1']) {
    assert.equal((await stagingHealth({ stagingHealthUrl: url }, fetchImpl)).diagnosticCode, 'STAGING_NOT_CONFIGURED');
  }
  assert.equal(calls, 0);
  assert.equal((await stagingHealth({}, fetchImpl)).result.status, 'PENDING');
});

test('staging health classifies healthy, invalid JSON, server error and timeout', async () => {
  const profile = { stagingHealthUrl: 'https://zj-staging.example.workers.dev/' };
  const healthy = await stagingHealth(profile, async (url, options) => {
    assert.equal(url.pathname, '/healthz'); assert.equal(options.method, 'GET'); assert.equal(options.redirect, 'error');
    return Response.json({ status: 'healthy', service: 'zj', privateData: 'ignored' });
  });
  assert.equal(healthy.result.status, 'HEALTHY'); assert.deepEqual(healthy.result.health, { status: 'healthy', service: 'zj' });
  const malformed = await stagingHealth(profile, async () => new Response('{'));
  assert.equal(malformed.diagnosticCode, 'REMOTE_HEALTH_BAD_RESPONSE');
  const serverError = await stagingHealth(profile, async () => new Response('{}', { status: 503 }));
  assert.equal(serverError.result.httpStatus, 503);
  const timeout = await stagingHealth(profile, async () => { const error = new Error('timed out'); error.name = 'TimeoutError'; throw error; });
  assert.equal(timeout.diagnosticCode, 'REMOTE_HEALTH_TIMEOUT');
  assert.equal((await stagingHealth(profile, async () => new Response('x'.repeat(4097)))).diagnosticCode, 'REMOTE_HEALTH_BAD_RESPONSE');
});

test('readiness retains unknown and pending distinct from pass', () => {
  const store = new ControlPlaneStore();
  const result = store.createJob({ projectId: 'zj', type: 'ZJ_RELEASE_READINESS' });
  assert.equal(result.job.status, 'SUCCEEDED');
  assert.equal(result.job.result.status, 'PENDING');
  assert.equal(result.job.result.gates.LOCAL_TESTS, 'UNKNOWN');
  assert.equal(result.job.result.gates.PRODUCTION_ISOLATION, 'PASS');
});

test('active ProjectManager paths no longer use the former checkout', () => {
  const root = path.resolve(__dirname, '..');
  for (const relative of ['wrangler.jsonc', 'docs/project-plan/PJ_CURRENT_PLAN.md', 'docs/project-memory/PROJECTMANAGER-MASTER-CONTEXT.md', 'docs/handoffs/PJ-012/CLOUDFLARE-RUNBOOK.md']) {
    assert.equal(fs.readFileSync(path.join(root, relative), 'utf8').includes('cloned\\ProjectManager'), false, relative);
  }
});

test('Telegram admin can navigate projects and queue only typed ZJ actions', async () => {
  const store = new ControlPlaneStore(); const env = { TELEGRAM_ADMIN_USER_IDS: '123' };
  const update = (text, id = 123) => ({ update_id: Math.floor(Math.random() * 1e6), message: { text, from: { id }, chat: { id, type: 'private' } } });
  const unauthorized = await dispatchTelegramUpdate(update('/projects', 999), { store, env });
  assert.equal(unauthorized.authorized, false);
  const projects = await dispatchTelegramUpdate(update('/projects'), { store, env });
  assert.match(projects.text, /DailySystem/); assert.match(projects.text, /ZJ/);
  const zj = await dispatchTelegramUpdate(update('/project zj'), { store, env });
  assert.deepEqual(zj.replyMarkup.inline_keyboard.flat().map((button) => button.callback_data), ['zj_status', 'zj_repo', 'zj_release', 'zj_readiness', 'zj_validate', 'zj_staging', 'zj_jobs']);
  const queued = await dispatchTelegramUpdate(update('/zj_repo'), { store, env });
  assert.match(queued.text, /Queued ZJ_REPO_STATUS/);
  assert.equal(store.listJobs('zj').length, 1);
});

test('runner bounds output and terminates a timed-out child', async () => {
  const large = await runCommand(process.execPath, ['-e', "process.stdout.write('x'.repeat(20000))"], os.tmpdir(), { timeoutMs: 10_000 });
  assert.equal(large.ok, true); assert.ok(large.stdout.length <= 12000);
  const slow = await runCommand(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], os.tmpdir(), { timeoutMs: 1000 });
  assert.equal(slow.timedOut, true);
});
