const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { execFileSync } = require('node:child_process');
const { Miniflare } = require('miniflare');
const { D1ControlPlaneStore } = require('../src/controlPlane/d1Store');
const { dispatchTelegramUpdate } = require('../src/telegramApplication');
const { configureWebhook } = require('../tools/telegram-webhook');
const { RunnerClient } = require('../src/runner/client');
const { runOnce } = require('../src/runner');
let mf, db, buildRoot;
const delivered = [];
let failDelivery = false;
let editFailure = null;
const secret = 'fixture-webhook-secret';
function update(id, text = '/status', sender = 123) { return { update_id: id, message: { from: { id: sender }, chat: { id: sender, type: 'private' }, text } }; }
function webhook(body, supplied = secret, headers = {}) { return mf.dispatchFetch('https://pj.invalid/telegram/webhook', { method: 'POST', headers: { 'content-type': 'application/json', ...(supplied == null ? {} : { 'X-Telegram-Bot-Api-Secret-Token': supplied }), ...headers }, body: typeof body === 'string' ? body : JSON.stringify(body) }); }
async function store() { const value = new D1ControlPlaneStore(db); await value.ready; return value; }
before(async () => {
  buildRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'pj012-build-'));
  execFileSync(process.execPath, [path.resolve(__dirname, '../node_modules/wrangler/bin/wrangler.js'), 'deploy', '--dry-run', '--outdir', buildRoot], { cwd: path.resolve(__dirname, '..'), windowsHide: true, stdio: 'pipe', env: { ...process.env, WRANGLER_SEND_METRICS: 'false' }, timeout: 60000 });
  mf = new Miniflare({ modules: true, modulesRoot: buildRoot, scriptPath: path.join(buildRoot, 'entry.js'), compatibilityDate: '2026-08-01', compatibilityFlags: ['nodejs_compat'], cf: false, d1Databases: { CONTROL_PLANE_DB: 'test-pj012' }, bindings: { TELEGRAM_BOT_TOKEN: 'fixture-token', TELEGRAM_WEBHOOK_SECRET: secret, TELEGRAM_ADMIN_USER_IDS: '123', PG_CONTROL_PLANE_ADMIN_TOKEN: 'fixture-admin', PG_RUNNER_TOKENS_JSON: JSON.stringify({ 'windows-01': { token: 'fixture-runner', project: 'daily-system' } }), LEGACY_CONFIG_DB_ENABLED: 'false' }, outboundService: async (request) => {
    if (failDelivery) return Response.json({ ok:false },{status:503});
    const method = new URL(request.url).pathname.split('/').pop();
    delivered.push({ method, body: await request.json() });
    if (method === 'editMessageText' && editFailure) return Response.json({ ok: false, error_code: editFailure.code, description: editFailure.description }, { status: editFailure.code });
    return Response.json({ ok: true, result: true });
  } });
  db = await mf.getD1Database('CONTROL_PLANE_DB');
  for (const file of fs.readdirSync(path.resolve(__dirname, '../migrations/d1')).filter((name) => name.endsWith('.sql')).sort()) {
    const sql = fs.readFileSync(path.resolve(__dirname, '../migrations/d1', file), 'utf8').replace(/--[^\n]*/g, '');
    for (const statement of sql.split(';').filter((part) => part.trim())) await db.prepare(statement).run();
  }
});
after(async () => { if (mf) await mf.dispose(); if (buildRoot) fs.rmSync(buildRoot, { recursive: true, force: true }); });


test('Cloudflare bundle starts on real workerd/D1 without any legacy DSN or warmup module', async () => {
  const response = await mf.dispatchFetch('https://pj.invalid/health');
  assert.equal(response.status, 200); assert.equal((await response.json()).runtime, 'cloudflare');
  const bundle = fs.readFileSync(path.join(buildRoot, 'entry.js'), 'utf8');
  for (const prohibited of ['startConfigDbWarmup', 'DATABASE_URL_PM', 'PATH_APPLIER_CONFIG_DSN', 'child_process', 'Excel.Application']) assert.equal(bundle.includes(prohibited), false, prohibited);
  assert.equal((await db.prepare("SELECT COUNT(*) AS count FROM cp_events WHERE category LIKE '%DB%' ").first()).count, 0);
});
test('Webhook rejects invalid and missing secret before parsing body', async () => {
  assert.equal((await webhook('not-json', 'wrong')).status, 401);
  assert.equal((await webhook(update(1), null)).status, 401);
});
test('Webhook validates update shape and bounds actual streamed request size', async () => {
  assert.equal((await webhook('{')).status, 400);
  assert.equal((await webhook({ update_id: 2 })).status, 400);
  assert.equal((await webhook('x'.repeat(32769))).status, 413);
});
test('Authorized webhook uses durable replay protection across fresh request stores', async () => {
  const beforeCount = delivered.length;
  assert.equal((await webhook(update(3))).status, 200);
  assert.equal((await webhook(update(3))).status, 200);
  assert.equal(delivered.length - beforeCount, 1);
  assert.equal((await db.prepare('SELECT status FROM cp_telegram_updates WHERE update_id=3').first()).status, 'DONE');
});
test('Unauthorized user and group context cannot read admin data or create jobs', async () => {
  const beforeCount = delivered.length;
  await webhook(update(4, '/health daily-system', 456));
  const group = update(5, '/health daily-system'); group.message.chat.type = 'group'; group.message.chat.id = -123;
  await webhook(group); assert.equal(delivered.length, beforeCount);
  assert.equal((await db.prepare('SELECT COUNT(*) AS count FROM cp_jobs').first()).count, 0);
});
test('Callbacks route through shared Ops Center models and explicit read-only action allowlist', async () => {
  const callback = { update_id: 6, callback_query: { id: 'callback-fixture', from: { id: 123 }, message: { chat: { id: 123, type: 'private' } }, data: 'admin' } };
  const before = delivered.length;
  assert.equal((await webhook(callback)).status, 200);
  assert.deepEqual(delivered.slice(before).map((call) => call.method), ['answerCallbackQuery', 'sendMessage']); // No editable message ID in this fixture.
  const application = await dispatchTelegramUpdate(update(7, '/shell rm -rf'), { store: await store(), env: { TELEGRAM_ADMIN_USER_IDS: '123' } });
  assert.match(application.text, /disabled/); assert.equal((await db.prepare('SELECT COUNT(*) AS count FROM cp_jobs').first()).count, 0);
});
test('ProjectManager callback navigation edits one menu and acknowledges without duplicate messages', async () => {
  const initial = delivered.length;
  assert.equal((await webhook(update(101, '/projects'))).status, 200);
  assert.deepEqual(delivered.slice(initial).map((call) => call.method), ['sendMessage']);
  const callback = (id, data) => ({ update_id: id, callback_query: { id: `callback-${id}`, from: { id: 123 }, message: { message_id: 77, chat: { id: 123, type: 'private' } }, data } });
  for (const [id, data, label] of [[102, 'project zj', '🎓 ZJ'], [103, 'projects', '📁 Projects'], [104, 'project daily-system', '📘 DailySystem'], [105, 'projects', '📁 Projects'], [106, 'start', 'ProjectManager Admin']]) {
    const before = delivered.length;
    assert.equal((await webhook(callback(id, data))).status, 200);
    assert.deepEqual(delivered.slice(before).map((call) => call.method), ['answerCallbackQuery', 'editMessageText']);
    assert.equal(delivered.at(-1).body.message_id, 77);
    assert.match(delivered.at(-1).body.text, new RegExp(label));
  }
  const unauthorized = delivered.length;
  assert.equal((await webhook({ update_id: 107, callback_query: { id: 'unauthorized', from: { id: 456 }, message: { message_id: 77, chat: { id: 456, type: 'private' } }, data: 'project zj' } })).status, 200);
  assert.deepEqual(delivered.slice(unauthorized).map((call) => call.method), ['answerCallbackQuery']);
});
test('Telegram edit edge cases distinguish unchanged, impossible and unexpected failures', async () => {
  const callback = (id) => ({ update_id: id, callback_query: { id: `callback-${id}`, from: { id: 123 }, message: { message_id: 78, chat: { id: 123, type: 'private' } }, data: 'projects' } });
  try {
    editFailure = { code: 400, description: 'Bad Request: message is not modified' };
    let before = delivered.length;
    assert.equal((await webhook(callback(108))).status, 200);
    assert.deepEqual(delivered.slice(before).map((call) => call.method), ['answerCallbackQuery', 'editMessageText']);
    editFailure = { code: 400, description: 'Bad Request: message to edit not found' };
    before = delivered.length;
    assert.equal((await webhook(callback(109))).status, 200);
    assert.deepEqual(delivered.slice(before).map((call) => call.method), ['answerCallbackQuery', 'editMessageText', 'sendMessage']);
    editFailure = { code: 403, description: 'Forbidden: bot was blocked by the user' };
    before = delivered.length;
    assert.equal((await webhook(callback(110))).status, 503);
    assert.deepEqual(delivered.slice(before).map((call) => call.method), ['answerCallbackQuery', 'editMessageText']);
  } finally { editFailure = null; }
});
test('Telegram -> Worker/D1 -> authenticated local Runner -> typed result/status', async () => {
  await webhook(update(8, '/health daily-system'));
  const client = new RunnerClient({ baseUrl: 'https://pj.invalid', token: 'fixture-runner', runnerId: 'windows-01', projectId: 'daily-system', fetch: (url, init) => mf.dispatchFetch(url, init) });
  assert.equal((await client.heartbeat()).ok, true);
  const result = await runOnce(client, { profile: { id: 'daily-system', projectId: 'daily-system', repoPath: process.cwd() } });
  assert.equal(result.ok, true);
  const row = await db.prepare("SELECT * FROM cp_jobs WHERE idempotency_key='telegram:8'").first();
  assert.equal(row.status, 'SUCCEEDED'); assert.equal(JSON.parse(row.result_json).status, 'available');
  await webhook(update(9, `/job ${row.id}`)); assert.match(delivered.at(-1).body.text, /SUCCEEDED/);
  assert.equal((await mf.dispatchFetch('https://pj.invalid/api/control-plane/status', { headers: { authorization: 'Bearer fixture-admin' } })).status, 200);
});
test('D1 job claims are atomic across independent store instances', async () => {
  const a = await store(), b = await store();
  const created = await a.createJob({ projectId: 'daily-system', type: 'PROJECT_STATUS', idempotencyKey: 'atomic-claim' });
  const claims = await Promise.all([a.claimJob('one', 'daily-system'), b.claimJob('two', 'daily-system')]);
  assert.equal(claims.filter(Boolean).length, 1); assert.equal(claims.find(Boolean).id, created.job.id);
});
test('D1 job idempotency survives concurrent creation from separate isolates', async () => {
  const a = await store(), b = await store(); const input = { projectId: 'daily-system', type: 'PROJECT_STATUS', idempotencyKey: 'concurrent-create' };
  const results = await Promise.all([a.createJob(input), b.createJob(input)]);
  assert.equal(results[0].job.id, results[1].job.id);
});
test('D1 result rejects wrong runner/stale attempt and accepts terminal duplicate only from its owner', async () => {
  const value = await store(); const claim = await value.claimJob('right', 'daily-system');
  assert.ok(claim); assert.equal((await value.resultJob(claim.id, 'wrong', { ok: true, attemptCount: claim.attemptCount })).ok, false);
  assert.equal((await value.resultJob(claim.id, 'right', { ok: true, attemptCount: claim.attemptCount - 1 })).ok, false);
  const done = { ok: true, attemptCount: claim.attemptCount, result: { status: 'available' } };
  assert.equal((await value.resultJob(claim.id, 'right', done)).ok, true);
  assert.equal((await (await store()).resultJob(claim.id, 'right', done)).duplicate, true);
  assert.equal((await (await store()).resultJob(claim.id, 'wrong', done)).ok, false);
});
test('D1 retryable failure returns a job to pending and increments a new lease attempt', async () => {
  const value = await store(); const created = await value.createJob({ projectId: 'daily-system', type: 'HEALTHCHECK', idempotencyKey: 'retry-fixture' });
  const claim = await value.claimJob('retry-runner', 'daily-system'); assert.equal(claim.id, created.job.id);
  assert.equal((await value.resultJob(claim.id, 'retry-runner', { ok: false, retryable: true, attemptCount: claim.attemptCount })).job.status, 'PENDING');
  assert.equal((await value.claimJob('retry-runner', 'daily-system')).attemptCount, claim.attemptCount + 1);
});
test('D1 persists incident/event idempotency, config metadata and audit through rehydration', async () => {
  const value = await store();
  const event = { schemaVersion: 1, eventId: 'pj012-persist-event', project: 'daily-system', environment: 'dev', severity: 'ERROR', category: 'EXCEL_OPEN_FAILED', timestamp: new Date().toISOString(), message: 'Safe diagnostic failure', source: 'test' };
  assert.equal((await value.ingestEvent(event)).ok, true); assert.equal((await (await store()).ingestEvent(event)).duplicate, true);
  const config = { admin: { adminUserIds: ['123'], archive: { channelId: '-100123', label: 'Shared archive' } }, projects: [{ projectId: 'daily-system', environment: 'dev', repositoryPath: process.cwd(), runnerBinding: 'windows-01' }], desktopProfiles: [], excelAssets: [], providers: [] };
  assert.equal((await value.saveOperationalConfig(config, 'fixture-admin', 'pj012-config')).ok, true);
  const hydrated = await store(); assert.equal(hydrated.configAudit.length, 1); assert.equal(hydrated.operationalConfig.projects[0].projectId, 'daily-system'); assert.ok(hydrated.alerts.size);
});
test('Project scope and disabled authoritative Excel sync remain enforced', async () => {
  assert.equal((await (await store()).createJob({ projectId: 'unknown', type: 'HEALTHCHECK' })).ok, false);
  const { executeJob } = require('../src/runner/jobExecutor');
  assert.equal((await executeJob({ type: 'EXCEL_SYNC', projectId: 'daily-system', payload: {} }, { profile: { id: 'daily-system', repoPath: process.cwd() } })).ok, false);
});
test('Webhook setup sends secrets only to Telegram and returns allowlisted safe verification', async () => {
  const calls = []; const fetchImpl = async (url, options) => { calls.push({ url, body: JSON.parse(options.body) }); return Response.json({ ok: true, result: url.endsWith('getWebhookInfo') ? { url: 'https://pj.example/telegram/webhook', pending_update_count: 0 } : true }); };
  const output = await configureWebhook({ mode: 'set', origin: 'https://pj.example', token: 'private-fixture', secret: 'private-secret', fetchImpl });
  assert.equal(output.webhookMatchesExpected, true); assert.equal(calls[0].body.secret_token, 'private-secret'); assert.equal(JSON.stringify(output).includes('private'), false);
  await assert.rejects(configureWebhook({ mode: 'set', origin: 'http://unsafe.example', token: 'fixture', secret: 'fixture', fetchImpl }));
});

test('Telegram delivery failure retries the same durable update without duplicating its typed job', async () => {
  failDelivery = true;
  try { assert.equal((await webhook(update(100, '/project_status daily-system'))).status,503); }
  finally { failDelivery = false; }
  assert.equal((await webhook(update(100, '/project_status daily-system'))).status,200);
  assert.equal((await db.prepare("SELECT COUNT(*) AS count FROM cp_jobs WHERE idempotency_key='telegram:100'").first()).count,1);
});
test('Control Plane config requires admin auth and bounds POST bodies', async () => {
  const forbidden = await mf.dispatchFetch('https://pj.invalid/api/v1/config',{headers:{authorization:'Bearer fixture-runner'}});
  assert.equal(forbidden.status,403);
  const config = await mf.dispatchFetch('https://pj.invalid/api/v1/config',{headers:{authorization:'Bearer fixture-admin'}});
  assert.equal(config.status,200); assert.equal((await config.json()).config.admin.adminUserIds[0],'123');
  const oversized = await mf.dispatchFetch('https://pj.invalid/api/v1/jobs',{method:'POST',headers:{authorization:'Bearer fixture-admin'},body:'x'.repeat(32769)});
  assert.equal(oversized.status,413);
});

test('D1 persists ZJ registry, scoped jobs, fenced completion and readiness', async () => {
  const cp = await store();
  assert.equal(cp.getProject('zj').allowedJobTypes.includes('ZJ_REPO_STATUS'), true);
  const created = await cp.createJob({ projectId: 'zj', type: 'ZJ_REPO_STATUS', payload: {}, idempotencyKey: 'zj-d1-fixture' }, 'test');
  assert.equal(created.ok, true);
  const claimed = await cp.claimJob('zj-fixture-runner', 'zj');
  assert.equal(claimed.id, created.job.id);
  assert.equal((await cp.resultJob(claimed.id, 'zj-fixture-runner', { projectId: 'daily-system', attemptCount: claimed.attemptCount })).status, 403);
  const accepted = await cp.resultJob(claimed.id, 'zj-fixture-runner', { projectId: 'zj', attemptCount: claimed.attemptCount, ok: true, result: { projectId: 'zj', state: 'CLEAN_SYNCED', head: 'a'.repeat(40) } });
  assert.equal(accepted.ok, true);
  const fresh = await store();
  assert.equal((await fresh.getJob(claimed.id)).result.state, 'CLEAN_SYNCED');
  const readiness = await fresh.createJob({ projectId: 'zj', type: 'ZJ_RELEASE_READINESS', payload: {} }, 'test');
  assert.equal(readiness.job.status, 'SUCCEEDED');
  assert.equal((await (await store()).getJob(readiness.job.id)).result.gates.LOCAL_REPO, 'PASS');
});
