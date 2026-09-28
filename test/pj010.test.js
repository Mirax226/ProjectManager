const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const { createDailySystemAdapter, createFakeDailySystemAdapter } = require('../src/dailySystemAdapter');
const { ControlPlaneStore } = require('../src/controlPlane/store');
const { executeExcelJob } = require('../src/runner/excelOperations');
const { executeJob } = require('../src/runner/jobExecutor');
const { buildProjectManagerAdminModel } = require('../src/telegramOpsCenter');

function response(status, body, json = true) { return { ok: status >= 200 && status < 300, status, async json() { if (!json) throw new Error('invalid json'); return body; } }; }

test('PJ-010 DailySystem adapter succeeds with correlation propagation and redaction', async () => {
  let request;
  const adapter = createDailySystemAdapter({ baseUrl: 'https://daily.test', token: 'secret-token', fetch: async (url, options) => { request = { url, options }; return response(200, { status: 'HEALTHY', service: 'DailySystem', diagnostics: { code: 'OK', studentName: 'private' }, token: 'must-not-return' }); } });
  const result = await adapter.health('corr-health');
  assert.equal(result.ok, true); assert.equal(result.correlationId, 'corr-health'); assert.equal(request.options.headers['x-correlation-id'], 'corr-health'); assert.match(request.options.headers.authorization, /^Bearer /); assert.equal(result.value.diagnostics.studentName, undefined); assert.equal(result.value.token, undefined);
});

test('PJ-010 DailySystem adapter classifies timeout, unavailable, unauthorized, and malformed responses', async () => {
  const timeout = createDailySystemAdapter({ baseUrl: 'https://daily.test', timeoutMs: 100, fetch: (_url, options) => new Promise((resolve, reject) => { options.signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { name: 'AbortError' }))); }) });
  const timed = await timeout.health('corr-timeout'); assert.equal(timed.error.code, 'TIMEOUT'); assert.equal(timed.error.retryable, true);
  const unavailable = createDailySystemAdapter({ baseUrl: 'https://daily.test', fetch: async () => { throw new TypeError('network down'); } });
  const down = await unavailable.health('corr-down'); assert.equal(down.error.code, 'UNAVAILABLE'); assert.equal(down.error.retryable, true);
  const unauthorized = createDailySystemAdapter({ baseUrl: 'https://daily.test', fetch: async () => response(401, {}) });
  const denied = await unauthorized.health('corr-denied'); assert.equal(denied.error.code, 'UNAUTHORIZED'); assert.equal(denied.error.retryable, false);
  const malformed = createDailySystemAdapter({ baseUrl: 'https://daily.test', fetch: async () => response(200, null, false) });
  const bad = await malformed.health('corr-bad'); assert.equal(bad.error.code, 'MALFORMED_RESPONSE'); assert.equal(bad.error.retryable, false);
});

test('PJ-010 adapter bounds response bodies and sanitizes transport errors', async () => {
  const oversized = createDailySystemAdapter({ baseUrl: 'https://daily.test', fetch: async () => ({ ok: true, status: 200, async text() { return JSON.stringify({ status: 'HEALTHY', diagnostics: { message: 'x'.repeat(40_000) } }); } }) });
  const result = await oversized.health('corr-large'); assert.equal(result.error.code, 'MALFORMED_RESPONSE'); assert.doesNotMatch(result.error.message, /x{100}/);
  const failed = createDailySystemAdapter({ baseUrl: 'https://daily.test', fetch: async () => { throw new Error('authorization=super-secret'); } });
  const error = await failed.health('corr-secret'); assert.equal(error.error.code, 'HTTP_ERROR'); assert.match(error.error.message, /\[REDACTED\]/); assert.doesNotMatch(error.error.message, /super-secret/);
});

test('PJ-010 fake adapter is deterministic and reconciliation stays operational-only', async () => {
  const fake = createFakeDailySystemAdapter({ excelReconciliation: { ok: true, operation: 'excelReconciliation', correlationId: 'corr-r', value: { workId: 'w-1', destination: 'mirror', status: 'MISMATCH', mismatchCount: 2, errorCount: 0, retryable: true, diagnostics: { studentName: 'private' } } } });
  const result = await fake.excelReconciliation({ workId: 'w-1' }, 'corr-r'); assert.equal(result.value.status, 'MISMATCH'); assert.equal(result.value.value, undefined); assert.equal(result.value.diagnostics.studentName, 'private');
  const store = new ControlPlaneStore();
  const evidence = store.recordDailySystemEvidence({ projectId: 'daily-system', operation: 'excelReconciliation', ...result });
  assert.equal(evidence.event.category, 'EXCEL_RECONCILIATION_ERROR'); assert.equal(store.reconciliationResults.get('w-1').diagnostics.studentName, undefined); assert.equal(store.status().openAlerts.length, 1);
  const recovery = store.recordDailySystemEvidence({ projectId: 'daily-system', operation: 'excelReconciliation', ok: true, correlationId: 'corr-r2', value: { workId: 'w-1', destination: 'mirror', status: 'MATCH', mismatchCount: 0, errorCount: 0, retryable: false, diagnostics: { message: 'verified' } } });
  assert.equal(recovery.event.category, 'EXCEL_HEALTH_RECOVERED'); assert.equal(store.status().openAlerts[0].status, 'RECOVERED');
});

test('PJ-010 evidence is idempotent and maps Excel failure to recovery', () => {
  const store = new ControlPlaneStore();
  const failed = { projectId: 'daily-system', operation: 'excelSyncDiagnostic', ok: false, correlationId: 'corr-x', error: { code: 'EXCEL_FILE_NOT_FOUND', retryable: false, message: 'path=private' } };
  const first = store.recordDailySystemEvidence(failed); const duplicate = store.recordDailySystemEvidence(failed);
  assert.equal(first.event.category, 'EXCEL_FILE_NOT_FOUND'); assert.equal(duplicate.duplicate, true); assert.equal(store.events.size, 1); assert.equal(store.status().openAlerts.length, 1);
  const recovered = store.recordDailySystemEvidence({ projectId: 'daily-system', operation: 'excelSyncDiagnostic', ok: true, correlationId: 'corr-x-recovery', value: { status: 'HEALTHY', assetId: 'mirax', diagnostics: { authorization: 'secret' } } });
  assert.equal(recovered.event.category, 'EXCEL_HEALTH_RECOVERED'); assert.equal(store.status().openAlerts[0].status, 'RECOVERED');
});

test('PJ-010 Excel health verifies fingerprint, openability, source immutability, and no VBA', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj010-excel-')); const source = path.join(root, 'Mirax.xlsm'); fs.writeFileSync(source, 'fixture'); const before = crypto.createHash('sha256').update(fs.readFileSync(source)).digest('hex');
  const profile = { projectId: 'daily-system', excelAssets: { mirax: { manualPath: source, expectedFingerprint: before, backupPath: root } } };
  const result = await executeExcelJob({ id: 'h-1', type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'mirax' } }, profile, { probeOpenability: async () => 'OPENABLE' });
  assert.equal(result.ok, true); assert.equal(result.result.openability, 'OPENABLE'); assert.equal(result.result.expectedFingerprintStatus, 'MATCH'); assert.equal(result.result.vbaExecuted, undefined); assert.equal(crypto.createHash('sha256').update(fs.readFileSync(source)).digest('hex'), before);
  const mismatch = await executeExcelJob({ id: 'h-2', type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'mirax' } }, { ...profile, excelAssets: { mirax: { manualPath: source, expectedFingerprint: '0'.repeat(64) } } });
  assert.equal(mismatch.diagnosticCode, 'EXCEL_FINGERPRINT_MISMATCH');
  const unavailable = await executeExcelJob({ id: 'h-3', type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'mirax', requireExcelDesktop: true } }, profile);
  if (process.platform !== 'win32') assert.equal(unavailable.diagnosticCode, 'EXCEL_DESKTOP_UNAVAILABLE');
});

test('PJ-010 Excel openability uses a disposable copy and records before/after hashes', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj010-disposable-')); const source = path.join(root, 'Mirax.xlsm'); fs.writeFileSync(source, 'fixture'); let probePath;
  const result = await executeExcelJob({ id: 'h-copy', type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'mirax' } }, { projectId: 'daily-system', excelAssets: { mirax: { manualPath: source } } }, { probeOpenability: async (copyPath) => { probePath = copyPath; assert.notEqual(copyPath, source); assert.equal(fs.existsSync(copyPath), true); return 'OPENABLE'; } });
  assert.equal(result.ok, true); assert.equal(result.result.disposableCopy, 'CREATED'); assert.equal(result.result.sourceUnchanged, true); assert.equal(result.result.sourceHashBefore, result.result.sourceHashAfter); assert.equal(fs.existsSync(probePath), false);
});

test('PJ-010 runner preserves project binding and EXCEL_SYNC disabled', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj010-runner-')); const source = path.join(root, 'Gozareshkar.xlsm'); fs.writeFileSync(source, 'fixture');
  const ok = await executeJob({ type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'gozareshkar' } }, { profile: { projectId: 'daily-system', excelAssets: { gozareshkar: { manualPath: source } } } }); assert.equal(ok.ok, true);
  const wrong = await executeExcelJob({ type: 'EXCEL_HEALTHCHECK', projectId: 'other-project', payload: { assetId: 'gozareshkar' } }, { projectId: 'daily-system', excelAssets: { gozareshkar: { manualPath: source } } }); assert.equal(wrong.diagnosticCode, 'PROJECT_SCOPE_DENIED');
  const sync = await executeExcelJob({ type: 'EXCEL_SYNC', projectId: 'daily-system', payload: {} }, { projectId: 'daily-system' }); assert.equal(sync.diagnosticCode, 'EXCEL_SYNC_DISABLED');
});

test('PJ-010 reconciliation rejects cross-project work identity and retryable failures alert', () => {
  const store = new ControlPlaneStore({ projects: [{ id: 'daily-system', environment: 'dev' }, { id: 'other-project', environment: 'dev' }] });
  assert.equal(store.saveReconciliationResult({ projectId: 'daily-system', workId: 'shared-work', status: 'MATCH' }).ok, true);
  assert.equal(store.saveReconciliationResult({ projectId: 'other-project', workId: 'shared-work', status: 'MATCH' }).code, 'PROJECT_SCOPE_DENIED');
  const created = store.createJob({ type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', idempotencyKey: 'retry-1', payload: { assetId: 'mirax' } }); const claimed = store.claimJob('windows-01', 'daily-system');
  store.resultJob(created.job.id, 'windows-01', { ok: false, retryable: true, error: 'temporary Excel failure', eventCategory: 'EXCEL_OPEN_FAILED' });
  assert.equal(store.status().openAlerts[0].category, 'EXCEL_OPEN_FAILED'); assert.equal(store.getJob(claimed.id).status, 'PENDING');
});

test('PJ-010 Telegram admin model renders operational status without secrets', () => {
  const model = buildProjectManagerAdminModel({ dailySystemHealth: { status: 'HEALTHY', correlationId: 'corr-1' }, excelAssetsByName: { Mirax: { status: 'HEALTHY' }, Gozareshkar: { status: 'DEGRADED' } }, latestReconciliation: { status: 'MATCH', mismatchCount: 0, errorCount: 0 }, latestExcelJob: { type: 'EXCEL_HEALTHCHECK', status: 'SUCCEEDED' }, runnerStatus: 'ONLINE', excelHealth: 'HEALTHY', reconciliationStatus: 'MATCH', config: { projects: [] } });
  assert.match(model.text, /DailySystem health: HEALTHY/); assert.match(model.text, /Mirax health: HEALTHY/); assert.match(model.text, /Gozareshkar health: DEGRADED/); assert.match(model.text, /Latest reconciliation: MATCH/); assert.match(model.text, /Runner status: ONLINE/); assert.doesNotMatch(model.text, /secret|authorization|student/i);
});
