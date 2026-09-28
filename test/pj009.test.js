const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { ControlPlaneStore } = require('../src/controlPlane/store');
const { executeExcelJob } = require('../src/runner/excelOperations');
const { executeJob } = require('../src/runner/jobExecutor');
const { createDeterministicProviderProbe, probeProvider } = require('../src/providerHealth');
const { validateManagedConfig } = require('../src/operationalConfig');
const { D1ControlPlaneStore } = require('../src/controlPlane/d1Store');
const { buildTypedOperationalJob } = require('../src/telegramOpsCenter');

function config(root) {
  return { admin: { adminUserIds: ['123'], archive: { channelId: '-100123', label: 'Shared archive' } }, projects: [{ projectId: 'daily-system', environment: 'dev', repositoryPath: root, runnerBinding: 'windows-01' }], desktopProfiles: [{ id: 'Amir-Desktop-01', name: 'Amir desktop', runnerId: 'windows-01', runnerType: 'Windows Runner', excelVersion: 'Excel 365', workbookPaths: [path.join(root, 'Mirax.xlsm')], backupLocation: path.join(root, 'backups') }], excelAssets: [{ projectId: 'daily-system', assetId: 'mirax', assetName: 'Mirax', filename: 'Mirax.xlsm', manualPath: path.join(root, 'Mirax.xlsm'), backupPath: path.join(root, 'backups') }], providers: [{ providerId: 'fake', projectId: 'daily-system', name: 'Fake', serviceType: 'OTHER', health: 'UNKNOWN', status: 'ENABLED' }] };
}

test('PJ-009 operational state is durable-contract shaped and project isolated in memory', () => {
  const store = new ControlPlaneStore({ projects: [{ id: 'daily-system', environment: 'dev' }, { id: 'other-project', environment: 'dev' }] });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj009-')); const saved = store.saveOperationalConfig(config(root), 'amir', 'cfg-009');
  assert.equal(saved.ok, true); assert.equal(store.configAudit.length, 1); assert.equal(store.projectConfigs.get('daily-system').runnerBinding, 'windows-01');
  assert.equal(store.saveOperationalConfig(config(root), 'amir', 'cfg-009').duplicate, true);
  const archive = store.createArchive({ projectId: 'daily-system', artifactType: 'EXCEL_BACKUP', contentHash: 'hash' }, 'arc-op');
  assert.equal(store.createArchive({ projectId: 'other-project', artifactType: 'EXCEL_BACKUP', contentHash: 'hash' }, 'arc-op').duplicate, true);
  assert.equal(store.updateArchiveRestore(archive.archive.archiveId, 'other-project', 'RESTORED').ok, false);
});

test('PJ-009 Excel health, snapshot, backup, and source immutability', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj009-excel-')); const source = path.join(root, 'Mirax.xlsm'); const backup = path.join(root, 'backups'); fs.writeFileSync(source, 'fixture workbook'); const before = fs.readFileSync(source, 'utf8');
  const profile = { excelAssets: { mirax: { manualPath: source, backupPath: backup } }, backupLocation: backup, runnerId: 'windows-01' };
  const job = { id: 'job-1', type: 'EXCEL_SNAPSHOT', projectId: 'daily-system', idempotencyKey: 'snap-1', payload: { assetId: 'mirax' } };
  const result = await executeExcelJob(job, profile); assert.equal(result.ok, true); assert.equal(result.result.verificationStatus, 'VERIFIED'); assert.equal(result.result.vbaExecuted, false); assert.equal(fs.readFileSync(source, 'utf8'), before); assert.equal(fs.existsSync(result.result.localBackupReference), true);
  const health = await executeExcelJob({ ...job, type: 'EXCEL_HEALTHCHECK', payload: { assetId: 'mirax' } }, profile); assert.equal(health.ok, true); assert.equal(health.result.pathExists, true); assert.equal(health.result.sha256.length, 64);
  const missing = await executeExcelJob({ ...job, type: 'EXCEL_HEALTHCHECK', payload: { assetId: 'missing' } }, profile); assert.equal(missing.ok, false); assert.equal(missing.diagnosticCode, 'EXCEL_ASSET_NOT_CONFIGURED');
});

test('PJ-009 reconciliation stores safe operational metadata and blocks sync publication', async () => {
  const result = await executeExcelJob({ id: 'r-1', type: 'EXCEL_RECONCILIATION', projectId: 'daily-system', payload: { reconciliationResult: { workId: 'w-1', status: 'MISMATCH', mismatchCount: 2, errorCount: 0, retryable: true, diagnostics: { studentName: 'must-not-be-copied' } } } });
  assert.equal(result.ok, false); assert.equal(result.result.status, 'MISMATCH'); assert.equal(result.result.mismatchCount, 2); assert.equal('studentName' in result.result.diagnostics, false);
  const sync = await executeExcelJob({ id: 's-1', type: 'EXCEL_SYNC', projectId: 'daily-system', payload: {} }); assert.equal(sync.ok, false); assert.equal(sync.diagnosticCode, 'EXCEL_SYNC_DISABLED');
});

test('PJ-009 provider health fake adapter updates counters and redacts limits', async () => {
  const healthy = await probeProvider({ providerId: 'fake', projectId: 'daily-system', enabled: true, requestCount: 3, errorCount: 1, limits: { apiKey: 'secret' } }, createDeterministicProviderProbe());
  assert.equal(healthy.ok, true); assert.equal(healthy.value.health, 'HEALTHY'); assert.equal(healthy.value.requestCount, 4); assert.equal(healthy.value.limits.apiKey, '[REDACTED]');
  const down = await probeProvider({ providerId: 'fake', projectId: 'daily-system' }, createDeterministicProviderProbe({ timeout: true })); assert.equal(down.ok, false); assert.equal(down.value.health, 'DOWN');
});

test('PJ-009 typed runner execution dispatches Excel without shell commands', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj009-runner-')); const source = path.join(root, 'Gozareshkar.xlsm'); fs.writeFileSync(source, 'fixture');
  const result = await executeJob({ type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'gozareshkar' } }, { profile: { excelAssets: { gozareshkar: { manualPath: source, backupPath: root } } } });
  assert.equal(result.ok, true); assert.equal(result.result.pathExists, true);
});

test('PJ-009 config rejects plaintext provider secret fields', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj009-config-')); const input = config(root); input.providers[0].apiKey = 'plaintext'; assert.equal(validateManagedConfig(input).ok, false);
});

test('PJ-009 D1 store uses additive operational statements and fails closed on malformed rows', async () => {
  const calls = [];
  const db = { prepare(sql) { calls.push(sql); return { bind() { return this; }, async all() { return { results: sql.includes('cp_operational_config') ? [{ config_json: '{bad' }] : [] }; }, async run() { return {}; } }; } };
  const store = new D1ControlPlaneStore(db, { projects: [{ id: 'daily-system', name: 'DailySystem', environment: 'dev' }] });
  await store.ready; assert.equal(store.operationalConfig, null);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj009-d1-')); const result = await store.saveOperationalConfig(config(root), 'amir', 'd1-op-1');
  assert.equal(result.ok, true); assert.ok(calls.some((sql) => sql.includes('cp_project_config'))); assert.ok(calls.some((sql) => sql.includes('cp_config_audit')));
});

test('PJ-009 Telegram operation builder emits only typed Excel jobs', () => {
  const job = buildTypedOperationalJob('excel_backup', 'daily-system', 'mirax', 'backup-1'); assert.equal(job.ok, true); assert.equal(job.job.type, 'EXCEL_BACKUP'); assert.equal(job.job.payload.assetId, 'mirax');
  assert.equal(buildTypedOperationalJob('shell', 'daily-system', 'mirax', 'x').ok, false);
});
