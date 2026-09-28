const test = require('node:test');
const assert = require('node:assert/strict');
const { validateAdminSettings, validateProjectSettings, validateManagedExcelAsset, OperationalConfigStore } = require('../src/operationalConfig');
const { validateDesktopProfile } = require('../src/excelAssets');
const { ArchiveStore, InMemoryArchiveProvider, TelegramArchiveAdapter, archiveCandidate } = require('../src/archive');
const { validateBackupRecord, retentionCandidate } = require('../src/backupRecords');
const { buildProjectManagerAdminModel } = require('../src/telegramOpsCenter');

const project = { projectId: 'daily-system', environment: 'dev', repositoryPath: 'C:\\Projects\\DailySystem', runnerBinding: 'windows-01' };
const asset = { projectId: 'daily-system', assetId: 'mirax', assetName: 'Mirax', filename: 'Mirax.xlsm', manualPath: 'C:\\Workbooks\\Mirax.xlsm', backupPath: 'C:\\Backups\\Mirax.xlsm' };

test('PJ-008 config validates admin IDs, archive destination, absolute paths, and command-like input', () => {
  assert.equal(validateAdminSettings({ adminUserIds: ['abc'], archive: { channelId: '-1001', label: 'Archive' } }).ok, false);
  assert.equal(validateAdminSettings({ adminUserIds: ['123'], archive: { channelId: 'not-a-channel', label: 'Archive' } }).ok, false);
  assert.equal(validateProjectSettings({ ...project, repositoryPath: 'relative\\repo' }).ok, false);
  assert.equal(validateProjectSettings({ ...project, repositoryPath: 'C:\\Projects\\DailySystem; powershell' }).ok, false);
  assert.equal(validateManagedExcelAsset({ ...asset, manualPath: 'C:\\Workbooks\\a.xlsm & whoami' }).ok, false);
});

test('PJ-008 desktop profile preserves the Amir Desktop 01 operational direction', () => {
  const profile = validateDesktopProfile({ id: 'Amir-Desktop-01', name: 'Amir desktop', runnerId: 'windows-01', runnerType: 'Windows Runner', excelVersion: 'Excel 365', workbookPaths: ['C:\\Workbooks\\Mirax.xlsm'], backupLocation: 'C:\\Backups' });
  assert.equal(profile.ok, true); assert.equal(profile.value.runnerType, 'Windows Runner'); assert.equal(profile.value.excelVersion, 'Excel 365');
});

test('PJ-008 configuration changes are attributable, idempotent, and redacted', () => {
  const events = [];
  const store = new OperationalConfigStore({ emit: (event) => events.push(event), now: () => new Date('2026-09-25T10:00:00Z') });
  const input = { admin: { adminUserIds: ['123'], archive: { channelId: '-100123', label: 'Shared private archive' } }, projects: [project], excelAssets: [asset], providers: [{ providerId: 'gemini', projectId: 'daily-system', name: 'Gemini', serviceType: 'AI', health: 'HEALTHY', status: 'ENABLED' }] };
  const first = store.update(input, 'amir', 'cfg-1');
  assert.equal(first.ok, true); assert.equal(first.event.category, 'CONFIG_CHANGED'); assert.equal(first.event.context.actor, 'amir'); assert.equal(events.length, 1);
  assert.equal(store.update(input, 'amir', 'cfg-1').duplicate, true);
  assert.doesNotMatch(JSON.stringify(store.read()), /secret|apiKey/i);
});

test('PJ-008 archive is shared but project-tagged, idempotent, hashed, and restore-scoped', async () => {
  const store = new ArchiveStore({ provider: new InMemoryArchiveProvider() });
  const input = { projectId: 'daily-system', artifactType: 'EXCEL_BACKUP', contentHash: 'source-content', safeMetadata: { token: 'never' }, telegramChannelId: '-100123' };
  const first = await store.create(input, 'archive-op-1');
  const duplicate = await store.create(input, 'archive-op-1');
  assert.equal(first.ok, true); assert.equal(duplicate.duplicate, true); assert.equal(first.record.status, 'STORED'); assert.equal(first.record.projectId, 'daily-system'); assert.equal(first.record.contentHash.length, 64); assert.doesNotMatch(JSON.stringify(first.record), /never/);
  assert.equal((await store.requestRestore(first.record.archiveId, 'other-project')).ok, false);
  assert.equal((await store.requestRestore(first.record.archiveId, 'daily-system')).record.restoreStatus, 'RESTORE_REQUESTED');
  assert.equal((await store.markRestored(first.record.archiveId, 'daily-system')).record.restoreStatus, 'RESTORED');
});

test('PJ-008 Telegram adapter is disabled by default and admin render never exposes channel IDs as secrets', async () => {
  assert.equal((await new TelegramArchiveAdapter().put({})).ok, false);
  const view = buildProjectManagerAdminModel({ config: { admin: { archive: { channelId: '-100123', label: 'Shared archive' } }, projects: [], excelAssets: [], providers: [] } });
  assert.match(view.text, /Archive destination/); assert.match(view.text, /\[configured\]/);
});

test('PJ-008 backup metadata and retention candidates never authorize deletion', () => {
  const backup = validateBackupRecord({ projectId: 'daily-system', assetId: 'mirax', sourceHash: 's', backupHash: 'b', createdAt: '2026-09-10T00:00:00Z', retentionClass: 'HISTORICAL_SEVEN_DAY' });
  assert.equal(backup.ok, true); assert.equal(retentionCandidate(backup.value, new Date('2026-09-25T00:00:00Z')).destructiveAction, false);
  const evidence = validateBackupRecord({ projectId: 'daily-system', assetId: 'mirax', sourceHash: 's', backupHash: 'b', verificationStatus: 'MISMATCH', retentionClass: 'UNRESOLVED_EVIDENCE' });
  assert.equal(retentionCandidate(evidence.value).candidate, false);
  assert.equal(archiveCandidate({ unusedSince: '2026-08-01T00:00:00Z', activeStorageBytes: 100 }, new Date('2026-09-25T00:00:00Z')).destructiveAction, false);
});
