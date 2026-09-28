const test = require('node:test');
const assert = require('node:assert/strict');
const {
  PREPARED_EXCEL_JOB_TYPES,
  validateDesktopProfile,
  validateExcelAsset,
  validatePreparedExcelJob,
  canAccessProject,
} = require('../src/excelAssets');

test('desktop profiles and supported Excel assets validate', () => {
  const profile = validateDesktopProfile({ id: 'desktop-1', name: 'Office Desktop', runnerId: 'windows-1', status: 'online', lastSeenAt: '2026-09-25T10:00:00Z' });
  assert.equal(profile.ok, true);
  assert.equal(profile.value.heartbeat.lastSuccessfulAt, '2026-09-25T10:00:00.000Z');
  const asset = validateExcelAsset({ projectId: 'daily-system', assetName: 'Mirax workbook', filename: 'Mirax.xlsm', manualPath: 'C:\\Workbooks\\Mirax.xlsm', backupPath: 'C:\\Backups\\Mirax.xlsm', status: 'verified', verificationTimestamp: '2026-09-25T10:00:00Z' });
  assert.equal(asset.ok, true);
  assert.equal(asset.value.filename, 'Mirax.xlsm');
  assert.equal(validateExcelAsset({ projectId: 'daily-system', assetName: 'Unknown', filename: 'other.xlsm' }).ok, false);
});

test('asset and job permissions stay project-scoped', () => {
  assert.equal(canAccessProject({ role: 'project', project: 'daily-system' }, 'daily-system'), true);
  assert.equal(canAccessProject({ role: 'project', project: 'daily-system' }, 'other-project'), false);
  assert.equal(canAccessProject({ role: 'runner', project: 'daily-system' }, 'daily-system', 'request'), false);
  const denied = validatePreparedExcelJob({ type: 'EXCEL_HEALTHCHECK', projectId: 'other-project', payload: { assetId: 'asset-1' } }, { role: 'runner', project: 'daily-system' });
  assert.equal(denied.ok, false);
});

test('prepared Excel job schemas are bounded and non-executing', () => {
  assert.deepEqual(PREPARED_EXCEL_JOB_TYPES, ['EXCEL_HEALTHCHECK', 'EXCEL_SNAPSHOT', 'EXCEL_BACKUP']);
  const health = validatePreparedExcelJob({ type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'asset-1', schemaVersion: 1 } }, { role: 'project', project: 'daily-system' });
  assert.equal(health.ok, true);
  assert.equal(health.value.executionAllowed, false);
  const snapshot = validatePreparedExcelJob({ type: 'EXCEL_SNAPSHOT', projectId: 'daily-system', idempotencyKey: 'snap-1', payload: { assetId: 'asset-1', mode: 'read_only' } }, { role: 'admin' });
  assert.equal(snapshot.ok, true);
  assert.equal(snapshot.value.timeoutMs, 180000);
  const unsafe = validatePreparedExcelJob({ type: 'EXCEL_BACKUP', projectId: 'daily-system', payload: { assetId: 'asset-1', destinationRef: 'backup-1', command: 'powershell.exe' } }, { role: 'admin' });
  assert.equal(unsafe.ok, false);
});
