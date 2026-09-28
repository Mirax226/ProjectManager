const test = require('node:test');
const assert = require('node:assert/strict');
const { validateProvider, canAccessProvider, sanitizeProviderForTelegram } = require('../src/providerRegistry');
const { buildDailySystemOpsView } = require('../src/dailySystemOps');
const { OPERATIONAL_JOB_TYPES, validateOperationalJob } = require('../src/operationalJobs');
const { validateOperationalEvent } = require('../src/controlPlane/contracts');

test('provider validation and Telegram sanitization exclude secrets', () => {
  const provider = validateProvider({ projectId: 'daily-system', name: 'Gemini API', serviceType: 'ai', status: 'enabled', health: 'healthy', usage: { requests: 4, units: 20 }, apiKey: 'must-not-accept' });
  assert.equal(provider.ok, false);
  const valid = validateProvider({ projectId: 'daily-system', name: 'Gemini API', serviceType: 'ai', status: 'enabled', health: 'healthy', usage: { requests: 4, units: 20 } });
  assert.equal(valid.ok, true);
  const safe = sanitizeProviderForTelegram({ ...valid.value, apiKey: 'hidden' });
  assert.equal('apiKey' in safe, false);
  assert.equal(safe.usage.requests, 4);
});

test('provider and job permissions are project isolated', () => {
  assert.equal(canAccessProvider({ role: 'project', project: 'daily-system' }, 'daily-system'), true);
  assert.equal(canAccessProvider({ role: 'project', project: 'daily-system' }, 'other-project'), false);
  const denied = validateOperationalJob({ type: 'EXCEL_HEALTHCHECK', projectId: 'other-project', payload: { assetId: 'a1' } }, { role: 'runner', project: 'daily-system' });
  assert.equal(denied.ok, false);
});

test('operational events support new categories and component sanitization', () => {
  const result = validateOperationalEvent({ schemaVersion: 1, project: 'daily-system', environment: 'dev', severity: 'ERROR', category: 'API_FAILED', component: 'gemini', source: 'daily-system', message: 'request failed', context: { token: 'secret', retry: 1 } }, { nowMs: Date.parse('2026-09-25T10:00:00Z') });
  assert.equal(result.ok, true);
  assert.equal(result.value.component, 'gemini');
  assert.equal(result.value.context.token, '[REDACTED]');
});

test('operational job schemas are typed, bounded, and non-executing', () => {
  assert.deepEqual(OPERATIONAL_JOB_TYPES, ['HEALTHCHECK', 'EXCEL_HEALTHCHECK', 'EXCEL_BACKUP', 'EXCEL_SYNC', 'RUN_TESTS']);
  const job = validateOperationalJob({ type: 'EXCEL_SYNC', projectId: 'daily-system', idempotencyKey: 'sync-1', payload: { assetId: 'asset-1', mode: 'dry_run' } }, { role: 'admin' });
  assert.equal(job.ok, true);
  assert.equal(job.value.executionAllowed, false);
  assert.equal(validateOperationalJob({ type: 'EXCEL_SYNC', projectId: 'daily-system', payload: { assetId: 'asset-1', mode: 'write', command: 'powershell' } }, { role: 'admin' }).ok, false);
  const view = buildDailySystemOpsView({ project: 'daily-system', excel: { status: 'VERIFIED' }, runner: { status: 'ONLINE' }, backup: { status: 'HEALTHY' }, sync: { status: 'IDLE' }, reconciliation: { status: 'CLEAN' } });
  assert.match(view.text, /Excel: VERIFIED/);
  assert.match(view.text, /Reconciliation: CLEAN/);
});
