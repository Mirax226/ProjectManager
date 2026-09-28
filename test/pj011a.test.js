const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const { executeExcelJob } = require('../src/runner/excelOperations');

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj011a-'));
  const source = path.join(root, 'Mirax.xlsm');
  fs.writeFileSync(source, 'macro-enabled-fixture');
  return { root, source, profile: { projectId: 'daily-system', excelAssets: { mirax: { manualPath: source } } }, job: { type: 'EXCEL_HEALTHCHECK', projectId: 'daily-system', payload: { assetId: 'mirax' } } };
}

test('PJ-011A classifies COM creation failures with stage and safe evidence', async () => {
  const f = fixture();
  const result = await executeExcelJob(f.job, f.profile, { probeOpenability: async () => ({ ok: false, openability: 'OPEN_FAILED', diagnosticCode: 'COM_CREATE_FAILED', failedStage: 'COM_CREATE', safeError: { exceptionClass: 'COMException', hresult: '0x80040154', message: 'class unavailable' }, durationMs: 12 }) });
  assert.equal(result.diagnosticCode, 'COM_CREATE_FAILED');
  assert.equal(result.result.failedStage, 'COM_CREATE');
  assert.equal(result.result.safeErrorClass, 'COMException');
  assert.equal(result.result.safeError.hresult, '0x80040154');
  assert.equal(result.result.sourceUnchanged, true);
});

test('PJ-011A classifies Workbooks.Open failure and timeout stage', async () => {
  const f = fixture();
  const open = await executeExcelJob(f.job, f.profile, { probeOpenability: async () => ({ ok: false, openability: 'OPEN_FAILED', diagnosticCode: 'WORKBOOK_OPEN_FAILED', failedStage: 'WORKBOOK_OPEN_START', safeError: { exceptionClass: 'COMException', hresult: '0x800A03EC', message: 'open failed' } }) });
  assert.equal(open.diagnosticCode, 'WORKBOOK_OPEN_FAILED');
  assert.equal(open.result.failedStage, 'WORKBOOK_OPEN_START');
  const timeout = await executeExcelJob(f.job, f.profile, { probeOpenability: async () => ({ ok: false, openability: 'OPEN_FAILED', diagnosticCode: 'TIMEOUT', failedStage: 'TIMEOUT' }) });
  assert.equal(timeout.diagnosticCode, 'TIMEOUT');
  assert.equal(timeout.result.failedStage, 'TIMEOUT');
});

test('PJ-011A preserves xlsm source hash and disposable-copy evidence', async () => {
  const f = fixture();
  const before = crypto.createHash('sha256').update(fs.readFileSync(f.source)).digest('hex');
  const result = await executeExcelJob(f.job, f.profile, { probeOpenability: async (copy) => { assert.notEqual(copy, f.source); assert.match(copy, /Mirax\.xlsm$/); return { ok: true, openability: 'OPENABLE' }; } });
  assert.equal(result.ok, true);
  assert.equal(result.result.sourceHashBefore, before);
  assert.equal(result.result.sourceHashAfter, before);
  assert.equal(result.result.sourceUnchanged, true);
  assert.equal(result.result.disposableCopyUsed, true);
  assert.deepEqual(result.result.stages.map((entry) => entry.stage), ['ASSET_RESOLVE', 'SOURCE_EXISTS', 'SOURCE_HASH_BEFORE', 'COPY_CREATE', 'COPY_DELETE', 'SOURCE_HASH_AFTER']);
  assert.equal(fs.readFileSync(f.source).toString(), 'macro-enabled-fixture');
});

test('PJ-011A preserves real probe lifecycle stages when supplied by the COM adapter', async () => {
  const f = fixture();
  const result = await executeExcelJob(f.job, f.profile, { probeOpenability: async () => ({ ok: true, openability: 'OPENABLE', stages: ['COM_CREATE', 'EXCEL_CONFIGURE', 'WORKBOOK_OPEN_START', 'WORKBOOK_OPEN_SUCCESS', 'WORKBOOK_READ_PROBE', 'WORKBOOK_CLOSE', 'EXCEL_QUIT'] }) });
  assert.equal(result.ok, true);
  assert.deepEqual(result.result.stages.map((entry) => entry.stage), ['ASSET_RESOLVE', 'SOURCE_EXISTS', 'SOURCE_HASH_BEFORE', 'COPY_CREATE', 'COM_CREATE', 'EXCEL_CONFIGURE', 'WORKBOOK_OPEN_START', 'WORKBOOK_OPEN_SUCCESS', 'WORKBOOK_READ_PROBE', 'WORKBOOK_CLOSE', 'EXCEL_QUIT', 'COPY_DELETE', 'SOURCE_HASH_AFTER']);
});
