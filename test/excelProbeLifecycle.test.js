const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { execFileSync } = require('node:child_process');
const { EventEmitter } = require('node:events');
const { probeExcelDesktop, healthcheck } = require('../src/runner/excelOperations');

function adapter({ timeout = false, cleanup = true, comFailure = false, stage = 'WORKBOOK_OPEN_START' }) {
  const calls = [];
  function spawn(file, args, options) {
    calls.push({ file, args, options });
    const child = new EventEmitter(); child.stdout = new EventEmitter(); child.stderr = new EventEmitter();
    child.kill = () => setImmediate(() => child.emit('close', null));
    setImmediate(() => {
      if (args.includes('-CleanupOnly')) {
        child.stdout.emit('data', JSON.stringify({ cleanupVerified: cleanup, noNewExcelProcessesObserved: comFailure, remainingOwnedProcessIds: cleanup ? [] : [100] }));
        child.emit('close', 0);
      } else {
        const laterStages = ['WORKBOOK_READ_PROBE', 'WORKBOOK_CLOSE', 'EXCEL_QUIT'].includes(stage) ? ['WORKBOOK_OPEN_SUCCESS', 'WORKBOOK_READ_PROBE', ...(['WORKBOOK_CLOSE', 'EXCEL_QUIT'].includes(stage) ? ['WORKBOOK_CLOSE'] : []), ...(stage === 'EXCEL_QUIT' ? ['EXCEL_QUIT'] : [])] : [];
        fs.writeFileSync(options.env.PJ_EXCEL_PROBE_STATE, JSON.stringify({ stage: comFailure ? 'COM_CREATE' : stage, substage: stage === 'WORKBOOK_CLOSE' ? 'SEED_WORKBOOK_CLOSE' : null, stages: comFailure ? ['COM_CREATE'] : ['COM_CREATE', 'EXCEL_CONFIGURE', 'WORKBOOK_OPEN_START', ...laterStages], before: [99], owned: comFailure ? [] : [{ id: 100, startTicks: '1' }] }));
        if (!timeout) {
          child.stdout.emit('data', JSON.stringify(comFailure ? { ok: false, failedStage: 'COM_CREATE', diagnosticCode: 'COM_CREATE_FAILED', safeError: { exceptionClass: 'COMException', hresult: '0x80070520' } } : { ok: true, version: '16.0', ownedProcessIds: [100] }));
          child.emit('close', 0);
        }
      }
    });
    return child;
  }
  return { spawn, calls };
}

test('Excel timeout retains actual Workbooks.Open stage and performs independent owned-process cleanup', { skip: process.platform !== 'win32' }, async () => {
  const fake = adapter({ timeout: true });
  const result = await probeExcelDesktop('C:\\disposable\\Mirax.xlsm', 1000, fake);
  assert.equal(result.diagnosticCode, 'TIMEOUT');
  assert.equal(result.failedStage, 'WORKBOOK_OPEN_START');
  assert.equal(result.cleanupVerified, true);
  assert.deepEqual(result.ownedExcelProcessIds, [100]);
  assert.equal(fake.calls.length, 2);
  assert.equal(fake.calls[1].args.includes('-CleanupOnly'), true);
  assert.equal(fake.calls[0].options.shell, false);
  assert.equal(fs.existsSync(fake.calls[0].options.env.PJ_EXCEL_PROBE_STATE), false);
});

test('Excel quit timeout recovers only after verified owned-process cleanup; open/read timeouts stay failed', { skip: process.platform !== 'win32' }, async () => {
  const recovered = await probeExcelDesktop('C:\\disposable\\Mirax.xlsm', 1000, adapter({ timeout: true, stage: 'EXCEL_QUIT' }));
  assert.equal(recovered.ok, true);
  assert.equal(recovered.lifecycleWarningCode, 'EXCEL_QUIT_TIMEOUT_RECOVERED');
  assert.equal(recovered.failedStage, 'EXCEL_QUIT');
  assert.equal(recovered.cleanupVerified, true);
  const failedCleanup = await probeExcelDesktop('C:\\disposable\\Mirax.xlsm', 1000, adapter({ timeout: true, stage: 'EXCEL_QUIT', cleanup: false }));
  assert.equal(failedCleanup.ok, false);
  const readTimeout = await probeExcelDesktop('C:\\disposable\\Mirax.xlsm', 1000, adapter({ timeout: true, stage: 'WORKBOOK_READ_PROBE' }));
  assert.equal(readTimeout.ok, false);
  assert.equal(readTimeout.failedStage, 'WORKBOOK_READ_PROBE');
  assert.equal(readTimeout.openability, 'OPENABLE');
});

test('close timeout preserves successful open, exact substage, safe TimeoutError and separate cleanup duration', { skip: process.platform !== 'win32' }, async () => {
  const result = await probeExcelDesktop('C:\\disposable\\Mirax.xlsm', 1000, adapter({ timeout: true, stage: 'WORKBOOK_CLOSE' }));
  assert.equal(result.ok, false); assert.equal(result.openability, 'OPENABLE'); assert.equal(result.workbookOpened, true);
  assert.equal(result.failedStage, 'WORKBOOK_CLOSE'); assert.equal(result.failedSubstage, 'SEED_WORKBOOK_CLOSE');
  assert.equal(result.safeError.exceptionClass, 'TimeoutError'); assert.equal(result.safeError.hresult, null);
  assert.ok(result.durationMs >= 1000); assert.ok(result.cleanupDurationMs >= 0); assert.equal(result.cleanupVerified, true);
});

test('healthcheck cannot convert a post-open lifecycle failure into success and preserves copy/source safety', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-post-open-')); const source = path.join(root, 'fixture.xlsm'); fs.writeFileSync(source, 'safe fixture');
  let copy;
  try {
    const result = await healthcheck({ projectId: 'daily-system', payload: { assetId: 'fixture' } }, { excelAssets: { fixture: { projectId: 'daily-system', manualPath: source } } }, { probeOpenability: async (file) => { copy = file; return { ok: false, openability: 'OPENABLE', workbookOpened: true, diagnosticCode: 'TIMEOUT', failedStage: 'WORKBOOK_CLOSE', failedSubstage: 'TARGET_WORKBOOK_CLOSE', durationMs: 1000, cleanupDurationMs: 20, safeError: { exceptionClass: 'TimeoutError', hresult: null } }; } });
    assert.equal(result.ok, false); assert.equal(result.result.openability, 'OPENABLE'); assert.equal(result.result.failedStage, 'WORKBOOK_CLOSE'); assert.equal(result.result.failedSubstage, 'TARGET_WORKBOOK_CLOSE'); assert.equal(result.result.workbookOpened, true);
    assert.equal(result.result.sourceHashBefore, result.result.sourceHashAfter); assert.equal(result.result.sourceUnchanged, true); assert.equal(fs.existsSync(copy), false);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('Excel cleanup failure cannot be reported as successful openability', { skip: process.platform !== 'win32' }, async () => {
  const result = await probeExcelDesktop('C:\\disposable\\Mirax.xlsm', 1000, adapter({ cleanup: false }));
  assert.equal(result.ok, false);
  assert.equal(result.diagnosticCode, 'EXCEL_PROCESS_CLEANUP_FAILED');
  assert.equal(result.cleanupVerified, false);
});

test('Actual COM logon-session failure preserves HRESULT and verifies no new process observation', { skip: process.platform !== 'win32' }, async () => {
  const result = await probeExcelDesktop('C:\\disposable\\Mirax.xlsm', 1000, adapter({ comFailure: true }));
  assert.equal(result.failedStage, 'COM_CREATE');
  assert.equal(result.safeError.hresult, '0x80070520');
  assert.equal(result.ownershipVerified, false);
  assert.equal(result.cleanupVerified, true);
});

test('Disposable refresh suppression changes only refresh metadata and retains XLSM/VBA payload', { skip: process.platform !== 'win32' }, () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-excel-probe-'));
  const source = path.join(root, 'source');
  fs.mkdirSync(path.join(source, 'xl', 'queryTables'), { recursive: true });
  fs.writeFileSync(path.join(source, 'xl', 'connections.xml'), '<connections><connection refreshOnLoad="1" saveData="1" /></connections>');
  fs.writeFileSync(path.join(source, 'xl', 'queryTables', 'queryTable1.xml'), '<queryTable refreshOnLoad="1" backgroundRefresh="0" />');
  fs.writeFileSync(path.join(source, 'xl', 'vbaProject.bin'), 'unchanged-vba-fixture');
  fs.writeFileSync(path.join(source, 'xl', 'workbook.xml'), '<workbook/>');
  const copy = path.join(root, 'Mirax.xlsm');
  const env = { ...process.env, PJ_FIXTURE_SOURCE: source, PJ_EXCEL_PROBE_PATH: copy, PJ_EXCEL_PROBE_STATE: path.join(root, 'state.json'), PJ_EXCEL_SUPPRESS_REFRESH: 'true' };
  const ps = (args) => execFileSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', ...args], { env, windowsHide: true, encoding: 'utf8' });
  try {
    ps(['-Command', 'Add-Type -AssemblyName System.IO.Compression; Add-Type -AssemblyName System.IO.Compression.FileSystem; $z=[IO.Compression.ZipFile]::Open($env:PJ_EXCEL_PROBE_PATH,[IO.Compression.ZipArchiveMode]::Create); try { foreach($f in Get-ChildItem -LiteralPath $env:PJ_FIXTURE_SOURCE -File -Recurse) { $name=$f.FullName.Substring($env:PJ_FIXTURE_SOURCE.Length+1).Replace([char]92,[char]47); [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($z,$f.FullName,$name) | Out-Null } } finally { $z.Dispose() }']);
    const prepared = JSON.parse(ps(['-File', path.resolve(__dirname, '../tools/excel-desktop-probe.ps1'), '-PrepareOnly']).trim());
    assert.equal(prepared.refreshFlagsCleared, 2, JSON.stringify(prepared));
    assert.equal(prepared.vbaProjectUnchanged, true);
    const check = JSON.parse(ps(['-Command', 'Add-Type -AssemblyName System.IO.Compression.FileSystem; $z=[IO.Compression.ZipFile]::OpenRead($env:PJ_EXCEL_PROBE_PATH); try { $data=@{}; foreach($name in @("xl/connections.xml","xl/queryTables/queryTable1.xml","xl/vbaProject.bin","xl/workbook.xml")) { $r=[IO.StreamReader]::new($z.GetEntry($name).Open()); try { $data[$name]=$r.ReadToEnd() } finally { $r.Dispose() } }; $data | ConvertTo-Json -Compress } finally { $z.Dispose() }']).trim());
    assert.equal(check['xl/vbaProject.bin'], 'unchanged-vba-fixture');
    assert.equal(check['xl/workbook.xml'], '<workbook/>');
    assert.match(check['xl/connections.xml'], /refreshOnLoad="0"/);
    assert.match(check['xl/queryTables/queryTable1.xml'], /refreshOnLoad="0"/);
    assert.match(check['xl/queryTables/queryTable1.xml'], /disableRefresh="1"/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
