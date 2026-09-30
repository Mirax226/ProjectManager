// Non-production: in-memory Control Plane + actual Windows Excel on copies.
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { ControlPlaneStore } = require('../src/controlPlane/store');
const { createControlPlaneHandler } = require('../src/controlPlane/handler');
const { createAuthenticator } = require('../src/controlPlane/auth');
const { RunnerClient } = require('../src/runner/client');
const { runOnce } = require('../src/runner');

function excelProcesses() {
  const output = execFileSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', '@(Get-Process -Name EXCEL -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id) | ConvertTo-Json -Compress'], { windowsHide: true, encoding: 'utf8' }).trim();
  const ids = output ? JSON.parse(output) : [];
  return Array.isArray(ids) ? ids : [ids];
}

async function main() {
  const before = excelProcesses();
  const projectId = 'daily-system';
  const store = new ControlPlaneStore();
  const handler = createControlPlaneHandler({ store, authenticator: createAuthenticator({ runnerTokens: { 'windows-01': { token: 'local-diagnostic-fixture', project: projectId } } }) });
  const client = new RunnerClient({ baseUrl: 'https://local-diagnostic.invalid', token: 'local-diagnostic-fixture', runnerId: 'windows-01', projectId, fetch: (url, init) => handler(new Request(url, init)) });
  const evidence = { date: '2026-09-30', runtime: 'local in-memory Control Plane / Windows Runner / real Excel COM', preexistingExcelProcessIds: before, workbooks: [] };
  const assetArg = process.argv.find((arg) => arg.startsWith('--asset='))?.slice(8);
  if (assetArg && !['Mirax.xlsm', 'Gozareshkar.xlsm'].includes(assetArg)) throw new Error('Unsupported diagnostic asset');
  for (const filename of assetArg ? [assetArg] : ['Mirax.xlsm', 'Gozareshkar.xlsm']) {
    const assetId = path.basename(filename, '.xlsm').toLowerCase();
    const source = path.resolve(__dirname, '../../..', 'ExcelMirror', filename);
    if (!fs.existsSync(source)) throw new Error(`Configured diagnostic asset missing: ${filename}`);
    const created = store.createJob({ projectId, type: 'EXCEL_HEALTHCHECK', payload: { assetId, requireExcelDesktop: true } }, 'local-diagnostic');
    if (!created.ok) throw new Error('Local diagnostic job creation failed');
    const result = await runOnce(client, { profile: { id: 'windows-01', projectId, runnerId: 'windows-01', excelAssets: { [assetId]: { projectId, manualPath: source } } } });
    const row = result.result;
    evidence.workbooks.push({ asset: filename, ok: result.ok, jobStatus: store.getJob(created.job.id).status, eventCategory: result.eventCategory, openability: row.openability, failedStage: row.failedStage, diagnosticCode: result.diagnosticCode || null, safeError: row.safeError || null, durationMs: row.durationMs, probeDurationMs: row.probeDurationMs, excelVersion: row.excelVersion, disposableCopyUsed: row.disposableCopyUsed, sourceHashBefore: row.sourceHashBefore, sourceHashAfter: row.sourceHashAfter, sourceUnchanged: row.sourceUnchanged, ownedExcelProcessIds: row.ownedExcelProcessIds, excelProcessOwnershipVerified: row.excelProcessOwnershipVerified, excelProcessCleanupVerified: row.excelProcessCleanupVerified, externalRefreshSuppressed: row.externalRefreshSuppressed, queryTablesDisabled: row.queryTablesDisabled, calculationDisabled: row.calculationDisabled, unverifiedNewExcelProcessIds: row.unverifiedNewExcelProcessIds, refreshFlagsCleared: row.refreshFlagsCleared, vbaProjectUnchanged: row.vbaProjectUnchanged, lifecycleWarningCode: row.lifecycleWarningCode, gracefulQuitTimedOut: row.gracefulQuitTimedOut, stages: row.stages.map((entry) => entry.stage) });
  }
  evidence.finalExcelProcessIds = excelProcesses();
  evidence.preexistingProcessesPreserved = before.every((id) => evidence.finalExcelProcessIds.includes(id));
  evidence.probeOwnedProcessesExited = evidence.workbooks.every((row) => row.ownedExcelProcessIds.every((id) => !evidence.finalExcelProcessIds.includes(id)));
  if (process.argv.includes('--record')) fs.writeFileSync(path.resolve(__dirname, '../docs/handoffs/PJ-011B/EXCEL-HOST-EVIDENCE.json'), JSON.stringify(evidence, null, 2) + '\n');
  console.log(JSON.stringify(evidence, null, 2));
  if (!evidence.workbooks.every((row) => row.ok && row.sourceUnchanged && row.excelProcessCleanupVerified) || !evidence.preexistingProcessesPreserved || !evidence.probeOwnedProcessesExited) process.exitCode = 1;
}
main().catch((error) => { console.error(error.message); process.exitCode = 1; });
