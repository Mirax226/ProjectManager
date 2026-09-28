const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn } = require('child_process');
const { JOB_TYPES, JOB_RISK, redact } = require('../controlPlane/contracts');
const { executeExcelJob } = require('./excelOperations');

const MAX_OUTPUT = 12000;
const DEFAULT_PROFILE = {
  id: 'daily-system',
  repoPath: process.env.DAILYSYSTEM_REPO_PATH || '',
  platform: 'windows',
  testCommand: ['npm.cmd', ['test', '--', '--run']],
  typecheckCommand: ['npm.cmd', ['run', 'typecheck']],
  healthCommand: null,
  runnerId: process.env.PG_RUNNER_ID || null,
  excelAssets: {},
  backupLocation: process.env.DAILYSYSTEM_BACKUP_PATH || null,
};

function supportedJob(type) { return JOB_TYPES.includes(String(type || '').toUpperCase()); }
function truncate(value) { const text = String(value || ''); return text.length <= MAX_OUTPUT ? text : `${text.slice(0, MAX_OUTPUT - 1)}…`; }
function commandFor(type, profile = DEFAULT_PROFILE) {
  const normalized = String(type || '').toUpperCase();
  if (normalized === 'RUN_TESTS') return profile.testCommand || ['npm.cmd', ['test', '--', '--run']];
  if (normalized === 'RUN_TYPECHECK') return profile.typecheckCommand || ['npm.cmd', ['run', 'typecheck']];
  if (normalized === 'DAILYSYSTEM_HEALTHCHECK') return profile.healthCommand;
  if (normalized === 'EXCEL_RECONCILIATION_CHECK') return profile.reconciliationCommand || null;
  return null;
}

function isInside(parent, target) {
  const relative = path.relative(path.resolve(parent), path.resolve(target));
  return relative === '' || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative));
}

function resolveRepoPath(profile, requested) {
  const repo = requested || profile.repoPath;
  if (!repo || !path.isAbsolute(repo)) throw new Error('project repoPath must be an absolute path');
  const resolved = path.resolve(repo);
  if (profile.repoRoot && !isInside(profile.repoRoot, resolved)) throw new Error('repoPath is outside the configured runner root');
  if (!requested && profile.repoPath) return path.resolve(profile.repoPath);
  if (requested && profile.repoPath && resolved !== path.resolve(profile.repoPath) && !profile.repoRoot) throw new Error('repoPath is not the configured project repository');
  return resolved;
}

function runCommand(file, args, cwd, options = {}) {
  const timeoutMs = Math.min(15 * 60 * 1000, Math.max(1000, Number(options.timeoutMs || 120000)));
  return new Promise((resolve) => {
    const child = spawn(file, args, { cwd, shell: false, windowsHide: true, env: { ...process.env, ...(options.env || {}) } });
    let stdout = ''; let stderr = ''; let timedOut = false;
    child.stdout?.on('data', (chunk) => { stdout += chunk; }); child.stderr?.on('data', (chunk) => { stderr += chunk; });
    const timer = setTimeout(() => { timedOut = true; child.kill(); }, timeoutMs);
    child.on('error', (error) => { clearTimeout(timer); resolve({ ok: false, exitCode: null, stdout: truncate(stdout), stderr: truncate(error.message), timedOut }); });
    child.on('close', (code) => { clearTimeout(timer); resolve({ ok: code === 0 && !timedOut, exitCode: code, stdout: truncate(stdout), stderr: truncate(stderr), timedOut }); });
  });
}

async function executeJob(job, options = {}) {
  if (!job || !supportedJob(job.type)) throw new Error('unsupported job type');
  const profile = { ...DEFAULT_PROFILE, ...(options.profile || {}) };
  const run = options.runCommand || runCommand;
  const type = String(job.type).toUpperCase();
  if (['EXCEL_HEALTHCHECK', 'EXCEL_SNAPSHOT', 'EXCEL_BACKUP', 'EXCEL_RECONCILIATION', 'EXCEL_RECONCILIATION_CHECK', 'EXCEL_SYNC'].includes(type)) return executeExcelJob(job, profile, options);
  const repoPath = resolveRepoPath(profile, job.payload?.repoPath);
  if (type === 'HEALTHCHECK' || type === 'PROJECT_STATUS') return { ok: true, result: { projectId: job.projectId, repoPath, runner: os.hostname(), status: fs.existsSync(repoPath) ? 'available' : 'missing' } };
  if (type === 'GIT_STATUS') return { ok: true, result: await run('git', ['status', '--short', '--branch'], repoPath, { timeoutMs: 60000 }) };
  if (type === 'RUN_TESTS' || type === 'RUN_TYPECHECK' || type === 'DAILYSYSTEM_HEALTHCHECK' || type === 'EXCEL_RECONCILIATION_CHECK') {
    const command = commandFor(type, profile); if (!command || !Array.isArray(command[1])) throw new Error(`${type} is not configured`);
    return { ok: true, result: await run(command[0], command[1], repoPath, { timeoutMs: job.payload?.timeoutMs || 120000 }) };
  }
  if (type === 'EXCEL_CONNECTIVITY_TEST') {
    if (process.platform !== 'win32') return { ok: false, error: 'Excel diagnostics require Windows' };
    const script = "$ErrorActionPreference='Stop'; $excel=New-Object -ComObject Excel.Application; $version=$excel.Version; $excel.Quit(); [Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null; Write-Output ('Excel ' + $version)";
    return { ok: true, result: await run('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', script], repoPath, { timeoutMs: 60000 }) };
  }
  if (type === 'EXCEL_SYNC_DIAGNOSTIC') {
    const files = Array.isArray(job.payload?.sourceWorkbooks) ? job.payload.sourceWorkbooks.slice(0, 20) : [];
    return { ok: true, result: { agentConfigured: Boolean(profile.excelAgentPath), sourceWorkbooks: files.map((file) => ({ path: String(file).slice(0, 260), exists: fs.existsSync(path.resolve(repoPath, String(file))) })) } };
  }
  if (type === 'CODEX_TASK') {
    const mode = String(job.payload?.mode || 'read-only').toLowerCase();
    if (mode === 'edit' && String(process.env.PG_ALLOW_CODEX_EDITS).toLowerCase() !== 'true') return { ok: false, error: 'Codex edit mode is disabled' };
    const task = String(job.payload?.task || '').trim().slice(0, 8000); if (!task) return { ok: false, error: 'task is required' };
    const bin = String(process.env.CODEX_BIN || 'codex'); const args = ['exec']; if (process.env.CODEX_PROFILE) args.push('--profile', String(process.env.CODEX_PROFILE)); args.push(task);
    return { ok: true, result: await run(bin, args, repoPath, { timeoutMs: job.payload?.timeoutMs || 15 * 60 * 1000 }) };
  }
  return { ok: false, error: 'job type is not enabled' };
}

module.exports = { DEFAULT_PROFILE, JOB_RISK, supportedJob, commandFor, resolveRepoPath, runCommand, executeJob, redact };
