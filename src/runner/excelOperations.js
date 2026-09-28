const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const os = require('node:os');
const { spawn } = require('node:child_process');

const SUPPORTED_EXTENSIONS = Object.freeze(['.xlsm', '.xlsx']);
const UNSAFE_PATH = /[\0\r\n<>"|?*;&`$]/;
const RECONCILIATION_STATUSES = Object.freeze(['MATCH', 'MISMATCH', 'ERROR', 'EXPECTED_INTENTIONAL_DIVERGENCE', 'KNOWN_LEGACY_DEFECT']);
const SAFE_META_KEYS = Object.freeze(['code', 'message', 'component', 'source', 'reference', 'retryable', 'sourceHash', 'destinationHash']);

function fail(error, code = 'EXCEL_OPERATION_INVALID') { return { ok: false, error, diagnosticCode: code }; }
function eventFor(code) { return ({ EXCEL_FILE_NOT_FOUND: 'EXCEL_FILE_NOT_FOUND', EXCEL_FINGERPRINT_MISMATCH: 'EXCEL_FINGERPRINT_MISMATCH', EXCEL_OPEN_FAILED: 'EXCEL_OPEN_FAILED', EXCEL_BACKUP_HASH_MISMATCH: 'EXCEL_BACKUP_HASH_MISMATCH' })[code] || 'EXCEL_AGENT_ERROR'; }
function safePath(value) {
  const candidate = String(value == null ? '' : value).trim();
  if (!candidate || UNSAFE_PATH.test(candidate) || /(^|[\\/])\.\.?([\\/]|$)/.test(candidate)) return null;
  if (!path.isAbsolute(candidate) && !/^[A-Za-z]:[\\/]/.test(candidate)) return null;
  return path.resolve(candidate);
}
function hashFile(filePath) { const digest = crypto.createHash('sha256'); const stream = fs.createReadStream(filePath); return new Promise((resolve, reject) => { stream.on('data', (chunk) => digest.update(chunk)); stream.on('error', reject); stream.on('end', () => resolve(digest.digest('hex'))); }); }
function statResult(filePath, hash) { const stat = fs.statSync(filePath); return { path: filePath, isFile: stat.isFile(), size: stat.size, modifiedAt: stat.mtime.toISOString(), sha256: hash || null }; }
function assetFor(job, profile, options = {}) { if (typeof options.resolveAsset === 'function') return options.resolveAsset(job.payload?.assetId, job.projectId); const assets = profile.excelAssets || {}; return assets[job.payload?.assetId] || assets[`${job.projectId}:${job.payload?.assetId}`] || null; }
function sourcePath(asset) { return safePath(asset?.manualPath || asset?.sourcePath || asset?.path); }
function destinationPath(asset, profile, payload = {}) { return safePath(payload.destinationPath || asset?.backupPath || profile.backupLocation || profile.backupPath); }
function safeMetadata(input) { const source = input && typeof input === 'object' ? input : {}; return Object.fromEntries(SAFE_META_KEYS.filter((key) => Object.prototype.hasOwnProperty.call(source, key)).map((key) => [key, typeof source[key] === 'string' ? String(source[key]).slice(0, 500) : source[key]])); }

const EXCEL_DESKTOP_PROBE = "$ErrorActionPreference='Stop'; $path=$env:PJ_EXCEL_PROBE_PATH; $before=@(Get-Process -Name EXCEL -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id); $excel=$null; $book=$null; $owned=@(); try { $excel=New-Object -ComObject Excel.Application; $afterCreate=@(Get-Process -Name EXCEL -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id); $owned=@($afterCreate | Where-Object { $before -notcontains $_ }); $excel.Visible=$false; $excel.DisplayAlerts=$false; $excel.AskToUpdateLinks=$false; $excel.EnableEvents=$false; $excel.AutomationSecurity=3; $book=$excel.Workbooks.Open($path, 0, $true); $version=[string]$excel.Version; $book.Close($false); $book=$null; $excel.Quit(); [pscustomobject]@{ ok=$true; version=$version; ownedProcessIds=@($owned) } | ConvertTo-Json -Compress } finally { if ($book) { try { $book.Close($false) } catch {} }; if ($excel) { try { $excel.Quit() } catch {}; try { [Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null } catch {} } }";

function probeExcelDesktop(filePath, timeoutMs = 60_000) {
  if (process.platform !== 'win32') return Promise.resolve({ ok: false, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', openability: 'NOT_VERIFIED' });
  return new Promise((resolve) => {
    let stdout = ''; let stderr = ''; let timedOut = false;
    const child = spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', EXCEL_DESKTOP_PROBE], { shell: false, windowsHide: true, env: { ...process.env, PJ_EXCEL_PROBE_PATH: filePath } });
    const timer = setTimeout(() => { timedOut = true; child.kill(); }, Math.min(120_000, Math.max(1_000, Number(timeoutMs) || 60_000)));
    child.stdout?.on('data', (chunk) => { stdout += chunk; }); child.stderr?.on('data', (chunk) => { stderr += chunk; });
    const finish = (result) => { clearTimeout(timer); resolve(result); };
    child.on('error', () => finish({ ok: false, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', openability: 'NOT_VERIFIED' }));
    child.on('close', (code) => {
      if (timedOut) return finish({ ok: false, diagnosticCode: 'EXCEL_OPEN_FAILED', openability: 'OPEN_FAILED' });
      if (code !== 0) return finish({ ok: false, diagnosticCode: 'EXCEL_OPEN_FAILED', openability: 'OPEN_FAILED' });
      try {
        const parsed = JSON.parse(stdout.trim());
        if (!parsed.ok) return finish({ ok: false, diagnosticCode: 'EXCEL_OPEN_FAILED', openability: 'OPEN_FAILED' });
        return finish({ ok: true, openability: 'OPENABLE', excelVersion: String(parsed.version || '').slice(0, 80), ownedExcelProcessIds: Array.isArray(parsed.ownedProcessIds) ? parsed.ownedProcessIds.slice(0, 8).map((id) => Number(id)).filter(Number.isInteger) : [] });
      } catch (_) { return finish({ ok: false, diagnosticCode: stderr ? 'EXCEL_OPEN_FAILED' : 'EXCEL_DESKTOP_UNAVAILABLE', openability: 'NOT_VERIFIED' }); }
    });
  });
}

async function healthcheck(job, profile, options = {}) {
  const asset = assetFor(job, profile, options); if (!asset) return fail('Excel asset is not configured', 'EXCEL_ASSET_NOT_CONFIGURED');
  if (asset.projectId && String(asset.projectId).toLowerCase() !== String(job.projectId).toLowerCase()) return fail('Excel asset is outside the job project', 'PROJECT_SCOPE_DENIED');
  const configuredPath = sourcePath(asset); const result = { projectId: job.projectId, assetId: String(job.payload?.assetId || ''), configurationPresent: true, path: configuredPath, pathExists: false, isFile: false, supportedExtension: false, size: null, modifiedAt: null, sha256: null, sourceHashBefore: null, sourceHashAfter: null, sourceUnchanged: null, excelDesktopAvailable: process.platform === 'win32', openability: 'NOT_CHECKED', disposableCopy: 'NOT_CREATED', ownedExcelProcessIds: [], macrosDisabled: true, eventsDisabled: true, linkUpdatesDisabled: true, expectedFingerprintStatus: 'NOT_CONFIGURED', diagnosticCode: null };
  if (!configuredPath) return { ok: false, result: { ...result, configurationPresent: false, diagnosticCode: 'EXCEL_PATH_INVALID' }, diagnosticCode: 'EXCEL_PATH_INVALID', eventCategory: 'EXCEL_AGENT_ERROR' };
  result.supportedExtension = SUPPORTED_EXTENSIONS.includes(path.extname(configuredPath).toLowerCase());
  if (job.payload?.requireExcelDesktop === true && !result.excelDesktopAvailable) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE' }, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', eventCategory: 'EXCEL_DESKTOP_UNAVAILABLE' };
  if (!fs.existsSync(configuredPath)) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_FILE_NOT_FOUND' }, diagnosticCode: 'EXCEL_FILE_NOT_FOUND', eventCategory: eventFor('EXCEL_FILE_NOT_FOUND') };
  const stat = fs.statSync(configuredPath); result.pathExists = true; result.isFile = stat.isFile(); result.size = stat.size; result.modifiedAt = stat.mtime.toISOString();
  if (!result.isFile || !result.supportedExtension) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_UNSUPPORTED_FILE' }, diagnosticCode: 'EXCEL_UNSUPPORTED_FILE', eventCategory: 'EXCEL_AGENT_ERROR' };
  result.sha256 = await hashFile(configuredPath);
  result.sourceHashBefore = result.sha256;
  const sourceAfter = async () => { result.sourceHashAfter = await hashFile(configuredPath); result.sourceUnchanged = result.sourceHashBefore === result.sourceHashAfter; return result; };
  if (asset.expectedFingerprint && String(asset.expectedFingerprint).toLowerCase() !== result.sha256) { result.expectedFingerprintStatus = 'MISMATCH'; await sourceAfter(); return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_FINGERPRINT_MISMATCH' }, diagnosticCode: 'EXCEL_FINGERPRINT_MISMATCH', eventCategory: eventFor('EXCEL_FINGERPRINT_MISMATCH') }; }
  result.expectedFingerprintStatus = asset.expectedFingerprint ? 'MATCH' : 'NOT_CONFIGURED';
  const shouldProbe = typeof options.probeOpenability === 'function' || job.payload?.requireExcelDesktop === true;
  if (shouldProbe) {
    let tempDir = null; let probePath = configuredPath;
    try {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-excel-probe-'));
      probePath = path.join(tempDir, path.basename(configuredPath));
      fs.copyFileSync(configuredPath, probePath);
      result.disposableCopy = 'CREATED';
      const probed = typeof options.probeOpenability === 'function' ? await options.probeOpenability(probePath) : await probeExcelDesktop(probePath, options.probeTimeoutMs);
      if (typeof probed === 'string') result.openability = probed;
      else if (probed && typeof probed === 'object') { result.openability = probed.openability || (probed.ok ? 'OPENABLE' : 'OPEN_FAILED'); result.excelVersion = probed.excelVersion || null; result.ownedExcelProcessIds = Array.isArray(probed.ownedExcelProcessIds) ? probed.ownedExcelProcessIds : []; }
      if (probed && typeof probed === 'object' && probed.diagnosticCode === 'EXCEL_DESKTOP_UNAVAILABLE') { await sourceAfter(); return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE' }, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', eventCategory: 'EXCEL_DESKTOP_UNAVAILABLE' }; }
      if (['OPEN_FAILED', 'FAILED', 'NOT_VERIFIED'].includes(String(result.openability).toUpperCase())) { await sourceAfter(); return { ok: false, result: { ...result, diagnosticCode: probed?.diagnosticCode || 'EXCEL_OPEN_FAILED' }, diagnosticCode: probed?.diagnosticCode || 'EXCEL_OPEN_FAILED', eventCategory: eventFor('EXCEL_OPEN_FAILED') }; }
    } catch (_) { result.openability = 'OPEN_FAILED'; await sourceAfter(); return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_OPEN_FAILED' }, diagnosticCode: 'EXCEL_OPEN_FAILED', eventCategory: eventFor('EXCEL_OPEN_FAILED') }; }
    finally { if (tempDir) { try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch (_) {} } }
  }
  await sourceAfter();
  if (!result.sourceUnchanged) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_SOURCE_CHANGED' }, diagnosticCode: 'EXCEL_SOURCE_CHANGED', eventCategory: 'EXCEL_AGENT_ERROR' };
  return { ok: true, result, eventCategory: 'EXCEL_HEALTH_RECOVERED' };
}

async function snapshotOrBackup(job, profile, options = {}) {
  const health = await healthcheck(job, profile, options); if (!health.result || !health.result.pathExists) return health;
  const asset = assetFor(job, profile, options); const source = health.result.path; const destination = destinationPath(asset, profile, job.payload); if (!destination) return { ...fail('backup destination is not configured', 'EXCEL_BACKUP_DESTINATION_INVALID'), eventCategory: 'BACKUP_FAILED' };
  fs.mkdirSync(destination, { recursive: true });
  const operationKey = String(job.idempotencyKey || job.payload?.idempotencyKey || job.id);
  const ext = path.extname(source).toLowerCase(); const base = path.basename(source, ext).replace(/[^A-Za-z0-9._-]/g, '_');
  const target = path.join(destination, `${base}_${operationKey.replace(/[^A-Za-z0-9._-]/g, '_')}${ext}`);
  if (!fs.existsSync(target)) fs.copyFileSync(source, target);
  const sourceHash = health.result.sha256 || await hashFile(source); const copyHash = await hashFile(target); const verificationStatus = sourceHash === copyHash ? 'VERIFIED' : 'MISMATCH';
  const result = { operation: job.type === 'EXCEL_SNAPSHOT' ? 'SNAPSHOT' : 'BACKUP', backupId: `bkp_${operationKey}`, projectId: job.projectId, assetId: String(job.payload?.assetId || ''), sourceHash, backupHash: copyHash, sourceSize: health.result.size, copySize: fs.statSync(target).size, createdAt: new Date().toISOString(), runnerId: profile.runnerId || null, localBackupReference: target, correlationId: job.correlationId || null, verificationStatus, deletionAuthorized: false, vbaExecuted: false };
  if (verificationStatus !== 'VERIFIED') return { ok: false, result, diagnosticCode: 'EXCEL_BACKUP_HASH_MISMATCH', eventCategory: 'BACKUP_FAILED' };
  return { ok: true, result, eventCategory: 'BACKUP_COMPLETED' };
}

function reconciliation(job) {
  const input = job.payload?.reconciliationResult || job.payload?.result; if (!input || typeof input !== 'object') return fail('structured reconciliation result is required', 'EXCEL_RECONCILIATION_RESULT_MISSING');
  const status = String(input.status || '').toUpperCase(); if (!RECONCILIATION_STATUSES.includes(status)) return fail('reconciliation status is unsupported', 'EXCEL_RECONCILIATION_STATUS_INVALID');
  return { ok: status === 'MATCH' || status === 'EXPECTED_INTENTIONAL_DIVERGENCE' || status === 'KNOWN_LEGACY_DEFECT', result: { workId: String(input.workId || job.id).slice(0, 160), projectId: job.projectId, destination: String(input.destination || '').slice(0, 200), status, mismatchCount: Math.max(0, Number(input.mismatchCount) || 0), errorCount: Math.max(0, Number(input.errorCount) || 0), retryable: input.retryable === true, hashes: safeMetadata(input.hashes), diagnostics: safeMetadata(input.diagnostics), correlationId: String(input.correlationId || job.correlationId || '').slice(0, 120) || null }, eventCategory: status === 'MATCH' ? 'EXCEL_HEALTH_RECOVERED' : 'EXCEL_RECONCILIATION_ERROR' };
}

async function executeExcelJob(job, profile = {}, options = {}) {
  const type = String(job?.type || '').toUpperCase();
  if (!job?.projectId || (profile.projectId && String(profile.projectId).toLowerCase() !== String(job.projectId).toLowerCase())) return fail('runner profile is not bound to the job project', 'PROJECT_SCOPE_DENIED');
  if (job.payload?.desktopProfileId && profile.id && String(job.payload.desktopProfileId) !== String(profile.id)) return fail('wrong Desktop Profile', 'DESKTOP_PROFILE_MISMATCH');
  if (type === 'EXCEL_HEALTHCHECK') return healthcheck(job, profile, options);
  if (type === 'EXCEL_SNAPSHOT' || type === 'EXCEL_BACKUP') return snapshotOrBackup(job, profile, options);
  if (type === 'EXCEL_RECONCILIATION' || type === 'EXCEL_RECONCILIATION_CHECK') return reconciliation(job);
  if (type === 'EXCEL_SYNC') return { ok: false, error: 'authoritative Excel sync publication is disabled', diagnosticCode: 'EXCEL_SYNC_DISABLED' };
  return fail('unsupported Excel operation');
}

module.exports = { SUPPORTED_EXTENSIONS, safePath, hashFile, probeExcelDesktop, healthcheck, snapshotOrBackup, reconciliation, executeExcelJob };
