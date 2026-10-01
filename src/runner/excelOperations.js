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
function eventFor(code) { return ({ EXCEL_FILE_NOT_FOUND: 'EXCEL_FILE_NOT_FOUND', EXCEL_FINGERPRINT_MISMATCH: 'EXCEL_FINGERPRINT_MISMATCH', EXCEL_OPEN_FAILED: 'EXCEL_OPEN_FAILED', COM_CREATE_FAILED: 'EXCEL_OPEN_FAILED', EXCEL_CONFIGURATION_FAILED: 'EXCEL_OPEN_FAILED', WORKBOOK_OPEN_FAILED: 'EXCEL_OPEN_FAILED', WORKBOOK_READ_FAILED: 'EXCEL_OPEN_FAILED', WORKBOOK_CLOSE_FAILED: 'EXCEL_OPEN_FAILED', EXCEL_QUIT_FAILED: 'EXCEL_OPEN_FAILED', TIMEOUT: 'EXCEL_OPEN_FAILED', COPY_CLEANUP_FAILED: 'EXCEL_OPEN_FAILED', EXCEL_BACKUP_HASH_MISMATCH: 'EXCEL_BACKUP_HASH_MISMATCH' })[code] || 'EXCEL_AGENT_ERROR'; }
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

const EXCEL_DESKTOP_SCRIPT = path.resolve(__dirname, '../../tools/excel-desktop-probe.ps1');

async function probeExcelDesktop(filePath, timeoutMs = 60_000, options = {}) {
  if (process.platform !== 'win32') return { ok: false, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', openability: 'NOT_VERIFIED' };
  const spawnProcess = options.spawn || spawn;
  const probeStartedAt = Date.now();
  const stateRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-excel-state-'));
  const statePath = path.join(stateRoot, 'state.json');
  const env = { ...process.env, PJ_EXCEL_PROBE_PATH: filePath, PJ_EXCEL_PROBE_STATE: statePath, PJ_EXCEL_SUPPRESS_REFRESH: options.suppressExternalRefresh === false ? 'false' : 'true' };
  let timedOut = false;
  let gracefulQuitTimedOut = false;
  let savedState = null;
  const invoke = (cleanup) => new Promise((resolve) => {
    let stdout = ''; let settled = false;
    const args = ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', EXCEL_DESKTOP_SCRIPT, ...(cleanup ? ['-CleanupOnly'] : [])];
    const child = spawnProcess('powershell.exe', args, { shell: false, windowsHide: true, env });
    const timer = setTimeout(() => {
      if (!cleanup) timedOut = true;
      child.kill();
    }, cleanup ? 15_000 : Math.min(120_000, Math.max(1000, Number(timeoutMs) || 60_000)));
    let quitStartedAt = null;
    const quitWatch = cleanup ? null : setInterval(() => {
      try {
        const state = JSON.parse(fs.readFileSync(statePath, 'utf8').replace(/^\uFEFF/, ''));
        if (state.stage === 'EXCEL_QUIT') {
          quitStartedAt ||= Date.now();
          if (Date.now() - quitStartedAt >= 5000) { timedOut = true; gracefulQuitTimedOut = true; child.kill(); }
        }
      } catch (_) {}
    }, 250);
    const finish = (value) => { if (settled) return; settled = true; clearTimeout(timer); clearInterval(quitWatch); resolve(value); };
    child.stdout?.on('data', (chunk) => { stdout = (stdout + chunk).slice(0, 16000); });
    child.stderr?.on('data', () => {});
    child.on('error', () => finish(null));
    child.on('close', () => {
      try { finish(JSON.parse(stdout.trim().replace(/^\uFEFF/, ''))); } catch (_) { finish(null); }
    });
  });
  try {
    const parsed = await invoke(false);
    const durationMs = Date.now() - probeStartedAt;
    try { savedState = JSON.parse(fs.readFileSync(statePath, 'utf8').replace(/^\uFEFF/, '')); } catch (_) {}
    // Always run cleanup in a separate process: a killed COM worker cannot run
    // its finally block. Ownership comes from the HWND and creation timestamp.
    const cleanupStartedAt = Date.now();
    const cleanup = await invoke(true);
    const cleanupDurationMs = Date.now() - cleanupStartedAt;
    const result = timedOut
      ? { ok: false, diagnosticCode: 'TIMEOUT', failedStage: savedState?.stage || 'COM_CREATE', failedSubstage: savedState?.substage || null, safeError: { exceptionClass: 'TimeoutError', hresult: null, message: `Excel diagnostic timed out at ${savedState?.stage || 'COM_CREATE'}` }, stages: savedState?.stages || [], ownedProcessIds: (savedState?.owned || []).map((entry) => entry.id) }
      : parsed || { ok: false, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', failedStage: savedState?.stage || 'COM_CREATE' };
    const cleanupVerified = cleanup?.cleanupVerified === true && (savedState?.owned?.length > 0 || cleanup.noNewExcelProcessesObserved === true);
    const quitRecovered = timedOut && savedState?.stage === 'EXCEL_QUIT' && ['WORKBOOK_OPEN_SUCCESS', 'WORKBOOK_READ_PROBE', 'WORKBOOK_CLOSE', 'EXCEL_QUIT'].every((stage) => savedState?.stages?.includes(stage)) && savedState?.owned?.length > 0 && cleanupVerified;
    if (quitRecovered) { gracefulQuitTimedOut = true; result.ok = true; result.diagnosticCode = null; result.lifecycleWarningCode = 'EXCEL_QUIT_TIMEOUT_RECOVERED'; result.safeError = { exceptionClass: 'TimeoutError', hresult: null, message: 'Excel.Quit timed out; verified owned process cleaned up' }; }
    if (result.ok && !cleanupVerified) { result.ok = false; result.diagnosticCode = 'EXCEL_PROCESS_CLEANUP_FAILED'; result.failedStage = 'EXCEL_QUIT'; }
    const workbookOpened = (result.stages || savedState?.stages || []).includes('WORKBOOK_OPEN_SUCCESS');
    return { ...result, durationMs: result.durationMs ?? durationMs, cleanupDurationMs, workbookOpened, stageTimings: result.stageTimings || savedState?.stageTimings || [], processOwnership: result.processOwnership || savedState?.owned || [], hostSessionId: result.hostSessionId ?? savedState?.hostSessionId ?? null, interactive: result.interactive ?? savedState?.interactive ?? null, openability: result.ok || workbookOpened ? 'OPENABLE' : 'OPEN_FAILED', excelVersion: result.version || savedState?.excelVersion || null, externalRefreshSuppressed: result.externalRefreshSuppressed ?? savedState?.externalRefreshSuppressed ?? null, refreshFlagsCleared: result.refreshFlagsCleared ?? savedState?.refreshFlagsCleared ?? null, queryTablesDisabled: result.queryTablesDisabled ?? savedState?.queryTablesDisabled ?? null, calculationDisabled: result.calculationDisabled ?? savedState?.calculationDisabled ?? null, vbaProjectUnchanged: result.vbaProjectUnchanged ?? savedState?.vbaProjectUnchanged ?? null, gracefulQuitTimedOut, ownedExcelProcessIds: result.ownedProcessIds || [], cleanupVerified, ownershipVerified: Boolean(savedState?.owned?.length), remainingOwnedProcessIds: cleanup?.remainingOwnedProcessIds || [], unverifiedNewExcelProcessIds: cleanup?.unverifiedNewExcelProcessIds || [] };
  } finally {
    fs.rmSync(stateRoot, { recursive: true, force: true });
  }
}

async function healthcheck(job, profile, options = {}) {
  const asset = assetFor(job, profile, options); if (!asset) return fail('Excel asset is not configured', 'EXCEL_ASSET_NOT_CONFIGURED');
  if (asset.projectId && String(asset.projectId).toLowerCase() !== String(job.projectId).toLowerCase()) return fail('Excel asset is outside the job project', 'PROJECT_SCOPE_DENIED');
  const startedAt = Date.now(); const stages = []; const mark = (stage) => { stages.push({ stage, atMs: Date.now() - startedAt }); };
  mark('ASSET_RESOLVE'); const configuredPath = sourcePath(asset); const result = { projectId: job.projectId, assetId: String(job.payload?.assetId || ''), configurationPresent: true, path: configuredPath, pathExists: false, isFile: false, supportedExtension: false, size: null, modifiedAt: null, sha256: null, sourceHashBefore: null, sourceHashAfter: null, sourceUnchanged: null, excelDesktopAvailable: process.platform === 'win32', openability: 'NOT_CHECKED', disposableCopy: 'NOT_CREATED', disposableCopyUsed: false, ownedExcelProcessIds: [], macrosDisabled: true, eventsDisabled: true, linkUpdatesDisabled: true, expectedFingerprintStatus: 'NOT_CONFIGURED', diagnosticCode: null, operation: 'EXCEL_HEALTHCHECK', failedStage: null, safeErrorClass: null, durationMs: null, stages };
  if (!configuredPath) return { ok: false, result: { ...result, configurationPresent: false, diagnosticCode: 'EXCEL_PATH_INVALID', durationMs: Date.now() - startedAt }, diagnosticCode: 'EXCEL_PATH_INVALID', eventCategory: 'EXCEL_AGENT_ERROR' };
  mark('SOURCE_EXISTS');
  result.supportedExtension = SUPPORTED_EXTENSIONS.includes(path.extname(configuredPath).toLowerCase());
  if (job.payload?.requireExcelDesktop === true && !result.excelDesktopAvailable) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE' }, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', eventCategory: 'EXCEL_DESKTOP_UNAVAILABLE' };
  if (!fs.existsSync(configuredPath)) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_FILE_NOT_FOUND', durationMs: Date.now() - startedAt }, diagnosticCode: 'EXCEL_FILE_NOT_FOUND', eventCategory: eventFor('EXCEL_FILE_NOT_FOUND') };
  const stat = fs.statSync(configuredPath); result.pathExists = true; result.isFile = stat.isFile(); result.size = stat.size; result.modifiedAt = stat.mtime.toISOString();
  if (!result.isFile || !result.supportedExtension) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_UNSUPPORTED_FILE' }, diagnosticCode: 'EXCEL_UNSUPPORTED_FILE', eventCategory: 'EXCEL_AGENT_ERROR' };
  mark('SOURCE_HASH_BEFORE'); result.sha256 = await hashFile(configuredPath);
  result.sourceHashBefore = result.sha256;
  const sourceAfter = async () => { mark('SOURCE_HASH_AFTER'); result.sourceHashAfter = await hashFile(configuredPath); result.sourceUnchanged = result.sourceHashBefore === result.sourceHashAfter; return result; };
  if (asset.expectedFingerprint && String(asset.expectedFingerprint).toLowerCase() !== result.sha256) { result.expectedFingerprintStatus = 'MISMATCH'; await sourceAfter(); return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_FINGERPRINT_MISMATCH' }, diagnosticCode: 'EXCEL_FINGERPRINT_MISMATCH', eventCategory: eventFor('EXCEL_FINGERPRINT_MISMATCH') }; }
  result.expectedFingerprintStatus = asset.expectedFingerprint ? 'MATCH' : 'NOT_CONFIGURED';
  const shouldProbe = typeof options.probeOpenability === 'function' || job.payload?.requireExcelDesktop === true;
  if (shouldProbe) {
    let tempDir = null; let probePath = configuredPath;
    try {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-excel-probe-')); mark('COPY_CREATE');
      probePath = path.join(tempDir, path.basename(configuredPath));
      fs.copyFileSync(configuredPath, probePath);
      result.disposableCopy = 'CREATED'; result.disposableCopyUsed = true;
      const probed = typeof options.probeOpenability === 'function' ? await options.probeOpenability(probePath) : await probeExcelDesktop(probePath, options.probeTimeoutMs);
      if (typeof probed === 'string') result.openability = probed;
      else if (probed && typeof probed === 'object') { result.openability = probed.openability || (probed.ok ? 'OPENABLE' : 'OPEN_FAILED'); result.excelVersion = probed.excelVersion || null; result.ownedExcelProcessIds = Array.isArray(probed.ownedExcelProcessIds) ? probed.ownedExcelProcessIds : []; result.failedStage = probed.failedStage || null; result.safeErrorClass = probed.safeError?.exceptionClass || null; result.safeError = probed.safeError ? { exceptionClass: String(probed.safeError.exceptionClass || '').slice(0, 80), hresult: String(probed.safeError.hresult || '').slice(0, 20), message: String(probed.safeError.message || '').slice(0, 240) } : null; result.probeDurationMs = Number.isFinite(probed.durationMs) ? probed.durationMs : null; result.probeStageTimings = probed.stageTimings || []; result.processOwnership = probed.processOwnership || []; result.hostSessionId = probed.hostSessionId ?? null; result.interactive = probed.interactive ?? null; result.excelProcessCleanupVerified = probed.cleanupVerified ?? null; result.excelProcessOwnershipVerified = probed.ownershipVerified ?? null; result.unverifiedNewExcelProcessIds = probed.unverifiedNewExcelProcessIds || []; result.externalRefreshSuppressed = probed.externalRefreshSuppressed ?? null; result.queryTablesDisabled = probed.queryTablesDisabled ?? null; result.calculationDisabled = probed.calculationDisabled ?? null; result.refreshFlagsCleared = probed.refreshFlagsCleared ?? null; result.vbaProjectUnchanged = probed.vbaProjectUnchanged ?? null; result.lifecycleWarningCode = probed.lifecycleWarningCode ?? null; result.gracefulQuitTimedOut = probed.gracefulQuitTimedOut ?? false; }
      if (probed && typeof probed === 'object') { result.failedSubstage = typeof probed.failedSubstage === 'string' ? probed.failedSubstage.slice(0, 80) : null; result.workbookOpened = probed.workbookOpened ?? null; result.probeCleanupDurationMs = Number.isFinite(probed.cleanupDurationMs) ? probed.cleanupDurationMs : null; }
      if (Array.isArray(probed?.stages)) probed.stages.forEach((stage) => { if (typeof stage === 'string' && !result.stages.some((entry) => entry.stage === stage)) mark(stage); });
      if (result.failedStage && !result.stages.some((entry) => entry.stage === result.failedStage)) mark(result.failedStage);
      if (probed && typeof probed === 'object' && probed.diagnosticCode === 'EXCEL_DESKTOP_UNAVAILABLE') { await sourceAfter(); return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE' }, diagnosticCode: 'EXCEL_DESKTOP_UNAVAILABLE', eventCategory: 'EXCEL_DESKTOP_UNAVAILABLE' }; }
      if (probed?.ok === false || ['OPEN_FAILED', 'FAILED', 'NOT_VERIFIED'].includes(String(result.openability).toUpperCase())) { await sourceAfter(); const code = probed?.diagnosticCode || 'EXCEL_OPEN_FAILED'; return { ok: false, result: { ...result, diagnosticCode: code, durationMs: Date.now() - startedAt }, diagnosticCode: code, eventCategory: eventFor(code) }; }
    } catch (error) { result.openability = 'OPEN_FAILED'; result.failedStage = result.failedStage || 'COPY_CREATE'; result.safeErrorClass = error?.constructor?.name || 'Error'; await sourceAfter(); const code = result.failedStage === 'COPY_CREATE' ? 'COPY_CREATE_FAILED' : 'EXCEL_OPEN_FAILED'; return { ok: false, result: { ...result, diagnosticCode: code, durationMs: Date.now() - startedAt }, diagnosticCode: code, eventCategory: eventFor(code) }; }
    finally { if (tempDir) { try { mark('COPY_DELETE'); fs.rmSync(tempDir, { recursive: true, force: true }); } catch (_) { result.cleanupDiagnosticCode = 'COPY_CLEANUP_FAILED'; } } }
  }
  await sourceAfter();
  if (!result.sourceUnchanged) return { ok: false, result: { ...result, diagnosticCode: 'EXCEL_SOURCE_CHANGED' }, diagnosticCode: 'EXCEL_SOURCE_CHANGED', eventCategory: 'EXCEL_AGENT_ERROR' };
  result.durationMs = Date.now() - startedAt; return { ok: true, result, eventCategory: 'EXCEL_HEALTH_RECOVERED' };
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
