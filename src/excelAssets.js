const { normalizeProjectKey } = require('./controlPlane/contracts');

const DESKTOP_STATUSES = Object.freeze(['ONLINE', 'STALE', 'OFFLINE', 'UNKNOWN']);
const ASSET_STATUSES = Object.freeze(['UNVERIFIED', 'VERIFIED', 'MISSING', 'STALE', 'ERROR']);
const SUPPORTED_EXCEL_FILES = Object.freeze(['Mirax.xlsm', 'Gozareshkar.xlsm']);
const PREPARED_EXCEL_JOB_TYPES = Object.freeze(['EXCEL_HEALTHCHECK', 'EXCEL_SNAPSHOT', 'EXCEL_BACKUP']);
const EXCEL_JOB_TIMEOUTS_MS = Object.freeze({ EXCEL_HEALTHCHECK: 60_000, EXCEL_SNAPSHOT: 180_000, EXCEL_BACKUP: 300_000 });

function text(value, max = 200) {
  return String(value == null ? '' : value).trim().slice(0, max);
}

function fail(error) { return { ok: false, error }; }

function isoOrNull(value) {
  if (value == null || value === '') return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function validateDesktopProfile(input = {}) {
  const id = text(input.id, 80);
  const name = text(input.name, 120);
  const runnerId = text(input.runnerId || input.runnerBinding, 120);
  const runnerType = text(input.runnerType || input.runner || 'Windows Runner', 80);
  const excelVersion = text(input.excelVersion || input.excel || 'Excel 365', 80);
  const workbookPaths = Array.isArray(input.workbookPaths) ? input.workbookPaths.map((item) => text(item, 500)) : [];
  const backupLocation = text(input.backupLocation || input.backupPath, 500);
  const status = text(input.status || 'UNKNOWN', 20).toUpperCase();
  if (!/^[a-z0-9][a-z0-9._-]{0,79}$/i.test(id)) return fail('desktop profile id is invalid');
  if (!name) return fail('desktop profile name is required');
  if (!runnerId) return fail('desktop profile runner binding is required');
  if (runnerType !== 'Windows Runner') return fail('desktop profile runner type is unsupported');
  if (excelVersion !== 'Excel 365') return fail('desktop profile Excel version is unsupported');
  if (workbookPaths.some((item) => !/^[A-Za-z]:[\\/]/.test(item) || /[\0\r\n<>"|?*;&`$]/.test(item))) return fail('desktop profile workbook paths must be absolute safe paths');
  if (backupLocation && (!/^[A-Za-z]:[\\/]/.test(backupLocation) || /[\0\r\n<>"|?*;&`$]/.test(backupLocation))) return fail('desktop profile backup location must be absolute and safe');
  if (!DESKTOP_STATUSES.includes(status)) return fail('desktop profile status is unsupported');
  const heartbeat = input.heartbeat && typeof input.heartbeat === 'object' ? input.heartbeat : {};
  const lastSeenAt = isoOrNull(heartbeat.lastSeenAt || input.lastSeenAt);
  const lastSuccessfulAt = isoOrNull(heartbeat.lastSuccessfulAt || input.lastSuccessfulAt || lastSeenAt);
  if ((heartbeat.lastSeenAt || input.lastSeenAt) && !lastSeenAt) return fail('desktop profile heartbeat timestamp is invalid');
  return { ok: true, value: {
    id, name, runnerId, runnerType, excelVersion, workbookPaths, backupLocation: backupLocation || null, status,
    heartbeat: {
      lastSeenAt,
      lastSuccessfulAt,
      intervalMs: Math.max(1000, Number(heartbeat.intervalMs || input.heartbeatIntervalMs || 30_000)),
    },
  } };
}

function validateExcelAsset(input = {}) {
  const project = normalizeProjectKey(input.project || input.projectId);
  const assetName = text(input.assetName || input.name, 120);
  const filename = text(input.filename, 120);
  const manualPath = text(input.manualPath, 500);
  const backupPath = text(input.backupPath, 500);
  const status = text(input.status || 'UNVERIFIED', 20).toUpperCase();
  const verificationTimestamp = isoOrNull(input.verificationTimestamp || input.verifiedAt);
  if (!project) return fail('asset project is invalid or missing');
  if (!assetName) return fail('asset name is required');
  if (!SUPPORTED_EXCEL_FILES.includes(filename)) return fail('asset filename is not supported');
  if (/[\r\n\0]/.test(manualPath) || /[\r\n\0]/.test(backupPath)) return fail('asset path contains invalid characters');
  if (!ASSET_STATUSES.includes(status)) return fail('asset status is unsupported');
  if ((input.verificationTimestamp || input.verifiedAt) && !verificationTimestamp) return fail('asset verification timestamp is invalid');
  return { ok: true, value: { project, assetName, filename, manualPath: manualPath || null, backupPath: backupPath || null, status, verificationTimestamp } };
}

function canAccessProject(actor = {}, projectId, action = 'read') {
  const project = normalizeProjectKey(projectId);
  if (!project) return false;
  const role = text(actor.role, 30).toLowerCase();
  if (role === 'admin' || role === 'owner') return true;
  if (!['project', 'runner'].includes(role)) return false;
  const boundProject = normalizeProjectKey(actor.projectId || actor.project);
  if (boundProject !== project) return false;
  if (role === 'runner' && action === 'request') return false;
  return true;
}

function validatePreparedExcelJob(input = {}, actor = {}) {
  const type = text(input.type, 60).toUpperCase();
  const projectId = normalizeProjectKey(input.projectId || input.project);
  const payload = input.payload && typeof input.payload === 'object' && !Array.isArray(input.payload) ? input.payload : null;
  if (!PREPARED_EXCEL_JOB_TYPES.includes(type)) return fail('unsupported prepared Excel job type');
  if (!projectId) return fail('job projectId is invalid or missing');
  if (!payload) return fail('job payload must be an object');
  if (!canAccessProject(actor, projectId, actor.role === 'runner' ? 'execute' : 'request')) return fail('project scope denied');
  const assetId = text(payload.assetId, 160);
  if (!assetId) return fail('job assetId is required');
  const idempotencyKey = text(input.idempotencyKey || payload.idempotencyKey, 160);
  if ((type === 'EXCEL_SNAPSHOT' || type === 'EXCEL_BACKUP') && !idempotencyKey) return fail('idempotencyKey is required');
  const forbidden = Object.keys(payload).find((key) => /(^|_)(command|shell|powershell|exec|token|secret|password|dsn)$/i.test(key));
  if (forbidden) return fail(`job field is not allowed: ${forbidden}`);
  if (type === 'EXCEL_SNAPSHOT' && text(payload.mode || 'read_only', 20).toLowerCase() !== 'read_only') return fail('snapshot mode must be read_only');
  if (type === 'EXCEL_BACKUP' && !text(payload.destinationRef, 160)) return fail('backup destinationRef is required');
  return { ok: true, value: {
    projectId, type, assetId, idempotencyKey: idempotencyKey || null,
    timeoutMs: Math.min(EXCEL_JOB_TIMEOUTS_MS[type], Math.max(1000, Number(input.timeoutMs || EXCEL_JOB_TIMEOUTS_MS[type]))),
    payload: { ...payload, schemaVersion: Number(payload.schemaVersion || 1) },
    executionAllowed: false,
  } };
}

module.exports = {
  DESKTOP_STATUSES,
  ASSET_STATUSES,
  SUPPORTED_EXCEL_FILES,
  PREPARED_EXCEL_JOB_TYPES,
  EXCEL_JOB_TIMEOUTS_MS,
  validateDesktopProfile,
  validateExcelAsset,
  canAccessProject,
  validatePreparedExcelJob,
};
