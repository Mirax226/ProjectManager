const { normalizeProjectKey } = require('./controlPlane/contracts');

const OPERATIONAL_JOB_TYPES = Object.freeze(['HEALTHCHECK', 'EXCEL_HEALTHCHECK', 'EXCEL_BACKUP', 'EXCEL_SYNC', 'RUN_TESTS']);
const JOB_TIMEOUTS_MS = Object.freeze({ HEALTHCHECK: 60_000, EXCEL_HEALTHCHECK: 60_000, EXCEL_BACKUP: 300_000, EXCEL_SYNC: 600_000, RUN_TESTS: 900_000 });
const FORBIDDEN_KEYS = /(^|_)(command|shell|powershell|exec|token|secret|password|dsn)$/i;

function text(value, max = 200) { return String(value == null ? '' : value).trim().slice(0, max); }
function fail(error) { return { ok: false, error }; }

function canAccessJob(actor = {}, projectId) {
  const project = normalizeProjectKey(projectId);
  if (!project) return false;
  const role = text(actor.role, 30).toLowerCase();
  if (role === 'admin' || role === 'owner') return true;
  return ['project', 'runner'].includes(role) && normalizeProjectKey(actor.projectId || actor.project) === project;
}

function validateOperationalJob(input = {}, actor = {}) {
  const type = text(input.type, 60).toUpperCase();
  const projectId = normalizeProjectKey(input.projectId || input.project);
  const payload = input.payload && typeof input.payload === 'object' && !Array.isArray(input.payload) ? input.payload : {};
  if (!OPERATIONAL_JOB_TYPES.includes(type)) return fail('unsupported operational job type');
  if (!projectId) return fail('job projectId is invalid or missing');
  if (!canAccessJob(actor, projectId)) return fail('project scope denied');
  const forbidden = Object.keys(payload).find((key) => FORBIDDEN_KEYS.test(key));
  if (forbidden) return fail(`job field is not allowed: ${forbidden}`);
  const assetRequired = ['EXCEL_HEALTHCHECK', 'EXCEL_BACKUP', 'EXCEL_SYNC'].includes(type);
  if (assetRequired && !text(payload.assetId, 160)) return fail('assetId is required');
  if (type === 'EXCEL_BACKUP' && !text(payload.destinationRef, 160)) return fail('destinationRef is required');
  if (type === 'EXCEL_SYNC' && text(payload.mode || 'dry_run', 20).toLowerCase() !== 'dry_run') return fail('EXCEL_SYNC mode must be dry_run');
  const idempotencyKey = text(input.idempotencyKey || payload.idempotencyKey, 160);
  if (['EXCEL_BACKUP', 'EXCEL_SYNC'].includes(type) && !idempotencyKey) return fail('idempotencyKey is required');
  return { ok: true, value: { projectId, type, idempotencyKey: idempotencyKey || null, timeoutMs: Math.min(JOB_TIMEOUTS_MS[type], Math.max(1000, Number(input.timeoutMs || JOB_TIMEOUTS_MS[type])),), payload: { ...payload }, executionAllowed: false } };
}

module.exports = { OPERATIONAL_JOB_TYPES, JOB_TIMEOUTS_MS, canAccessJob, validateOperationalJob };
