const { normalizeProjectKey } = require('./controlPlane/contracts');

const PROVIDER_STATUSES = Object.freeze(['ENABLED', 'DISABLED', 'DEGRADED', 'UNAVAILABLE', 'UNKNOWN']);
const PROVIDER_HEALTH = Object.freeze(['HEALTHY', 'DEGRADED', 'DOWN', 'UNKNOWN']);
const SERVICE_TYPES = Object.freeze(['AI', 'DATABASE', 'TELEGRAM', 'EXCEL', 'BACKUP', 'OTHER']);
const SECRET_KEY = /(token|secret|password|authorization|api[-_]?key|credential|dsn|private[-_]?key)/i;

function text(value, max = 200) { return String(value == null ? '' : value).trim().slice(0, max); }
function fail(error) { return { ok: false, error }; }
function isoOrNull(value) { if (value == null || value === '') return null; const date = new Date(value); return Number.isNaN(date.getTime()) ? null : date.toISOString(); }

function validateProvider(input = {}) {
  const project = normalizeProjectKey(input.project || input.projectId);
  const providerId = text(input.providerId || input.id || input.name, 120).toLowerCase().replace(/[^a-z0-9._-]+/g, '-');
  const name = text(input.name || input.providerName, 120);
  const serviceType = text(input.serviceType || input.type, 30).toUpperCase();
  const status = text(input.status || 'UNKNOWN', 30).toUpperCase();
  const health = text(input.health || 'UNKNOWN', 30).toUpperCase();
  const updatedAt = isoOrNull(input.updatedAt || input.healthUpdatedAt);
  const usage = input.usage && typeof input.usage === 'object' ? input.usage : {};
  const limits = input.limits && typeof input.limits === 'object' ? input.limits : {};
  if (!project) return fail('provider project is invalid or missing');
  if (!providerId || !/^[a-z0-9][a-z0-9._-]{0,119}$/i.test(providerId)) return fail('provider ID is invalid or missing');
  if (!name) return fail('provider name is required');
  if (!SERVICE_TYPES.includes(serviceType)) return fail('provider service type is unsupported');
  if (!PROVIDER_STATUSES.includes(status)) return fail('provider status is unsupported');
  if (!PROVIDER_HEALTH.includes(health)) return fail('provider health is unsupported');
  if ((input.updatedAt || input.healthUpdatedAt) && !updatedAt) return fail('provider timestamp is invalid');
  const forbidden = Object.keys(input).find((key) => SECRET_KEY.test(key));
  if (forbidden) return fail(`provider secret field is not allowed: ${forbidden}`);
  return { ok: true, value: {
    providerId, project, name, serviceType, enabled: input.enabled !== false, status, health, updatedAt,
    usage: {
      requests: Math.max(0, Number(usage.requests) || 0),
      errors: Math.max(0, Number(usage.errors) || 0),
      units: Math.max(0, Number(usage.units) || 0),
      lastUsedAt: isoOrNull(usage.lastUsedAt),
    },
    limits: { requestsPerMinute: Math.max(0, Number(limits.requestsPerMinute) || 0), monthlyUnits: Math.max(0, Number(limits.monthlyUnits) || 0) },
  } };
}

function canAccessProvider(actor = {}, projectId) {
  const project = normalizeProjectKey(projectId);
  if (!project) return false;
  const role = text(actor.role, 30).toLowerCase();
  if (role === 'admin' || role === 'owner') return true;
  return ['project', 'runner'].includes(role) && normalizeProjectKey(actor.projectId || actor.project) === project;
}

function sanitizeProviderForTelegram(provider = {}) {
  const result = { ...provider };
  for (const key of Object.keys(result)) if (SECRET_KEY.test(key)) delete result[key];
  if (result.usage && typeof result.usage === 'object') result.usage = {
    requests: Number(result.usage.requests) || 0,
    errors: Number(result.usage.errors) || 0,
    units: Number(result.usage.units) || 0,
    lastUsedAt: result.usage.lastUsedAt || null,
  };
  if (result.limits && typeof result.limits === 'object') result.limits = {
    requestsPerMinute: Number(result.limits.requestsPerMinute) || 0,
    monthlyUnits: Number(result.limits.monthlyUnits) || 0,
  };
  return result;
}

module.exports = { PROVIDER_STATUSES, PROVIDER_HEALTH, SERVICE_TYPES, validateProvider, canAccessProvider, sanitizeProviderForTelegram };
