function randomId() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function') return globalThis.crypto.randomUUID();
  return `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

const EVENT_CATEGORIES = Object.freeze([
  'APP_ERROR', 'D1_ERROR', 'TELEGRAM_ERROR', 'EXCEL_AGENT_ERROR',
  'EXCEL_RECONCILIATION_ERROR', 'EXCEL_OFFLINE', 'EXCEL_RECOVERED',
  'EXCEL_SYNC_FAILED', 'API_FAILED', 'AI_TIMEOUT', 'PROVIDER_UNAVAILABLE', 'BACKUP_FAILED',
  'SYNC_BACKLOG', 'HEALTHCHECK_FAILED', 'HEALTHCHECK_RECOVERED', 'CONFIG_CHANGED',
  'ARCHIVE_CREATED', 'ARCHIVE_FAILED', 'ARCHIVE_RESTORED', 'BACKUP_COMPLETED',
  'EXCEL_FILE_NOT_FOUND', 'EXCEL_FINGERPRINT_MISMATCH', 'EXCEL_DESKTOP_UNAVAILABLE',
  'EXCEL_OPEN_FAILED', 'EXCEL_HEALTH_RECOVERED', 'EXCEL_BACKUP_HASH_MISMATCH',
  'DEPLOYMENT_ERROR', 'RUNNER_OFFLINE', 'RUNNER_RECOVERED',
]);
const SEVERITIES = Object.freeze(['INFO', 'WARN', 'ERROR', 'CRITICAL']);
const HEALTH_STATES = Object.freeze(['HEALTHY', 'DEGRADED', 'DOWN', 'UNKNOWN']);
const JOB_TYPES = Object.freeze([
  'HEALTHCHECK', 'PROJECT_STATUS', 'GIT_STATUS', 'RUN_TESTS', 'RUN_TYPECHECK',
  'CODEX_TASK', 'DAILYSYSTEM_HEALTHCHECK', 'EXCEL_CONNECTIVITY_TEST',
  'EXCEL_SYNC_DIAGNOSTIC', 'EXCEL_RECONCILIATION_CHECK', 'EXCEL_HEALTHCHECK',
  'EXCEL_SNAPSHOT', 'EXCEL_BACKUP', 'EXCEL_RECONCILIATION', 'EXCEL_SYNC',
]);
const JOB_STATUSES = Object.freeze(['PENDING', 'CLAIMED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED']);
const JOB_RISKS = Object.freeze(['READ_ONLY', 'CONTROLLED', 'DANGEROUS']);
const JOB_RISK = Object.freeze({
  HEALTHCHECK: 'READ_ONLY', PROJECT_STATUS: 'READ_ONLY', GIT_STATUS: 'READ_ONLY',
  RUN_TESTS: 'READ_ONLY', RUN_TYPECHECK: 'READ_ONLY', DAILYSYSTEM_HEALTHCHECK: 'READ_ONLY',
  EXCEL_CONNECTIVITY_TEST: 'READ_ONLY', EXCEL_SYNC_DIAGNOSTIC: 'READ_ONLY',
  EXCEL_RECONCILIATION_CHECK: 'READ_ONLY', EXCEL_HEALTHCHECK: 'READ_ONLY',
  EXCEL_SNAPSHOT: 'CONTROLLED', EXCEL_BACKUP: 'CONTROLLED', EXCEL_RECONCILIATION: 'READ_ONLY',
  EXCEL_SYNC: 'CONTROLLED', CODEX_TASK: 'CONTROLLED',
});

function string(value, max = 200) {
  return String(value == null ? '' : value).trim().slice(0, max);
}

function normalizeProjectKey(value) {
  const key = string(value, 80).toLowerCase();
  return /^[a-z0-9][a-z0-9._-]{0,79}$/.test(key) ? key : null;
}

function redact(value, depth = 0) {
  if (depth > 5) return '[TRUNCATED]';
  if (value == null || typeof value === 'number' || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    if (/(token|secret|password|authorization|api[-_]?key|cookie|dsn|private[-_]?key)/i.test(value)) return '[REDACTED]';
    return value.slice(0, 1000);
  }
  if (Array.isArray(value)) return value.slice(0, 50).map((item) => redact(item, depth + 1));
  if (typeof value === 'object') {
    const out = {};
    for (const [key, item] of Object.entries(value).slice(0, 50)) {
      out[key] = /(token|secret|password|authorization|api[-_]?key|cookie|dsn|private[-_]?key)/i.test(key)
        ? '[REDACTED]' : redact(item, depth + 1);
    }
    return out;
  }
  return String(value).slice(0, 1000);
}

function safeJsonSize(value) {
  try { const text = JSON.stringify(value); return typeof TextEncoder === 'function' ? new TextEncoder().encode(text).length : text.length; } catch (_) { return Infinity; }
}

function validateOperationalEvent(input, options = {}) {
  const nowMs = options.nowMs == null ? Date.now() : Number(options.nowMs);
  const maxSkewMs = options.maxSkewMs == null ? 24 * 60 * 60 * 1000 : Number(options.maxSkewMs);
  if (!input || typeof input !== 'object' || Array.isArray(input)) return { ok: false, error: 'event must be an object' };
  const schemaVersion = Number(input.schemaVersion);
  const project = normalizeProjectKey(input.project || input.projectId);
  const environment = string(input.environment || input.env, 40);
  const category = string(input.category, 80).toUpperCase();
  const severity = string(input.severity || 'INFO', 20).toUpperCase();
  const message = string(input.message, 4000);
  const source = string(input.source, 120);
  const component = string(input.component || input.source, 120);
  const correlationId = string(input.correlationId || input.context?.correlationId, 120) || null;
  const timestampValue = input.timestamp || new Date(nowMs).toISOString();
  const timestampMs = Date.parse(timestampValue);
  if (schemaVersion !== 1) return { ok: false, error: 'schemaVersion must be 1' };
  if (!project) return { ok: false, error: 'project is invalid or missing' };
  if (!environment) return { ok: false, error: 'environment is required' };
  if (!EVENT_CATEGORIES.includes(category)) return { ok: false, error: 'category is unsupported' };
  if (!SEVERITIES.includes(severity)) return { ok: false, error: 'severity is unsupported' };
  if (!message) return { ok: false, error: 'message is required' };
  if (!source) return { ok: false, error: 'source is required' };
  if (!Number.isFinite(timestampMs) || Math.abs(nowMs - timestampMs) > maxSkewMs) return { ok: false, error: 'timestamp is outside the replay window' };
  const event = {
    schemaVersion: 1, eventId: string(input.eventId, 120) || randomId(), project, environment,
    severity, category, component, timestamp: new Date(timestampMs).toISOString(), message,
    context: redact(input.context && typeof input.context === 'object' && !Array.isArray(input.context) ? input.context : {}),
    source, correlationId,
  };
  if (safeJsonSize(event) > (options.maxBytes || 16 * 1024)) return { ok: false, error: 'event is too large' };
  return { ok: true, value: event };
}

function normalizeJobType(value) { return string(value, 80).toUpperCase(); }
function classifyJob(type) { return JOB_RISK[normalizeJobType(type)] || null; }

module.exports = {
  EVENT_CATEGORIES, SEVERITIES, HEALTH_STATES, JOB_TYPES, JOB_STATUSES, JOB_RISKS, JOB_RISK,
  normalizeProjectKey, redact, validateOperationalEvent, normalizeJobType, classifyJob,
};
