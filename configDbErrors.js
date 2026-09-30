const crypto = require('node:crypto');

function sanitizeDbErrorMessage(message) {
  if (!message) return null;
  let sanitized = String(message);
  sanitized = sanitized.replace(/postgres(?:ql)?:\/\/[^\s]+/gi, 'postgresql://[REDACTED]');
  sanitized = sanitized.replace(/password=([^\s]+)/gi, 'password=***');
  sanitized = sanitized.replace(/token=([^\s]+)/gi, 'token=***');
  sanitized = sanitized.replace(/key=([^\s]+)/gi, 'key=***');
  return sanitized;
}

function classifyDbError(error) {
  const code = error?.code;
  const message = String(error?.message || '');
  const lower = message.toLowerCase();

  if (code === 'ERR_INVALID_URL' || lower.includes('invalid url')) {
    return 'INVALID_URL';
  }

  if (
    code === 'ETIMEDOUT' ||
    code === 'DB_TIMEOUT' ||
    lower.includes('timeout') ||
    lower.includes('timed out')
  ) {
    return 'DB_TIMEOUT';
  }

  if (code === 'ECONNRESET' || lower.includes('connection terminated unexpectedly')) {
    return 'CONNECTION_TERMINATED_UNEXPECTEDLY';
  }

  if (code === '28P01' || lower.includes('password authentication failed') || lower.includes('authentication failed') || /tenant\s*(?:\/|or)\s*user\b[^\r\n]*\bnot found/i.test(message)) {
    return 'AUTH_FAILED';
  }

  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN' || lower.includes('getaddrinfo')) {
    return 'DNS_FAILED';
  }

  if (
    code === 'SELF_SIGNED_CERT_IN_CHAIN' ||
    code === 'DEPTH_ZERO_SELF_SIGNED_CERT' ||
    lower.includes('ssl') ||
    lower.includes('certificate')
  ) {
    return 'SSL_ERROR';
  }

  return 'UNKNOWN_DB_ERROR';
}

const CONFIG_DB_CATEGORIES = Object.freeze({
  INVALID_URL: 'CONFIG_DB_DSN_INVALID',
  HOST_INVALID: 'CONFIG_DB_HOST_INVALID',
  DNS_FAILED: 'CONFIG_DB_DNS_FAILED',
  AUTH_FAILED: 'CONFIG_DB_AUTH_FAILED',
  DB_TIMEOUT: 'CONFIG_DB_CONNECT_TIMEOUT',
  SSL_ERROR: 'CONFIG_DB_TLS_FAILED',
  QUERY_FAILED: 'CONFIG_DB_QUERY_FAILED',
  UNKNOWN_DB_ERROR: 'UNKNOWN_DB_ERROR',
});

function classifyConfigDbError(error, options = {}) {
  if (options.preflightCategory) return options.preflightCategory;
  const typed = error?.configDbCategory || error?.code;
  if (/^CONFIG_DB_(DSN_MISSING|DSN_INVALID|HOST_INVALID|DNS_FAILED|AUTH_FAILED|CONNECT_TIMEOUT|TLS_FAILED|QUERY_FAILED)$/.test(typed || '')) return typed;
  // A provider's tenant/user lookup failure is distinct from OS getaddrinfo.
  // Some wrappers label both ENOTFOUND; retain actual network DNS errors below.
  if (/tenant\s*(?:\/|or)\s*user\b[^\r\n]*\bnot found/i.test(String(error?.message || ''))) return 'CONFIG_DB_AUTH_FAILED';
  if (/^[0-9A-Z]{5}$/.test(String(error?.code || '')) && error.code !== '28P01') return 'CONFIG_DB_QUERY_FAILED';
  const legacy = classifyDbError(error);
  return CONFIG_DB_CATEGORIES[legacy] || CONFIG_DB_CATEGORIES.UNKNOWN_DB_ERROR;
}

function configDbIncidentFingerprint({ projectId = null, source = null, category, hostname = null, message = '' }) {
  // Known connection/configuration classes have a stable root independent of
  // retry reason, wrapper wording, reference ID, and attempt count.
  const root = ['CONFIG_DB_AUTH_FAILED', 'CONFIG_DB_TLS_FAILED', 'CONFIG_DB_QUERY_FAILED', 'UNKNOWN_DB_ERROR'].includes(category) ? (sanitizeDbErrorMessage(message) || '').replace(/^Config DB warmup (?:failed|exception):\s*/i, '') : '';
  return crypto.createHash('sha256').update(JSON.stringify([projectId || 'global', source || 'unconfigured', category, hostname, root])).digest('hex');
}

function isConfigDbRetryable(category) {
  return ['CONFIG_DB_DNS_FAILED', 'CONFIG_DB_CONNECT_TIMEOUT', 'UNKNOWN_DB_ERROR'].includes(category);
}

module.exports = {
  classifyDbError,
  classifyConfigDbError,
  CONFIG_DB_CATEGORIES,
  configDbIncidentFingerprint,
  isConfigDbRetryable,
  sanitizeDbErrorMessage,
};
