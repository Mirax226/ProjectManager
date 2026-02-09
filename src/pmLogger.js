const crypto = require('crypto');


const SENSITIVE_META_KEY = /(token|secret|password|authorization|api[-_]?key|cookie)/i;
const MAX_STRING_LENGTH = 1500;
const MAX_META_DEPTH = 4;
const MAX_META_KEYS = 50;

function parseBoolean(value, defaultValue = false) {
  if (value == null) return defaultValue;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return defaultValue;
}

function truncateString(value, max = MAX_STRING_LENGTH) {
  const text = String(value == null ? '' : value);
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 1))}…`;
}

function normalizeMetaValue(value, depth = 0) {
  if (value == null) return value;
  if (typeof value === 'string') return truncateString(value);
  if (typeof value !== 'object') return value;
  if (depth >= MAX_META_DEPTH) return '[TRUNCATED]';
  if (Array.isArray(value)) {
    return value.slice(0, MAX_META_KEYS).map((entry) => normalizeMetaValue(entry, depth + 1));
  }
  const clean = {};
  let count = 0;
  for (const [key, entry] of Object.entries(value)) {
    if (count >= MAX_META_KEYS) {
      clean.__truncated__ = true;
      break;
    }
    count += 1;
    if (SENSITIVE_META_KEY.test(key)) {
      clean[key] = '[MASKED]';
      continue;
    }
    clean[key] = normalizeMetaValue(entry, depth + 1);
  }
  return clean;
}

function normalizeMeta(meta) {
  if (!meta || typeof meta !== 'object' || Array.isArray(meta)) return {};
  return normalizeMetaValue(meta, 0);
}

function getCorrelationId(meta) {
  const existing = meta && typeof meta.correlationId === 'string' ? meta.correlationId.trim() : '';
  if (existing) return existing;
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function createPmLogger(options = {}) {
  const env = options.env || process.env;
  const fetchImpl = options.fetch || global.fetch || require('node-fetch');
  const baseUrl = (options.pmUrl || env.PM_URL || env.PATH_APPLIER_URL || '').trim().replace(/\/$/, '');
  const ingestToken = (options.ingestToken || env.PM_INGEST_TOKEN || env.PM_TOKEN || '').trim();
  const testEnabled = parseBoolean(options.testEnabled ?? env.PM_TEST_ENABLED, false);
  const testToken = (options.testToken || env.PM_TEST_TOKEN || '').trim();
  const enabled = Boolean(baseUrl && ingestToken);
  const state = {
    lastSend: null,
    hooksInstalled: false,
  };

  async function send(level, message, meta) {
    const normalizedMeta = normalizeMeta(meta);
    const correlationId = getCorrelationId(normalizedMeta);
    normalizedMeta.correlationId = correlationId;

    const requestMeta = {
      at: new Date().toISOString(),
      level: String(level || 'info'),
      ok: false,
      statusCode: null,
      correlationId,
      skipped: false,
    };

    if (!enabled) {
      requestMeta.ok = false;
      requestMeta.skipped = true;
      requestMeta.error = 'disabled';
      state.lastSend = requestMeta;
      return { ok: false, skipped: true, correlationId };
    }

    try {
      const payload = {
        level: requestMeta.level,
        message: truncateString(message == null ? '' : String(message)),
        meta: normalizedMeta,
      };
      const ingestPaths = ['/api/logs', '/api/pm/logs'];
      let response = null;
      for (const ingestPath of ingestPaths) {
        response = await fetchImpl(`${baseUrl}${ingestPath}`, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${ingestToken}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(payload),
        });
        if (response.ok) {
          break;
        }
        if (response.status !== 404) {
          break;
        }
      }
      requestMeta.statusCode = response ? response.status : null;
      requestMeta.ok = response ? response.ok : false;
      if (!requestMeta.ok) requestMeta.error = `status_${requestMeta.statusCode || 'unknown'}`;
      state.lastSend = requestMeta;
      return { ok: requestMeta.ok, correlationId, statusCode: requestMeta.statusCode };
    } catch (error) {
      requestMeta.error = error?.message || 'request_failed';
      state.lastSend = requestMeta;
      return { ok: false, correlationId, error: requestMeta.error };
    }
  }

  function diagnostics() {
    return {
      flags: {
        enabled,
        hasPmUrl: Boolean(baseUrl),
        hasIngestToken: Boolean(ingestToken),
        testEnabled,
        hasTestToken: Boolean(testToken),
        hooksInstalled: state.hooksInstalled,
      },
      lastSend: state.lastSend,
    };
  }

  function isTestRequestAllowed(token) {
    if (!testEnabled || !testToken) return false;
    return token === testToken;
  }

  function attachProcessHooks() {
    if (state.hooksInstalled) return;
    state.hooksInstalled = true;
    process.on('unhandledRejection', (reason) => {
      const error = reason instanceof Error ? reason : new Error(String(reason || 'Unhandled rejection'));
      send('error', 'unhandledRejection', {
        source: 'process',
        error: truncateString(error.message, 600),
        stack: truncateString(error.stack || '', 2500),
      });
    });
    process.on('uncaughtException', (error) => {
      const resolved = error instanceof Error ? error : new Error(String(error || 'Uncaught exception'));
      send('error', 'uncaughtException', {
        source: 'process',
        error: truncateString(resolved.message, 600),
        stack: truncateString(resolved.stack || '', 2500),
      });
    });
  }

  return {
    send,
    info: (message, meta) => send('info', message, meta),
    warn: (message, meta) => send('warn', message, meta),
    error: (message, meta) => send('error', message, meta),
    diagnostics,
    isTestRequestAllowed,
    attachProcessHooks,
  };
}

function attachAxiosInterceptor(axiosInstance, pmLogger) {
  if (!axiosInstance || !axiosInstance.interceptors || !pmLogger) return false;
  axiosInstance.interceptors.response.use(
    (response) => response,
    async (error) => {
      const config = error?.config || {};
      const response = error?.response;
      await pmLogger.error('axios_request_failed', {
        url: config.url,
        method: config.method,
        statusCode: response?.status,
        correlationId: response?.headers?.['x-correlation-id'] || config?.headers?.['x-correlation-id'],
      });
      return Promise.reject(error);
    },
  );
  return true;
}

function createPmFetch(fetchFn, pmLogger) {
  if (!fetchFn || !pmLogger) {
    throw new Error('createPmFetch requires fetch implementation and logger');
  }
  return async function pmFetch(url, options = {}) {
    const response = await fetchFn(url, options);
    if (response && !response.ok) {
      await pmLogger.error('fetch_request_failed', {
        url: String(url),
        method: options.method || 'GET',
        statusCode: response.status,
        correlationId: response.headers?.get?.('x-correlation-id') || null,
      });
    }
    return response;
  };
}

module.exports = {
  createPmLogger,
  attachAxiosInterceptor,
  createPmFetch,
};
