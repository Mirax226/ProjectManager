const fetchImpl = global.fetch || require('node-fetch');

const DEFAULT_RATE_LIMIT_PER_MINUTE = 20;
const DEFAULT_RESPONSE_SNIPPET_LIMIT = 500;

function parseBoolean(value, defaultValue = false) {
  if (value == null) return defaultValue;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return defaultValue;
}

function truncateText(value, limit) {
  if (!value) return '';
  const text = String(value);
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 1))}…`;
}

function normalizeMeta(meta) {
  if (!meta || typeof meta !== 'object' || Array.isArray(meta)) {
    return {};
  }
  return { ...meta };
}

function createRateLimiter(maxPerMinute) {
  const entries = new Map();
  const windowMs = 60_000;

  return function isAllowed(projectId, nowMs) {
    if (!projectId) return false;
    const state = entries.get(projectId) || { timestamps: [] };
    const cutoff = nowMs - windowMs;
    state.timestamps = state.timestamps.filter((value) => value >= cutoff);
    if (state.timestamps.length >= maxPerMinute) {
      entries.set(projectId, state);
      return false;
    }
    state.timestamps.push(nowMs);
    entries.set(projectId, state);
    return true;
  };
}

function getHeaderValue(headers, key) {
  if (!headers) return null;
  const target = String(key).toLowerCase();
  for (const [headerKey, headerValue] of Object.entries(headers)) {
    if (String(headerKey).toLowerCase() === target) {
      if (Array.isArray(headerValue)) return headerValue[0];
      return headerValue;
    }
  }
  return null;
}

function formatResponseSnippet(data) {
  if (data == null) return '';
  if (typeof data === 'string') return truncateText(data, DEFAULT_RESPONSE_SNIPPET_LIMIT);
  try {
    return truncateText(JSON.stringify(data), DEFAULT_RESPONSE_SNIPPET_LIMIT);
  } catch (error) {
    return truncateText(String(data), DEFAULT_RESPONSE_SNIPPET_LIMIT);
  }
}

function createPathApplierLogger(options = {}) {
  const env = options.env || process.env;

  const enabled =
    options.enabled ?? parseBoolean(env.PATH_APPLIER_LOGGER_ENABLED, true);
  const url = options.url || env.PATH_APPLIER_URL;
  const token = options.token || env.PATH_APPLIER_TOKEN;
  const project = options.project || env.PROJECT_NAME;
  const nodeEnv = options.nodeEnv || env.NODE_ENV || 'development';
  const rateLimitPerMinute =
    options.rateLimitPerMinute ||
    Number(env.PATH_APPLIER_LOG_RATE_LIMIT_PER_MINUTE || DEFAULT_RATE_LIMIT_PER_MINUTE);
  const now = options.now || (() => Date.now());
  const rateLimiter = createRateLimiter(rateLimitPerMinute);

  async function send(level, message, meta) {
    const resolvedMeta = normalizeMeta(meta);
    if (!enabled) {
      return false;
    }
    if (!url || !token || !project) {
      console.warn('[path-applier-logger] missing configuration', {
        hasUrl: Boolean(url),
        hasToken: Boolean(token),
        hasProject: Boolean(project),
        nodeEnv,
      });
      return false;
    }
    if (resolvedMeta.log_ingest_failed) {
      return false;
    }

    const nowMs = now();
    if (!rateLimiter(project, nowMs)) {
      console.warn('[path-applier-logger] rate limit reached', {
        project,
        rateLimitPerMinute,
      });
      return false;
    }

    const payload = {
      project,
      level,
      message: message == null ? '' : String(message),
      meta: resolvedMeta,
      timestamp: new Date(nowMs).toISOString(),
    };

    try {
      const response = await fetchImpl(url, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const body = await response.text().catch(() => '');
        console.error('[path-applier-logger] log ingest failed', {
          status: response.status,
          responseSnippet: truncateText(body, DEFAULT_RESPONSE_SNIPPET_LIMIT),
          meta: { ...resolvedMeta, log_ingest_failed: true },
        });
        return false;
      }
      return true;
    } catch (error) {
      console.error('[path-applier-logger] log ingest failed', {
        error: error?.message || error,
        meta: { ...resolvedMeta, log_ingest_failed: true },
      });
      return false;
    }
  }

  return {
    info: (message, meta) => send('info', message, meta),
    warn: (message, meta) => send('warn', message, meta),
    error: (message, meta) => send('error', message, meta),
  };
}

function attachAxiosInterceptors(axiosInstance, logger) {
  if (!axiosInstance || !logger) {
    throw new Error('attachAxiosInterceptors requires axios instance and logger');
  }

  axiosInstance.interceptors.response.use(
    (response) => response,
    async (error) => {
      const config = error?.config || {};
      if (config.__isPathApplierLog) {
        return Promise.reject(error);
      }
      const response = error?.response;
      const correlationId =
        getHeaderValue(response?.headers, 'x-correlation-id') ||
        getHeaderValue(config?.headers, 'x-correlation-id');
      const meta = {
        url: config?.url,
        method: config?.method ? String(config.method).toUpperCase() : undefined,
        status: response?.status,
        responseSnippet: formatResponseSnippet(response?.data),
        correlationId,
      };
      try {
        await logger.error('HTTP request failed', meta);
      } catch (logError) {
        console.error('[path-applier-logger] failed to record axios error', logError);
      }
      return Promise.reject(error);
    },
  );
}

module.exports = {
  createPathApplierLogger,
  attachAxiosInterceptors,
};
