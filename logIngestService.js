const crypto = require('crypto');
const { normalizeLogLevel, LOG_LEVELS } = require('./logLevels');
const { sanitizeTelegramText } = require('./telegramApi');

const DEFAULT_DEDUPE_WINDOW_SEC = 180;
const DEFAULT_RATE_LIMIT_PER_MIN = 20;
const DEFAULT_RATE_LIMIT_BURST = 10;

function truncateText(value, limit) {
  if (!value) return '';
  const text = String(value);
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 1))}…`;
}

function formatContext(context, limit) {
  if (!context || typeof context !== 'object') return '';
  try {
    return truncateText(JSON.stringify(context), limit);
  } catch (error) {
    return '';
  }
}

function parseTimestamp(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function validatePayload(body) {
  if (!body || typeof body !== 'object') {
    return { ok: false, error: 'Payload must be a JSON object.' };
  }

  const projectId = body.projectId != null ? String(body.projectId).trim() : '';
  const service = body.service != null ? String(body.service).trim() : '';
  const env = body.env != null ? String(body.env).trim() : '';
  const level = normalizeLogLevel(body.level);
  const message = body.message != null ? String(body.message) : '';
  const timestamp = parseTimestamp(body.timestamp) || new Date().toISOString();

  if (!projectId) {
    return { ok: false, error: 'projectId is required.' };
  }
  if (!service) {
    return { ok: false, error: 'service is required.' };
  }
  if (!env) {
    return { ok: false, error: 'env is required.' };
  }
  if (!level) {
    return { ok: false, error: 'level must be one of error, warn, info.' };
  }
  if (!message) {
    return { ok: false, error: 'message is required.' };
  }

  return {
    ok: true,
    value: {
      projectId,
      service,
      env,
      level,
      timestamp,
      message,
      stack: body.stack != null ? String(body.stack) : '',
      context: body.context ?? null,
    },
  };
}

function createRateLimiter({ limitPerMinute, burst }) {
  const state = new Map();
  const refillPerMs = limitPerMinute / 60000;

  return function consume(projectId, nowMs) {
    const entry = state.get(projectId) || { tokens: burst, lastRefill: nowMs };
    const elapsed = Math.max(0, nowMs - entry.lastRefill);
    entry.tokens = Math.min(burst, entry.tokens + elapsed * refillPerMs);
    entry.lastRefill = nowMs;

    const allowed = entry.tokens >= 1;
    if (allowed) {
      entry.tokens -= 1;
    }
    state.set(projectId, entry);
    return allowed;
  };
}


function shouldRouteBySeverity({ threshold = 'warn', level = 'info', muteCategories = [], category = null }) {
  const weights = { info: 1, warn: 2, error: 3, critical: 4 };
  if (category && Array.isArray(muteCategories) && muteCategories.includes(category)) return false;
  const min = weights[String(threshold || 'warn').toLowerCase()] || weights.warn;
  const current = weights[String(level || 'info').toLowerCase()] || weights.info;
  return current >= min;
}

function createLogIngestService(options) {
  const {
    getProjectById,
    resolveProjectLogSettings,
    addRecentLog,
    sendTelegramMessage,
    logger = console,
    now = () => Date.now(),
    onMetrics = null,
  } = options;

  const dedupeWindowMs =
    Number(process.env.LOG_DEDUPE_WINDOW_SEC || DEFAULT_DEDUPE_WINDOW_SEC) * 1000;
  const rateLimitPerMin = Number(
    process.env.LOG_RATE_LIMIT_PER_MIN || DEFAULT_RATE_LIMIT_PER_MIN,
  );
  const rateLimitBurst = Number(process.env.LOG_RATE_LIMIT_BURST || DEFAULT_RATE_LIMIT_BURST);

  const rateLimiter = createRateLimiter({
    limitPerMinute: Number.isFinite(rateLimitPerMin) ? rateLimitPerMin : DEFAULT_RATE_LIMIT_PER_MIN,
    burst: Number.isFinite(rateLimitBurst) ? rateLimitBurst : DEFAULT_RATE_LIMIT_BURST,
  });

  const dedupeCache = new Map();
  const rateLimitedSummary = new Map();

  function computeDedupeHash(entry) {
    const base = `${entry.projectId}|${entry.level}|${entry.message}|${entry.stack || ''}`;
    return crypto.createHash('sha256').update(base).digest('hex');
  }

  function isDedupeHit(hash, nowMs) {
    const lastSeen = dedupeCache.get(hash);
    if (lastSeen && nowMs - lastSeen < dedupeWindowMs) {
      return true;
    }
    dedupeCache.set(hash, nowMs);
    return false;
  }

  function cleanupDedupe(nowMs) {
    const threshold = nowMs - dedupeWindowMs;
    for (const [hash, timestamp] of dedupeCache.entries()) {
      if (timestamp < threshold) {
        dedupeCache.delete(hash);
      }
    }
  }

  function shouldBlockForwarding(entry) {
    if (entry.projectId === 'path-applier') {
      return 'blocked_self_project';
    }
    const context = entry.context || {};
    if (context.log_ingest_failed || context.logIngestFailed) {
      return 'blocked_log_ingest_failed';
    }
    if (Array.isArray(context.tags) && context.tags.includes('log_ingest_failed')) {
      return 'blocked_log_ingest_failed';
    }
    return null;
  }

  function buildTelegramMessage(entry) {
    const lines = [
      '🧾 Log Alert',
      `Project: ${entry.projectId}`,
      `Service: ${entry.service}`,
      `Env: ${entry.env}`,
      `Level: ${entry.level.toUpperCase()}`,
      `Time: ${entry.timestamp}`,
      `Message: ${truncateText(entry.message, 1000)}`,
    ];

    if (entry.stack) {
      lines.push(`Stack: ${truncateText(entry.stack, 3000)}`);
    }

    const contextText = formatContext(entry.context, 1500);
    if (contextText) {
      lines.push(`Context: ${contextText}`);
    }

    return sanitizeTelegramText(lines.join('\n'));
  }

  async function ingestLog(payload) {
    const validation = validatePayload(payload);
    if (!validation.ok) {
      return { ok: false, error: validation.error, status: 'invalid_payload' };
    }

    const entry = validation.value;
    const project = await getProjectById(entry.projectId);
    if (!project) {
      return { ok: false, error: 'Unknown projectId.', status: 'unknown_project' };
    }

    const nowMs = now();
    cleanupDedupe(nowMs);
    const hash = computeDedupeHash(entry);
    const deduped = isDedupeHit(hash, nowMs);
    if (deduped) {
      logger.info('[log-ingest] dedupe hit', { projectId: entry.projectId, hash });
      if (onMetrics) onMetrics('dropped_by_dedupe_count');
      return { ok: true, status: 'deduped' };
    }

    const allowedByRate = rateLimiter(entry.projectId, nowMs);
    if (!allowedByRate) {
      const bucket = Math.floor(nowMs / 60000);
      const lastBucket = rateLimitedSummary.get(entry.projectId);
      if (lastBucket !== bucket) {
        rateLimitedSummary.set(entry.projectId, bucket);
        await addRecentLog(entry.projectId, {
          id: crypto.randomUUID(),
          createdAt: new Date(nowMs).toISOString(),
          level: 'warn',
          service: 'log-ingest',
          env: entry.env,
          timestamp: new Date(nowMs).toISOString(),
          message: `Rate limit reached; logs are being suppressed.`,
          stack: '',
          context: {
            projectId: entry.projectId,
            limitPerMinute: rateLimitPerMin,
            burst: rateLimitBurst,
          },
        });
      }
      logger.info('[log-ingest] rate limited', { projectId: entry.projectId });
      if (onMetrics) onMetrics('dropped_by_rate_limit_count');
      return { ok: true, status: 'rate_limited' };
    }

    await addRecentLog(entry.projectId, {
      id: crypto.randomUUID(),
      createdAt: new Date(nowMs).toISOString(),
      level: entry.level,
      service: entry.service,
      env: entry.env,
      timestamp: entry.timestamp,
      message: entry.message,
      stack: entry.stack,
      context: entry.context,
    });

    const blockReason = shouldBlockForwarding(entry);
    if (blockReason) {
      logger.info('[log-ingest] forwarding blocked', {
        projectId: entry.projectId,
        reason: blockReason,
      });
      return { ok: true, status: 'blocked' };
    }

    const settings = await resolveProjectLogSettings(project);
    if (!settings.enabled) {
      return { ok: true, status: 'disabled' };
    }

    const normalizedLevels = settings.levels
      .map((level) => normalizeLogLevel(level))
      .filter((level) => LOG_LEVELS.includes(level));
    const allowedLevels = normalizedLevels.length ? normalizedLevels : ['error'];

    if (!allowedLevels.includes(entry.level)) {
      return { ok: true, status: 'level_filtered' };
    }

    const destinationMode = settings.destinationMode || 'admin';
    const targets = new Set();
    if (destinationMode === 'admin' || destinationMode === 'both') {
      targets.add(process.env.ADMIN_TELEGRAM_ID || process.env.ADMIN_CHAT_ID);
    }
    if (destinationMode === 'channel' || destinationMode === 'both') {
      if (settings.destinationChatId) {
        targets.add(settings.destinationChatId);
      }
    }
    const resolvedTargets = Array.from(targets).filter(Boolean);
    if (!resolvedTargets.length) {
      return { ok: true, status: 'no_destination' };
    }

    try {
      const message = buildTelegramMessage(entry);
      await Promise.all(resolvedTargets.map((chatId) => sendTelegramMessage(chatId, message)));
      if (onMetrics) onMetrics('forwarded_total_count', resolvedTargets.length);
      return { ok: true, status: 'forwarded' };
    } catch (error) {
      logger.error('[log-ingest] Failed to forward to Telegram', {
        projectId: entry.projectId,
        error: error?.message,
      });
      if (onMetrics) onMetrics('delivery_fail_count');
      return { ok: true, status: 'forward_failed' };
    }
  }

  return {
    ingestLog,
    validatePayload,
  };
}

module.exports = {
  createLogIngestService,
  validatePayload,
  truncateText,
  formatContext,
  createRateLimiter,
  shouldRouteBySeverity,
};
