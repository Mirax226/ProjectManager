const { sanitizeTelegramText } = require('../telegramApi');

function normalizeLevel(level) {
  if (!level) return null;
  const normalized = String(level).trim().toLowerCase();
  if (['error', 'warn', 'info'].includes(normalized)) {
    return normalized;
  }
  return null;
}

function titleCaseProject(project) {
  const text = String(project || '').trim();
  if (!text) return '';
  const words = text.replace(/[-_]+/g, ' ').split(/\s+/);
  return words
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function formatTimestamp(timestamp) {
  const date = timestamp ? new Date(timestamp) : new Date();
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const iso = date.toISOString();
  return `${iso.slice(0, 10)} ${iso.slice(11, 19)}`;
}

function formatMeta(meta) {
  if (!meta || typeof meta !== 'object') {
    return '{}';
  }
  try {
    return JSON.stringify(meta, null, 2);
  } catch (error) {
    return '{}';
  }
}

function validateLogPayload(payload) {
  if (!payload || typeof payload !== 'object') {
    return { ok: false, error: 'Invalid payload' };
  }

  const project = payload.project != null ? String(payload.project).trim() : '';
  const level = normalizeLevel(payload.level);
  const message = payload.message != null ? String(payload.message).trim() : '';
  const timestamp = payload.timestamp;
  const meta = payload.meta && typeof payload.meta === 'object' ? payload.meta : {};

  if (!project) {
    return { ok: false, error: 'Missing project' };
  }
  if (!level) {
    return { ok: false, error: 'Missing level' };
  }
  if (!message) {
    return { ok: false, error: 'Missing message' };
  }

  return {
    ok: true,
    value: {
      project,
      level,
      message,
      meta,
      timestamp: timestamp || new Date().toISOString(),
    },
  };
}

function buildLogMessage(entry) {
  const projectName = titleCaseProject(entry.project);
  const timestamp = formatTimestamp(entry.timestamp);
  const header = {
    error: '🚨 ERROR',
    warn: '⚠️ WARN',
    info: 'ℹ️ INFO',
  }[entry.level];

  const lines = [
    header,
    `📦 Project: ${projectName || entry.project}`,
    `🕒 ${timestamp}`,
    '',
    'Message:',
    entry.message,
  ];

  if (entry.level === 'error') {
    lines.push('', 'Meta:', formatMeta(entry.meta));
  }

  return sanitizeTelegramText(lines.join('\n'));
}

function withTimeout(promise, timeoutMs) {
  if (!timeoutMs) return promise;
  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error('Telegram send timed out')), timeoutMs);
  });

  return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timeoutId));
}

function createLogForwarder(options) {
  const {
    adminChatId,
    sendTelegramMessage,
    logger = console,
    now = () => Date.now(),
    timeoutMs = 5000,
    rateLimit = { maxPerWindow: 10, windowMs: 5000 },
  } = options;

  const projectWindows = new Map();

  function trackProjectWindow(projectId, nowMs) {
    const entry = projectWindows.get(projectId) || {
      timestamps: [],
      lastSummaryAt: 0,
    };
    const cutoff = nowMs - rateLimit.windowMs;
    entry.timestamps = entry.timestamps.filter((value) => value >= cutoff);
    entry.timestamps.push(nowMs);
    projectWindows.set(projectId, entry);
    return entry;
  }

  function shouldBatch(entry, nowMs) {
    if (entry.timestamps.length <= rateLimit.maxPerWindow) {
      return false;
    }
    const recentSummary = entry.lastSummaryAt && nowMs - entry.lastSummaryAt < rateLimit.windowMs;
    if (recentSummary) {
      return true;
    }
    entry.lastSummaryAt = nowMs;
    return true;
  }

  async function forwardLog(payload) {
    const validation = validateLogPayload(payload);
    if (!validation.ok) {
      return { ok: false, error: validation.error };
    }

    const entry = validation.value;
    const nowMs = now();
    const windowState = trackProjectWindow(entry.project, nowMs);
    if (shouldBatch(windowState, nowMs)) {
      const summary = `⚠️ ${rateLimit.maxPerWindow} logs received from ${
        titleCaseProject(entry.project) || entry.project
      } in 5s`;
      try {
        await withTimeout(sendTelegramMessage(adminChatId, summary), timeoutMs);
      } catch (error) {
        logger.error('[LOG_API] failed to send batch summary', {
          project: entry.project,
          error: error?.message,
        });
      }
      return { ok: true, status: 'batched' };
    }

    const message = buildLogMessage(entry);
    try {
      await withTimeout(sendTelegramMessage(adminChatId, message), timeoutMs);
      return { ok: true, status: 'forwarded' };
    } catch (error) {
      logger.error('[LOG_API] failed to send log', {
        project: entry.project,
        error: error?.message,
      });
      return { ok: true, status: 'forward_failed' };
    }
  }

  return {
    forwardLog,
    buildLogMessage,
  };
}

module.exports = {
  createLogForwarder,
  validateLogPayload,
};
