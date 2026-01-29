const { sanitizeTelegramText } = require('../../telegramApi');

const DEFAULT_RATE_LIMIT_PER_MINUTE = 20;
const DEFAULT_PAYLOAD_LIMIT_BYTES = 10 * 1024;

const LEVEL_ICONS = {
  info: 'ℹ️',
  warn: '⚠️',
  error: '❌',
};

function escapeHtml(value) {
  if (value == null) return '';
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function normalizeLevel(level) {
  if (!level) return null;
  const normalized = String(level).trim().toLowerCase();
  return LEVEL_ICONS[normalized] ? normalized : null;
}

function parseAllowedProjects(raw) {
  if (!raw) return new Set();
  return new Set(
    String(raw)
      .split(',')
      .map((entry) => entry.trim().toLowerCase())
      .filter(Boolean),
  );
}

function formatTimestamp(value, nowProvider) {
  const date = value ? new Date(value) : new Date(nowProvider());
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const iso = date.toISOString();
  return `${iso.slice(0, 10)} ${iso.slice(11, 19)}`;
}

function formatMetaLines(meta) {
  if (!meta || typeof meta !== 'object' || Array.isArray(meta)) {
    return [];
  }
  return Object.entries(meta).map(([key, value]) => {
    let rendered = '';
    if (typeof value === 'string') {
      rendered = value;
    } else {
      try {
        rendered = JSON.stringify(value);
      } catch (error) {
        rendered = String(value);
      }
    }
    return `- ${key}: ${rendered}`;
  });
}

function validatePayload(payload, nowProvider) {
  if (!payload || typeof payload !== 'object') {
    return { ok: false, error: 'Invalid payload' };
  }
  const project = payload.project != null ? String(payload.project).trim() : '';
  const level = normalizeLevel(payload.level);
  const message = payload.message != null ? String(payload.message).trim() : '';
  const meta = payload.meta ?? {};
  const timestamp = payload.timestamp ?? null;

  if (!project) {
    return { ok: false, error: 'project is required' };
  }
  if (!level) {
    return { ok: false, error: 'level must be info, warn, or error' };
  }
  if (!message) {
    return { ok: false, error: 'message is required' };
  }
  if (meta != null && (typeof meta !== 'object' || Array.isArray(meta))) {
    return { ok: false, error: 'meta must be an object' };
  }
  if (timestamp != null) {
    const parsed = new Date(timestamp);
    if (Number.isNaN(parsed.getTime())) {
      return { ok: false, error: 'timestamp must be ISO string' };
    }
  }

  return {
    ok: true,
    value: {
      project,
      level,
      message,
      meta: meta && typeof meta === 'object' ? meta : {},
      timestamp: timestamp || new Date(nowProvider()).toISOString(),
    },
  };
}

function createRateLimiter({ maxPerMinute }) {
  const entries = new Map();
  const windowMs = 60_000;

  return function isAllowed(projectId, nowMs) {
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

function getBearerToken(req) {
  const header = req.headers.authorization;
  if (!header || typeof header !== 'string') return null;
  const [type, token] = header.split(' ');
  if (type !== 'Bearer' || !token) return null;
  return token;
}

async function readRequestBodyWithLimit(req, limitBytes) {
  return new Promise((resolve, reject) => {
    let data = '';
    let resolved = false;

    function finish(result) {
      resolved = true;
      resolve(result);
    }

    req.on('data', (chunk) => {
      if (resolved) return;
      data += chunk;
      if (data.length > limitBytes) {
        finish({ ok: false, error: 'payload_too_large' });
        req.destroy();
      }
    });
    req.on('end', () => finish({ ok: true, data }));
    req.on('error', (error) => {
      if (resolved) return;
      reject(error);
    });
  });
}

function buildTelegramMessage(entry, nowProvider) {
  const icon = LEVEL_ICONS[entry.level];
  const metaLines = formatMetaLines(entry.meta);
  const timestamp = formatTimestamp(entry.timestamp, nowProvider);

  const lines = [
    `📦 Project: ${entry.project}`,
    `🧭 Level: ${icon} ${entry.level.toUpperCase()}`,
    '',
    '📝 Message:',
    entry.message,
  ];

  if (metaLines.length) {
    lines.push('', '📎 Meta:', ...metaLines);
  }

  lines.push('', `🕒 Time: ${timestamp}`);

  const sanitized = sanitizeTelegramText(lines.join('\n'));
  return escapeHtml(sanitized);
}

function createLogsRouter(options) {
  const {
    token,
    adminChatId,
    allowedProjects,
    logger = console,
    sendTelegramMessage,
    now = () => Date.now(),
    rateLimitPerMinute = DEFAULT_RATE_LIMIT_PER_MINUTE,
    payloadLimitBytes = DEFAULT_PAYLOAD_LIMIT_BYTES,
  } = options;

  const allowedSet = allowedProjects ?? parseAllowedProjects('');
  const isAllowed = createRateLimiter({ maxPerMinute: rateLimitPerMinute });

  async function handle(req, res) {
    if (!token || !adminChatId) {
      logger.error('[LOG_API] rejected: missing configuration');
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Server not configured' }));
      return;
    }

    const providedToken = getBearerToken(req);
    if (!providedToken || providedToken !== token) {
      logger.error('[LOG_API] rejected: unauthorized');
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    let bodyResult;
    try {
      bodyResult = await readRequestBodyWithLimit(req, payloadLimitBytes);
    } catch (error) {
      logger.error('[LOG_API] rejected: failed to read body', error);
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid payload' }));
      return;
    }

    if (!bodyResult.ok && bodyResult.error === 'payload_too_large') {
      logger.error('[LOG_API] rejected: payload too large');
      res.writeHead(413, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Payload too large' }));
      return;
    }

    let payload;
    try {
      payload = bodyResult.data ? JSON.parse(bodyResult.data) : null;
    } catch (error) {
      logger.error('[LOG_API] rejected: invalid json');
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid JSON body' }));
      return;
    }

    const validation = validatePayload(payload, now);
    if (!validation.ok) {
      logger.error('[LOG_API] rejected: invalid payload', { error: validation.error });
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: validation.error }));
      return;
    }

    const entry = validation.value;
    const projectKey = entry.project.toLowerCase();
    if (!allowedSet.has(projectKey)) {
      logger.error('[LOG_API] rejected: project not allowed', { project: entry.project });
      res.writeHead(403, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Project not allowed' }));
      return;
    }

    const nowMs = now();
    if (!isAllowed(projectKey, nowMs)) {
      logger.error('[LOG_API] rejected: rate limit', { project: entry.project });
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Rate limit exceeded' }));
      return;
    }

    logger.log(
      `[LOG_API] project=${entry.project} level=${entry.level} message=${JSON.stringify(
        entry.message,
      )}`,
    );

    try {
      const message = buildTelegramMessage(entry, now);
      await sendTelegramMessage(adminChatId, message, {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      });
    } catch (error) {
      logger.error('[LOG_API] failed to send log to telegram', {
        project: entry.project,
        error: error?.message,
      });
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
  }

  return {
    handle,
  };
}

module.exports = {
  createLogsRouter,
  parseAllowedProjects,
};
