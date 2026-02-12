const crypto = require('crypto');
const { getConfigDbPool } = require('./configDb');

const CALLBACK_TTL_MS = 6 * 60 * 60 * 1000;
const memoryCache = new Map();
let tableReady = false;

function isSafeCallbackData(value) {
  if (typeof value !== 'string') return false;
  if (!/^[a-zA-Z0-9:_-]+$/.test(value)) return false;
  return Buffer.byteLength(value, 'utf8') <= 64;
}

function serializePayload(payload) {
  if (payload == null) return '';
  if (Array.isArray(payload)) {
    return payload.map((item) => String(item)).join(':');
  }
  if (typeof payload === 'string' || typeof payload === 'number' || typeof payload === 'boolean') {
    return String(payload);
  }
  try {
    return JSON.stringify(payload);
  } catch (error) {
    return String(payload);
  }
}

function buildRawCallback(action, payload) {
  const base = String(action || '').trim();
  const suffix = serializePayload(payload);
  if (!suffix) return base;
  return `${base}:${suffix}`;
}

function logInvalidCallback(action, raw) {
  console.warn('[callback] Invalid callback payload', {
    action,
    rawLength: Buffer.byteLength(raw || '', 'utf8'),
    raw,
  });
}

function cachePayload(id, payload) {
  const expiresAt = Date.now() + CALLBACK_TTL_MS;
  memoryCache.set(id, { payload, expiresAt });
}

async function ensureCallbackTable(db) {
  if (!db || tableReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS callback_cache (
      id TEXT PRIMARY KEY,
      payload TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      expires_at TIMESTAMPTZ NOT NULL
    );
  `);
  tableReady = true;
}

async function persistPayload(id, payload) {
  try {
    const db = await getConfigDbPool();
    if (!db) return;
    await ensureCallbackTable(db);
    const expiresAt = new Date(Date.now() + CALLBACK_TTL_MS).toISOString();
    await db.query(
      `
        INSERT INTO callback_cache (id, payload, expires_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, expires_at = EXCLUDED.expires_at
      `,
      [id, payload, expiresAt],
    );
    await db.query('DELETE FROM callback_cache WHERE expires_at < NOW()');
  } catch (error) {
    console.error('[callback] Failed to persist callback payload', error);
  }
}

function buildCb(action, payload) {
  const raw = buildRawCallback(action, payload);
  if (isSafeCallbackData(raw)) {
    return raw;
  }
  logInvalidCallback(action, raw);
  const id = crypto.randomUUID().replace(/-/g, '').slice(0, 12);
  cachePayload(id, raw);
  void persistPayload(id, raw);
  return `cb:${id}`;
}

async function resolveCallbackData(data) {
  if (!data || !data.startsWith('cb:')) {
    return { data, expired: false };
  }
  const id = data.slice(3);
  const cached = memoryCache.get(id);
  if (cached) {
    if (cached.expiresAt > Date.now()) {
      return { data: cached.payload, expired: false };
    }
    memoryCache.delete(id);
  }
  const db = await getConfigDbPool();
  if (!db) {
    return { data: null, expired: true };
  }
  try {
    await ensureCallbackTable(db);
    const { rows } = await db.query(
      'SELECT payload, expires_at FROM callback_cache WHERE id = $1 LIMIT 1',
      [id],
    );
    if (!rows.length) {
      return { data: null, expired: true };
    }
    const row = rows[0];
    const expiresAt = row.expires_at instanceof Date ? row.expires_at.getTime() : Date.parse(row.expires_at);
    if (!expiresAt || expiresAt <= Date.now()) {
      await db.query('DELETE FROM callback_cache WHERE id = $1', [id]);
      return { data: null, expired: true };
    }
    const payload = row.payload;
    cachePayload(id, payload);
    return { data: payload, expired: false };
  } catch (error) {
    console.error('[callback] Failed to resolve callback payload', error);
    return { data: null, expired: true };
  }
}

function sanitizeReplyMarkup(replyMarkup) {
  if (!replyMarkup || !replyMarkup.inline_keyboard) return replyMarkup;
  const rows = replyMarkup.inline_keyboard;
  rows.forEach((row) => {
    row.forEach((button) => {
      const text = String(button?.text || '');
      const isBackText = /(?:⬅|↩|◀)/.test(text) || /^back\b/i.test(text) || /\bback\b/i.test(text);
      const isHomeText = /\bhome\b/i.test(text);
      if (button.callback_data && isBackText && !isHomeText) {
        button.callback_data = 'nav:back';
      }
      if (button.callback_data) {
        button.callback_data = buildCb(button.callback_data);
      }
    });
  });
  return replyMarkup;
}

module.exports = {
  buildCb,
  resolveCallbackData,
  sanitizeReplyMarkup,
  isSafeCallbackData,
};
