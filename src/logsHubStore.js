const crypto = require('crypto');
const { loadJson, saveJson } = require('../configStore');

const LOG_STORE_KEY = 'ops_event_log';
const AUTO_RESOLVE_WINDOW_MS = 10 * 60 * 1000;

function normalizeLevel(level) {
  const value = String(level || 'info').toLowerCase();
  return ['error', 'warn', 'info', 'debug'].includes(value) ? value : 'info';
}

function normalizeStatus(status) {
  const value = String(status || 'open').toLowerCase();
  return ['open', 'acknowledged', 'resolved'].includes(value) ? value : 'open';
}

function buildFingerprint({ category, messageShort, projectId }) {
  const raw = `${String(category || 'GENERAL').toUpperCase()}|${String(messageShort || '').trim()}|${projectId || 'global'}`;
  return crypto.createHash('sha1').update(raw).digest('hex');
}

function sanitizeMeta(meta) {
  if (meta == null) return null;
  if (typeof meta === 'object') return meta;
  return { value: String(meta) };
}

function normalizeStoredEvent(event = {}) {
  const category = String(event.category || 'GENERAL').toUpperCase();
  const messageShort = String(event.message_short || event.messageShort || '').slice(0, 500);
  const projectId = event.projectId || event.project_id || event?.meta_json?.projectId || null;
  const firstSeenAt = event.first_seen_at || event.created_at || event.ts || new Date().toISOString();
  const lastSeenAt = event.last_seen_at || event.created_at || event.ts || firstSeenAt;
  const fingerprint = event.fingerprint || buildFingerprint({ category, messageShort, projectId });
  return {
    id: event.id || crypto.randomUUID(),
    projectId,
    level: normalizeLevel(event.level),
    category,
    message_short: messageShort,
    message_full: String(event.message_full || event.messageShort || messageShort).slice(0, 4000),
    meta_json: sanitizeMeta(event.meta_json || event.meta || null),
    fingerprint,
    occurrence_count: Math.max(1, Number(event.occurrence_count) || 1),
    status: normalizeStatus(event.status),
    first_seen_at: firstSeenAt,
    last_seen_at: lastSeenAt,
  };
}

async function loadLogs() {
  const rows = await loadJson(LOG_STORE_KEY);
  if (!Array.isArray(rows)) return [];
  return rows.map(normalizeStoredEvent);
}

async function saveLogs(logs) {
  await saveJson(LOG_STORE_KEY, logs);
}

async function ingestLog(input = {}) {
  const now = new Date().toISOString();
  const candidate = normalizeStoredEvent({ ...input, first_seen_at: now, last_seen_at: now, status: 'open' });
  const logs = await loadLogs();
  const index = logs.findIndex((row) => row.status === 'open' && row.fingerprint === candidate.fingerprint);
  if (index >= 0) {
    logs[index] = {
      ...logs[index],
      level: candidate.level,
      message_full: candidate.message_full,
      meta_json: candidate.meta_json,
      occurrence_count: (logs[index].occurrence_count || 1) + 1,
      last_seen_at: now,
    };
    await saveLogs(logs);
    return { created: false, event: logs[index] };
  }
  logs.unshift(candidate);
  await saveLogs(logs);
  return { created: true, event: candidate };
}

async function listLogs(filters = {}) {
  const logs = await loadLogs();
  const out = logs
    .filter((row) => {
      if (filters.projectId != null && row.projectId !== filters.projectId) return false;
      if (filters.level && row.level !== normalizeLevel(filters.level)) return false;
      if (filters.category && row.category !== String(filters.category).toUpperCase()) return false;
      if (filters.status && row.status !== normalizeStatus(filters.status)) return false;
      return true;
    })
    .sort((a, b) => new Date(b.last_seen_at).getTime() - new Date(a.last_seen_at).getTime());
  return out;
}

async function setLogStatus({ id = null, fingerprint = null, status }) {
  const nextStatus = normalizeStatus(status);
  const logs = await loadLogs();
  const index = logs.findIndex((row) => (id ? row.id === id : row.fingerprint === fingerprint));
  if (index < 0) return null;
  logs[index] = { ...logs[index], status: nextStatus };
  await saveLogs(logs);
  return logs[index];
}

async function autoResolveLogs({ now = Date.now(), enabled = true, windowMs = AUTO_RESOLVE_WINDOW_MS } = {}) {
  if (!enabled) return { changed: 0, rows: [] };
  const logs = await loadLogs();
  const changed = [];
  const next = logs.map((row) => {
    if (row.status !== 'open' && row.status !== 'acknowledged') return row;
    const lastSeen = new Date(row.last_seen_at).getTime();
    if (Number.isNaN(lastSeen) || now - lastSeen < windowMs) return row;
    const resolved = { ...row, status: 'resolved' };
    changed.push(resolved);
    return resolved;
  });
  if (!changed.length) return { changed: 0, rows: [] };
  await saveLogs(next);
  return { changed: changed.length, rows: changed };
}

module.exports = {
  buildFingerprint,
  ingestLog,
  listLogs,
  setLogStatus,
  autoResolveLogs,
  normalizeStoredEvent,
};
