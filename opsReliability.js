const crypto = require('crypto');
const { loadJson, saveJson } = require('./configStore');

const EVENT_LIMIT = 500;
const NON_REPORTABLE = new Set(['INVALID_URL', 'ENV_MISCONFIG', 'MISSING_DSN']);
const LEVEL_WEIGHT = { info: 1, warn: 2, error: 3, critical: 4 };

const memory = {
  eventLog: [],
  dbHealth: {
    status: 'MISCONFIG',
    lastChangeAt: null,
    lastErrorCategory: null,
    lastErrorMessageMasked: null,
    lastOutageId: 0,
    lastRecoveryNotifiedOutageId: 0,
  },
  userAlertPrefs: {},
};

const rateBuckets = new Map();

function maskSecrets(value) {
  if (value == null) return value;
  const text = typeof value === 'string' ? value : JSON.stringify(value);
  return text
    .replace(/(postgres(?:ql)?:\/\/[^:\s@]+:)([^@\s]+)(@)/gi, (_m, p1, p2, p3) => `${p1}***${String(p2).slice(-4)}${p3}`)
    .replace(/(token|password|secret|api[_-]?key)\s*[:=]\s*([^,\s]+)/gi, '$1=***');
}

function defaultUserAlertPrefs(userId) {
  return {
    user_id: String(userId),
    enable_alerts: true,
    destinations: { admin_inbox: true, admin_room: false, admin_rob: false },
    severity_threshold: 'warn',
    mute_categories: ['INVALID_URL', 'ENV_MISCONFIG', 'MISSING_DSN'],
    rate_limits: { max_per_10m: 20, debounce_seconds_per_category: 90 },
  };
}

async function loadOpsState() {
  const [eventLog, dbHealth, userAlertPrefs] = await Promise.all([
    loadJson('ops_event_log'),
    loadJson('ops_db_health_snapshot'),
    loadJson('ops_user_alert_prefs'),
  ]);
  if (Array.isArray(eventLog)) memory.eventLog = eventLog.slice(0, EVENT_LIMIT);
  if (dbHealth && typeof dbHealth === 'object') memory.dbHealth = { ...memory.dbHealth, ...dbHealth };
  if (userAlertPrefs && typeof userAlertPrefs === 'object') memory.userAlertPrefs = userAlertPrefs;
}

async function persistOpsState() {
  await Promise.all([
    saveJson('ops_event_log', memory.eventLog),
    saveJson('ops_db_health_snapshot', memory.dbHealth),
    saveJson('ops_user_alert_prefs', memory.userAlertPrefs),
  ]);
}

async function appendEvent({ level = 'info', source = 'pm', category = 'GENERAL', messageShort = '', meta = null }) {
  let safeMeta = null;
  if (meta != null) {
    try {
      safeMeta = JSON.parse(maskSecrets(meta));
    } catch (_error) {
      safeMeta = { masked: maskSecrets(meta) };
    }
  }
  const event = {
    id: crypto.randomUUID(),
    ts: new Date().toISOString(),
    level,
    source,
    category,
    message_short: String(messageShort || '').slice(0, 800),
    meta_json: safeMeta,
  };
  memory.eventLog.unshift(event);
  if (memory.eventLog.length > EVENT_LIMIT) memory.eventLog.length = EVENT_LIMIT;
  await saveJson('ops_event_log', memory.eventLog);
  return event;
}

function getDbHealthSnapshot() {
  return { ...memory.dbHealth };
}

async function setDbHealthSnapshot(next) {
  const prev = memory.dbHealth.status;
  const now = new Date().toISOString();
  memory.dbHealth = {
    ...memory.dbHealth,
    ...next,
    lastChangeAt: prev !== next.status ? now : memory.dbHealth.lastChangeAt || now,
  };
  if (['DOWN', 'DEGRADED', 'MISCONFIG'].includes(memory.dbHealth.status) && prev === 'HEALTHY') {
    memory.dbHealth.lastOutageId += 1;
  }
  await saveJson('ops_db_health_snapshot', memory.dbHealth);
}

async function getUserAlertPrefs(userId) {
  const key = String(userId);
  if (!memory.userAlertPrefs[key]) {
    memory.userAlertPrefs[key] = defaultUserAlertPrefs(userId);
    await saveJson('ops_user_alert_prefs', memory.userAlertPrefs);
  }
  return memory.userAlertPrefs[key];
}

function shouldRouteEvent(prefs, event) {
  if (!prefs?.enable_alerts) return false;
  if (NON_REPORTABLE.has(event.category)) return false;
  if (prefs.mute_categories?.includes(event.category)) return false;
  const min = LEVEL_WEIGHT[prefs.severity_threshold] || LEVEL_WEIGHT.warn;
  const current = LEVEL_WEIGHT[event.level] || LEVEL_WEIGHT.info;
  if (current < min) return false;

  const now = Date.now();
  const bucketKey = `${prefs.user_id}:${event.category}`;
  const bucket = rateBuckets.get(bucketKey) || { sent: [], lastSentAt: 0 };
  const maxPer10m = prefs.rate_limits?.max_per_10m ?? 20;
  const debounceMs = (prefs.rate_limits?.debounce_seconds_per_category ?? 90) * 1000;
  bucket.sent = bucket.sent.filter((ts) => now - ts < 10 * 60 * 1000);
  if (bucket.sent.length >= maxPer10m) return false;
  if (bucket.lastSentAt && now - bucket.lastSentAt < debounceMs) return false;
  bucket.sent.push(now);
  bucket.lastSentAt = now;
  rateBuckets.set(bucketKey, bucket);
  return true;
}

function computeDestinations(prefs, eventLevel) {
  const level = eventLevel || 'error';
  const critical = level === 'critical';
  const error = level === 'error';
  return {
    admin_inbox: prefs.destinations?.admin_inbox !== false,
    admin_room: prefs.destinations?.admin_room === true && (critical || error),
    admin_rob: prefs.destinations?.admin_rob === true && critical,
  };
}

module.exports = {
  NON_REPORTABLE,
  loadOpsState,
  persistOpsState,
  appendEvent,
  maskSecrets,
  getDbHealthSnapshot,
  setDbHealthSnapshot,
  getUserAlertPrefs,
  shouldRouteEvent,
  computeDestinations,
};
