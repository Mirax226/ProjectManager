const { loadJson, saveJson } = require('./configStore');

const NAV_STACK_KEY = 'navigationStacks';
const DEFAULT_MAX_SIZE = 30;
const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

let cache = null;
let cacheLoaded = false;

function nowMs() {
  return Date.now();
}

function stackKey(chatId, userId) {
  return `${chatId || '0'}::${userId || '0'}`;
}

function normalizeSnapshot(snapshot) {
  return {
    routeId: String(snapshot?.routeId || 'main'),
    screenId: String(snapshot?.screenId || snapshot?.routeId || 'main'),
    params: snapshot?.params && typeof snapshot.params === 'object' ? snapshot.params : {},
    messageId: snapshot?.messageId || null,
    panelType: snapshot?.panelType || 'panel',
    timestamp: Number(snapshot?.timestamp) || nowMs(),
  };
}

async function ensureCacheLoaded() {
  if (cacheLoaded) return;
  const value = await loadJson(NAV_STACK_KEY);
  cache = value && typeof value === 'object' ? value : {};
  cacheLoaded = true;
  await cleanupExpiredStacks();
}

async function cleanupExpiredStacks(options = {}) {
  const ttlMs = Number(options.ttlMs) || DEFAULT_TTL_MS;
  const cutoff = nowMs() - ttlMs;
  let changed = false;
  Object.entries(cache || {}).forEach(([key, snapshots]) => {
    const pruned = pruneSnapshots(snapshots, options);
    if (!pruned.length || pruned[pruned.length - 1].timestamp < cutoff) {
      delete cache[key];
      changed = true;
      return;
    }
    if (pruned.length !== (Array.isArray(snapshots) ? snapshots.length : 0)) {
      cache[key] = pruned;
      changed = true;
    }
  });
  if (changed) {
    await saveJson(NAV_STACK_KEY, cache);
  }
}

function pruneSnapshots(snapshots, options = {}) {
  const ttlMs = Number(options.ttlMs) || DEFAULT_TTL_MS;
  const maxSize = Number(options.maxSize) || DEFAULT_MAX_SIZE;
  const cutoff = nowMs() - ttlMs;
  const kept = (Array.isArray(snapshots) ? snapshots : [])
    .map((entry) => normalizeSnapshot(entry))
    .filter((entry) => entry.timestamp >= cutoff)
    .slice(-maxSize);
  return kept;
}

async function pushSnapshot(chatId, userId, snapshot, options = {}) {
  await ensureCacheLoaded();
  const key = stackKey(chatId, userId);
  const existing = pruneSnapshots(cache[key], options);
  const next = [...existing, normalizeSnapshot(snapshot)];
  cache[key] = pruneSnapshots(next, options);
  await saveJson(NAV_STACK_KEY, cache);
  return [...cache[key]];
}

async function getStack(chatId, userId, options = {}) {
  await ensureCacheLoaded();
  const key = stackKey(chatId, userId);
  const pruned = pruneSnapshots(cache[key], options);
  cache[key] = pruned;
  return [...pruned];
}

async function setStack(chatId, userId, snapshots, options = {}) {
  await ensureCacheLoaded();
  const key = stackKey(chatId, userId);
  cache[key] = pruneSnapshots(snapshots, options);
  await saveJson(NAV_STACK_KEY, cache);
  return [...cache[key]];
}

async function clearStack(chatId, userId) {
  await ensureCacheLoaded();
  const key = stackKey(chatId, userId);
  delete cache[key];
  await saveJson(NAV_STACK_KEY, cache);
}

function resetForTests() {
  cache = {};
  cacheLoaded = true;
}

module.exports = {
  DEFAULT_MAX_SIZE,
  DEFAULT_TTL_MS,
  pushSnapshot,
  getStack,
  setStack,
  clearStack,
  cleanupExpiredStacks,
  resetForTests,
};
