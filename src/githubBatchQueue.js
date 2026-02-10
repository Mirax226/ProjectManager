const crypto = require('crypto');

const MAX_ITEMS = 10;
const OPEN_BATCH_TTL_MS = 7 * 24 * 60 * 60 * 1000;

function createGithubBatchQueueStore(options = {}) {
  const now = options.now || (() => Date.now());
  const batches = new Map();

  function key(projectId) {
    return String(projectId || '').trim();
  }

  function pruneExpired(projectId) {
    const existing = batches.get(projectId);
    if (!existing) return;
    const expired = existing.status === 'OPEN' && now() - Date.parse(existing.updatedAt) > OPEN_BATCH_TTL_MS;
    if (expired) batches.delete(projectId);
  }

  function getOpenBatch(projectId) {
    const projectKey = key(projectId);
    if (!projectKey) return null;
    pruneExpired(projectKey);
    const existing = batches.get(projectKey);
    if (existing && existing.status === 'OPEN') return existing;
    return null;
  }

  function openBatch(projectId) {
    const projectKey = key(projectId);
    if (!projectKey) throw new Error('project_required');
    const existing = getOpenBatch(projectKey);
    if (existing) return existing;
    const batch = {
      id: crypto.randomUUID(),
      projectId: projectKey,
      createdAt: new Date(now()).toISOString(),
      updatedAt: new Date(now()).toISOString(),
      items: [],
      status: 'OPEN',
      plan: null,
      pr: null,
      error: null,
    };
    batches.set(projectKey, batch);
    return batch;
  }

  function addItem(projectId, item) {
    const batch = openBatch(projectId);
    if (batch.items.length >= MAX_ITEMS) throw new Error('batch_limit_exceeded');
    const nextItem = {
      sourceType: item.sourceType,
      sourceId: item.sourceId,
      ruleId: item.ruleId,
      title: item.title,
      severity: item.severity || 'info',
      filesPlanned: Array.isArray(item.filesPlanned) ? item.filesPlanned.slice(0, 200) : [],
      patchPlanRef: item.patchPlanRef || null,
      refId: item.refId,
    };
    batch.items.push(nextItem);
    batch.updatedAt = new Date(now()).toISOString();
    return { batch, item: nextItem };
  }

  function update(projectId, updates = {}) {
    const batch = batches.get(key(projectId));
    if (!batch) return null;
    Object.assign(batch, updates);
    batch.updatedAt = new Date(now()).toISOString();
    return batch;
  }

  function removeItem(projectId, refId) {
    const batch = batches.get(key(projectId));
    if (!batch) return null;
    const before = batch.items.length;
    batch.items = batch.items.filter((item) => item.refId !== refId);
    if (batch.items.length !== before) batch.updatedAt = new Date(now()).toISOString();
    return batch;
  }

  function discard(projectId) {
    return batches.delete(key(projectId));
  }

  function list(projectId) {
    const batch = batches.get(key(projectId));
    if (!batch) return null;
    return JSON.parse(JSON.stringify(batch));
  }

  return {
    MAX_ITEMS,
    OPEN_BATCH_TTL_MS,
    getOpenBatch,
    openBatch,
    addItem,
    update,
    removeItem,
    discard,
    list,
  };
}

module.exports = { createGithubBatchQueueStore, MAX_ITEMS, OPEN_BATCH_TTL_MS };
