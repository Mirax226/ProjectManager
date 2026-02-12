const crypto = require('crypto');
const configStore = require('../configStore');

const KEY = 'codexTasks';
const DONE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

function nowIso() {
  return new Date().toISOString();
}

function computeFingerprint(body) {
  return crypto.createHash('sha256').update(String(body || '')).digest('hex');
}

function ensureList(value) {
  return Array.isArray(value) ? value : [];
}

async function loadTasks() {
  return ensureList(await configStore.loadJson(KEY));
}

async function saveTasks(tasks) {
  await configStore.saveJson(KEY, ensureList(tasks));
}

function makeTaskId() {
  return crypto.randomUUID();
}

async function createCodexTask({ sourceType, projectId = null, title, body, refId = null }) {
  const tasks = await loadTasks();
  const fingerprint = computeFingerprint(body);
  const normalizedProjectId = projectId || null;
  const existing = tasks.find((task) =>
    task.status === 'pending' &&
    task.fingerprint === fingerprint &&
    (task.projectId || null) === normalizedProjectId,
  );
  const updatedAt = nowIso();
  if (existing) {
    existing.updatedAt = updatedAt;
    await saveTasks(tasks);
    return { created: false, duplicate: true, task: existing };
  }
  const task = {
    id: makeTaskId(),
    createdAt: updatedAt,
    updatedAt,
    sourceType: sourceType || 'other',
    projectId: normalizedProjectId,
    title: String(title || 'Codex task').trim().slice(0, 120),
    body: String(body || ''),
    status: 'pending',
    refId: refId || null,
    fingerprint,
    doneAt: null,
  };
  tasks.unshift(task);
  await saveTasks(tasks);
  return { created: true, duplicate: false, task };
}

async function listCodexTasks({ status } = {}) {
  const tasks = await loadTasks();
  if (!status) return tasks;
  return tasks.filter((task) => task.status === status);
}

async function getCodexTask(id) {
  const tasks = await loadTasks();
  return tasks.find((task) => task.id === id) || null;
}

async function updateCodexTask(id, updater) {
  const tasks = await loadTasks();
  const index = tasks.findIndex((task) => task.id === id);
  if (index < 0) return null;
  const current = tasks[index];
  const next = typeof updater === 'function' ? updater({ ...current }) : { ...current, ...(updater || {}) };
  next.updatedAt = nowIso();
  tasks[index] = next;
  await saveTasks(tasks);
  return next;
}

async function markCodexTaskDone(id) {
  return updateCodexTask(id, (task) => ({
    ...task,
    status: 'done',
    doneAt: nowIso(),
  }));
}

async function deleteCodexTask(id) {
  const tasks = await loadTasks();
  const filtered = tasks.filter((task) => task.id !== id);
  if (filtered.length === tasks.length) return false;
  await saveTasks(filtered);
  return true;
}

async function clearDoneCodexTasks() {
  const tasks = await loadTasks();
  const filtered = tasks.filter((task) => task.status !== 'done');
  const removed = tasks.length - filtered.length;
  if (removed > 0) {
    await saveTasks(filtered);
  }
  return removed;
}

async function purgeOldDoneCodexTasks(retentionMs = DONE_RETENTION_MS) {
  const tasks = await loadTasks();
  const cutoff = Date.now() - retentionMs;
  const filtered = tasks.filter((task) => {
    if (task.status !== 'done') return true;
    const ts = Date.parse(task.doneAt || task.updatedAt || task.createdAt || 0);
    return !Number.isFinite(ts) || ts >= cutoff;
  });
  const removed = tasks.length - filtered.length;
  if (removed > 0) {
    await saveTasks(filtered);
  }
  return removed;
}

module.exports = {
  createCodexTask,
  listCodexTasks,
  getCodexTask,
  markCodexTaskDone,
  deleteCodexTask,
  clearDoneCodexTasks,
  purgeOldDoneCodexTasks,
};
