const crypto = require('crypto');
const { loadJson, saveJson } = require('../../configStore');

const EVENTS_KEY = 'deploy_events_v1';
const SETTINGS_KEY = 'deploy_settings_v1';

function createRefId() {
  return `DEPLOY-${crypto.randomBytes(3).toString('hex').toUpperCase()}`;
}

function normalizeStatus(status) {
  const value = String(status || 'queued').toLowerCase();
  return ['queued', 'building', 'deploying', 'verifying', 'live', 'failed', 'canceled'].includes(value) ? value : 'queued';
}

function normalizeProjectSettings(input = {}) {
  return {
    provider: input.provider || 'render',
    renderServiceId: input.renderServiceId || null,
    deployHookUrl: input.deployHookUrl || null,
    baseBranch: input.baseBranch || 'main',
    healthMode: ['off', 'path', 'auto'].includes(input.healthMode) ? input.healthMode : 'auto',
    healthPath: input.healthPath || '/healthz',
    healthTimeoutSec: Math.max(30, Number(input.healthTimeoutSec) || 180),
    healthPollIntervalSec: Math.max(1, Number(input.healthPollIntervalSec) || 5),
    notifyOn: ['success', 'fail', 'both'].includes(input.notifyOn) ? input.notifyOn : 'both',
    safeModeEnabled: Boolean(input.safeModeEnabled),
    shadowRunEnabled: Boolean(input.shadowRunEnabled),
  };
}

async function listDeployEvents(projectId = null) {
  const rows = await loadJson(EVENTS_KEY);
  const events = Array.isArray(rows) ? rows : [];
  return events
    .filter((event) => (projectId ? event.projectId === projectId : true))
    .sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
}

async function saveEvents(rows) {
  await saveJson(EVENTS_KEY, rows);
}

async function createDeployEvent(input) {
  const now = new Date().toISOString();
  const row = {
    id: crypto.randomUUID(),
    projectId: input.projectId,
    provider: input.provider || 'render',
    providerServiceId: input.providerServiceId || null,
    providerDeployId: input.providerDeployId || null,
    trigger: input.trigger || 'manual',
    status: normalizeStatus(input.status || 'queued'),
    startedAt: now,
    finishedAt: null,
    durationMs: null,
    commitSha: input.commitSha || null,
    branch: input.branch || null,
    message_short: input.message_short || 'Deploy requested',
    error_short: null,
    error_full: null,
    healthUrl: input.healthUrl || null,
    healthPath: input.healthPath || null,
    healthStatusCode: null,
    healthLatencyMs: null,
    refId: createRefId(),
    meta_json: input.meta_json || null,
  };
  const rows = await listDeployEvents();
  rows.unshift(row);
  await saveEvents(rows.slice(0, 1000));
  return row;
}

async function updateDeployEvent(id, patch = {}) {
  const rows = await listDeployEvents();
  const idx = rows.findIndex((row) => row.id === id);
  if (idx < 0) return null;
  const current = rows[idx];
  const next = {
    ...current,
    ...patch,
    status: patch.status ? normalizeStatus(patch.status) : current.status,
  };
  if (next.finishedAt && next.startedAt) {
    next.durationMs = Math.max(0, new Date(next.finishedAt).getTime() - new Date(next.startedAt).getTime());
  }
  rows[idx] = next;
  await saveEvents(rows);
  return next;
}

async function getDeploySettings(projectId) {
  const all = await loadJson(SETTINGS_KEY);
  return normalizeProjectSettings((all || {})[projectId] || {});
}

async function setDeploySettings(projectId, patch) {
  const all = (await loadJson(SETTINGS_KEY)) || {};
  all[projectId] = normalizeProjectSettings({ ...(all[projectId] || {}), ...(patch || {}) });
  await saveJson(SETTINGS_KEY, all);
  return all[projectId];
}

module.exports = { createDeployEvent, updateDeployEvent, listDeployEvents, getDeploySettings, setDeploySettings, normalizeProjectSettings };
