const path = require('node:path');
const { normalizeProjectKey, redact } = require('./controlPlane/contracts');
const { validateProvider } = require('./providerRegistry');
const { validateDesktopProfile, validateExcelAsset } = require('./excelAssets');

const CONFIG_EVENTS = Object.freeze(['CONFIG_CHANGED']);
const ENVIRONMENTS = Object.freeze(['dev', 'test', 'staging', 'production']);
const SECRET_KEY = /(token|secret|password|authorization|api[-_]?key|cookie|private[-_]?key|credential|dsn)/i;

function fail(error) { return { ok: false, error }; }
function text(value, max = 200) { return String(value == null ? '' : value).trim().slice(0, max); }
function iso(value, fallback = new Date()) { const d = value == null ? fallback : new Date(value); return Number.isNaN(d.getTime()) ? null : d.toISOString(); }
function absolutePath(value) {
  const candidate = text(value, 1000);
  const windows = /^[A-Za-z]:[\\/]/.test(candidate) || /^\\\\[^\\]+[\\]+/.test(candidate);
  if (!candidate || (!path.posix.isAbsolute(candidate) && !windows)) return null;
  if (/[\0\r\n<>"|?*;&`$]/.test(candidate) || /(^|[\\/])\.\.?([\\/]|$)/.test(candidate)) return null;
  return candidate;
}

function validateAdminSettings(input = {}) {
  const ids = Array.isArray(input.adminUserIds) ? input.adminUserIds : [];
  if (!ids.length || ids.some((id) => !/^-?\d{1,30}$/.test(String(id).trim()))) return fail('adminUserIds must contain Telegram numeric IDs');
  const archive = input.archive || {};
  const channelId = text(archive.channelId || archive.chatId, 100);
  const label = text(archive.label || archive.displayName, 120);
  if (!/^-?\d{1,30}$/.test(channelId) && !/^@[A-Za-z0-9_]{5,64}$/.test(channelId)) return fail('archive channelId must be a Telegram numeric ID or @username');
  if (!label || SECRET_KEY.test(label)) return fail('archive label is invalid');
  return { ok: true, value: { adminUserIds: [...new Set(ids.map((id) => String(id).trim()))], archive: { channelId, label } } };
}

function validateProjectSettings(input = {}) {
  const projectId = normalizeProjectKey(input.projectId || input.id);
  const environment = text(input.environment || input.env, 30).toLowerCase();
  const repositoryPath = absolutePath(input.repositoryPath || input.repoPath);
  if (!projectId) return fail('projectId is invalid or missing');
  if (!ENVIRONMENTS.includes(environment)) return fail('environment is unsupported');
  if (!repositoryPath) return fail('repositoryPath must be an absolute safe path');
  return { ok: true, value: {
    projectId, enabled: input.enabled !== false, environment, repositoryPath,
    operationalEvents: input.operationalEvents !== false,
    runnerBinding: text(input.runnerBinding || input.runnerId, 120) || null,
    archiveEnabled: input.archiveEnabled !== false, backupEnabled: input.backupEnabled !== false,
  } };
}

function validateManagedExcelAsset(input = {}) {
  const base = validateExcelAsset(input);
  if (!base.ok) return base;
  if (!absolutePath(input.manualPath) || !absolutePath(input.backupPath)) return fail('Excel asset paths must be absolute and explicitly configured');
  return { ok: true, value: { ...base.value, assetId: text(input.assetId || input.filename, 120), manualPath: absolutePath(input.manualPath), backupPath: absolutePath(input.backupPath) } };
}

function validateManagedConfig(input = {}) {
  const admin = validateAdminSettings(input.admin || input.telegram || {}); if (!admin.ok) return admin;
  const projects = Array.isArray(input.projects) ? input.projects.map(validateProjectSettings) : [];
  if (projects.some((result) => !result.ok)) return projects.find((result) => !result.ok);
  const projectIds = new Set(projects.map((result) => result.value.projectId));
  const profiles = Array.isArray(input.desktopProfiles) ? input.desktopProfiles.map((item) => ({ ...validateDesktopProfile(item), item })) : [];
  if (profiles.some((result) => !result.ok)) return profiles.find((result) => !result.ok);
  const assets = Array.isArray(input.excelAssets) ? input.excelAssets.map(validateManagedExcelAsset) : [];
  if (assets.some((result) => !result.ok)) return assets.find((result) => !result.ok);
  if (assets.some((result) => !projectIds.has(result.value.project))) return fail('Excel asset project is not registered');
  const providers = Array.isArray(input.providers) ? input.providers.map((provider) => validateProvider({ ...provider, providerId: provider.providerId })) : [];
  if (providers.some((result) => !result.ok)) return providers.find((result) => !result.ok);
  if (providers.some((result) => !projectIds.has(result.value.project))) return fail('provider project is not registered');
  return { ok: true, value: { admin: admin.value, projects: projects.map((x) => x.value), desktopProfiles: profiles.map((x) => x.value), excelAssets: assets.map((x) => x.value), providers: providers.map((x) => x.value) } };
}

function sanitizeConfig(value) {
  const safe = redact(value);
  function walk(item) {
    if (!item || typeof item !== 'object') return item;
    if (Array.isArray(item)) return item.map(walk);
    const out = {};
    for (const [key, child] of Object.entries(item)) out[key] = SECRET_KEY.test(key) ? '[REDACTED]' : walk(child);
    return out;
  }
  return walk(safe);
}

class OperationalConfigStore {
  constructor(options = {}) { this.config = null; this.events = []; this.operations = new Set(); this.now = options.now || (() => new Date()); this.emit = options.emit || (() => {}); }
  read() { return this.config ? sanitizeConfig(this.config) : null; }
  update(input, actor = 'system', operationKey = null) {
    if (operationKey && this.operations.has(operationKey)) return { ok: true, duplicate: true, config: this.read() };
    const validated = validateManagedConfig(input); if (!validated.ok) return validated;
    const timestamp = iso(this.now());
    const event = { schemaVersion: 1, eventId: `config:${operationKey || timestamp}`, project: 'project-manager', environment: 'control-plane', severity: 'INFO', category: 'CONFIG_CHANGED', timestamp, message: 'Operational configuration changed', source: 'operational-config', correlationId: operationKey || null, context: sanitizeConfig({ actor: text(actor, 120), settingNames: Object.keys(input).filter((key) => !SECRET_KEY.test(key)), projectIds: validated.value.projects.map((project) => project.projectId) }) };
    this.config = validated.value; this.events.push(event); if (operationKey) this.operations.add(operationKey); this.emit(event); return { ok: true, duplicate: false, config: this.read(), event };
  }
}

module.exports = { CONFIG_EVENTS, ENVIRONMENTS, absolutePath, validateAdminSettings, validateProjectSettings, validateManagedExcelAsset, validateManagedConfig, sanitizeConfig, OperationalConfigStore };
