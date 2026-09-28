const crypto = require('node:crypto');
const { validateManagedConfig } = require('../operationalConfig');
const { validateDesktopProfile, validateExcelAsset } = require('../excelAssets');
const { validateProvider } = require('../providerRegistry');
const { validateArchiveRecord } = require('../archive');
const { validateBackupRecord } = require('../backupRecords');
const { normalizeProjectKey, redact } = require('./contracts');

function id(prefix) { return `${prefix}_${crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}_${Math.random().toString(36).slice(2)}`}`; }
function nowIso(store) { return new Date(store.now()).toISOString(); }
function copy(value) { return value == null ? value : JSON.parse(JSON.stringify(value)); }
function safeMetadata(value) { const source = value && typeof value === 'object' ? value : {}; const keys = ['code', 'message', 'component', 'source', 'reference', 'retryable', 'sourceHash', 'destinationHash']; return Object.fromEntries(keys.filter((key) => Object.prototype.hasOwnProperty.call(source, key)).map((key) => [key, typeof source[key] === 'string' ? String(source[key]).slice(0, 500) : source[key]])); }

function initOperationalState() {
  this.operationalConfig = null;
  this.projectConfigs = new Map();
  this.configAudit = [];
  this.desktopProfiles = new Map();
  this.excelAssets = new Map();
  this.providerOperations = new Map();
  this.archives = new Map();
  this.archiveOperations = new Map();
  this.backups = new Map();
  this.backupOperations = new Map();
  this.retentionCandidates = new Map();
  this.reconciliationResults = new Map();
  this._createBackupLocal = (...args) => createBackup.apply(this, args);
  this._saveReconciliationResultLocal = (...args) => saveReconciliationResult.apply(this, args);
  this._ingestEventLocal = (event) => this.constructor.prototype.ingestEvent.call(this, event);
}

function saveOperationalConfig(input, actor = 'system', operationKey = null) {
  const prior = operationKey && this.configAudit.find((item) => item.operationKey === operationKey);
  if (prior) return { ok: true, duplicate: true, config: copy(this.operationalConfig), audit: copy(prior) };
  const checked = validateManagedConfig(input); if (!checked.ok) return checked;
  const changedAt = nowIso(this);
  const audit = { auditId: id('cfg'), operationKey: operationKey || null, projectId: null, actor: String(actor).slice(0, 120), changedAt, settingNames: Object.keys(input).filter((key) => !/(token|secret|password|api[-_]?key|cookie|private[-_]?key|dsn)/i.test(key)), context: redact({ projectIds: checked.value.projects.map((project) => project.projectId) }) };
  this.operationalConfig = copy(checked.value); this.projectConfigs = new Map(checked.value.projects.map((project) => [project.projectId, copy(project)])); this.configAudit.push(audit);
  const eventProject = audit.context.projectIds[0] || this.projects.keys().next().value;
  const event = eventProject ? { schemaVersion: 1, eventId: `config:${audit.auditId}`, project: eventProject, environment: 'control-plane', severity: 'INFO', category: 'CONFIG_CHANGED', timestamp: changedAt, message: 'Operational configuration changed', source: 'control-plane', correlationId: operationKey, context: redact({ actor: audit.actor, settingNames: audit.settingNames, projectIds: audit.context.projectIds }) } : null;
  if (event) this._ingestEventLocal(event);
  return { ok: true, duplicate: false, config: copy(this.operationalConfig), audit: copy(audit), event };
}

function upsertDesktopProfile(input, projectId = null) { const checked = validateDesktopProfile(input); if (!checked.ok) return checked; const profile = { ...checked.value, projectId: normalizeProjectKey(projectId) || null, updatedAt: nowIso(this) }; this.desktopProfiles.set(profile.id, copy(profile)); return { ok: true, profile: copy(profile) }; }
function upsertExcelAsset(input) { const checked = validateExcelAsset(input); if (!checked.ok) return checked; const assetId = String(input.assetId || input.filename).slice(0, 120); const asset = { ...checked.value, assetId, updatedAt: nowIso(this) }; this.excelAssets.set(`${asset.project}:${assetId}`, copy(asset)); return { ok: true, asset: copy(asset) }; }
function upsertProviderOperation(input) { const checked = validateProvider(input); if (!checked.ok) return checked; const provider = { ...checked.value, providerId: String(input.providerId || input.id || checked.value.providerId).slice(0, 120), lastHealthCheck: input.lastHealthCheck || checked.value.updatedAt || null, updatedAt: nowIso(this) }; this.providerOperations.set(`${provider.project}:${provider.providerId}`, copy(provider)); return { ok: true, provider: copy(provider) }; }
function createArchive(input, operationKey = null) { if (operationKey && this.archiveOperations.has(operationKey)) return { ok: true, duplicate: true, archive: copy(this.archives.get(this.archiveOperations.get(operationKey))) }; const checked = validateArchiveRecord(input); if (!checked.ok) return checked; const archive = { ...checked.value, archiveId: checked.value.archiveId || id('arc'), operationKey: operationKey || null, updatedAt: nowIso(this) }; this.archives.set(archive.archiveId, copy(archive)); if (operationKey) this.archiveOperations.set(operationKey, archive.archiveId); return { ok: true, duplicate: false, archive: copy(archive) }; }
function updateArchiveRestore(archiveId, projectId, restoreStatus, restoredAt = null) { const archive = this.archives.get(String(archiveId)); if (!archive || archive.projectId !== normalizeProjectKey(projectId)) return { ok: false, error: 'archive record not found in project scope' }; if (!['NOT_REQUESTED', 'RESTORE_REQUESTED', 'RESTORED'].includes(restoreStatus)) return { ok: false, error: 'restore status is invalid' }; archive.restoreStatus = restoreStatus; archive.restoredAt = restoreStatus === 'RESTORED' ? (restoredAt || nowIso(this)) : null; archive.updatedAt = nowIso(this); return { ok: true, archive: copy(archive) }; }
function updateArchiveStatus(archiveId, projectId, status, providerReference = null) { const archive = this.archives.get(String(archiveId)); if (!archive || archive.projectId !== normalizeProjectKey(projectId)) return { ok: false, error: 'archive record not found in project scope' }; if (!['PENDING', 'STORED', 'FAILED'].includes(String(status).toUpperCase())) return { ok: false, error: 'archive status is invalid' }; archive.status = String(status).toUpperCase(); if (providerReference) archive.providerReference = String(providerReference).slice(0, 200); archive.updatedAt = nowIso(this); return { ok: true, archive: copy(archive) }; }
function createBackup(input, operationKey = null) { if (operationKey && this.backupOperations.has(operationKey)) return { ok: true, duplicate: true, backup: copy(this.backups.get(this.backupOperations.get(operationKey))) }; const checked = validateBackupRecord(input); if (!checked.ok) return checked; const backup = { ...checked.value, operationKey: operationKey || null, updatedAt: nowIso(this) }; this.backups.set(backup.backupId, copy(backup)); if (operationKey) this.backupOperations.set(operationKey, backup.backupId); return { ok: true, duplicate: false, backup: copy(backup) }; }
function saveRetentionCandidate(input) { const projectId = normalizeProjectKey(input.projectId || input.project); if (!projectId || !input.candidateId) return { ok: false, error: 'retention candidate project and candidateId are required' }; const candidate = { ...redact(input), projectId, destructiveAction: false, updatedAt: nowIso(this) }; this.retentionCandidates.set(String(candidate.candidateId), copy(candidate)); return { ok: true, candidate: copy(candidate) }; }
function saveReconciliationResult(input) { const projectId = normalizeProjectKey(input.projectId || input.project); const workId = String(input.workId || '').trim(); if (!projectId || !workId) return { ok: false, error: 'reconciliation project and workId are required' }; const prior = this.reconciliationResults.get(workId); if (prior && prior.projectId !== projectId) return { ok: false, error: 'reconciliation result is outside the project scope', code: 'PROJECT_SCOPE_DENIED' }; const status = String(input.status || '').toUpperCase(); if (!['MATCH', 'MISMATCH', 'ERROR', 'EXPECTED_INTENTIONAL_DIVERGENCE', 'KNOWN_LEGACY_DEFECT'].includes(status)) return { ok: false, error: 'reconciliation status is unsupported' }; const result = { workId, projectId, status, destination: String(input.destination || '').slice(0, 200), mismatchCount: Math.max(0, Number(input.mismatchCount) || 0), errorCount: Math.max(0, Number(input.errorCount) || 0), retryable: input.retryable === true, hashes: safeMetadata(input.hashes), diagnostics: safeMetadata(input.diagnostics), correlationId: String(input.correlationId || '').slice(0, 120) || null, updatedAt: nowIso(this) }; this.reconciliationResults.set(workId, copy(result)); return { ok: true, result: copy(result) }; }

function recordDailySystemEvidence(input = {}) {
  const projectId = normalizeProjectKey(input.projectId || input.project);
  if (!projectId || !this.projects.has(projectId)) return { ok: false, error: 'DailySystem evidence project is invalid' };
  const operation = String(input.operation || '').trim().toLowerCase();
  const value = input.value && typeof input.value === 'object' ? input.value : null;
  const error = input.error && typeof input.error === 'object' ? input.error : null;
  const correlationId = String(input.correlationId || value?.correlationId || error?.correlationId || '').slice(0, 120) || null;
  let category = 'HEALTHCHECK_FAILED'; let severity = 'ERROR'; let message = 'DailySystem operational check failed';
  if (operation === 'health' || operation === 'projectstatus') {
    if (input.ok && value && !error) { category = 'HEALTHCHECK_RECOVERED'; severity = 'INFO'; message = 'DailySystem operational health recovered'; }
  } else if (operation === 'excelsyncdiagnostic') {
    const code = String(error?.code || value?.diagnosticCode || '').toUpperCase();
    const map = { EXCEL_FILE_NOT_FOUND: 'EXCEL_FILE_NOT_FOUND', EXCEL_FINGERPRINT_MISMATCH: 'EXCEL_FINGERPRINT_MISMATCH', EXCEL_DESKTOP_UNAVAILABLE: 'EXCEL_DESKTOP_UNAVAILABLE', EXCEL_OPEN_FAILED: 'EXCEL_OPEN_FAILED' };
    category = map[code] || (input.ok ? 'EXCEL_HEALTH_RECOVERED' : 'EXCEL_AGENT_ERROR');
    severity = input.ok ? 'INFO' : 'ERROR'; message = input.ok ? 'DailySystem Excel diagnostic completed' : 'DailySystem Excel diagnostic failed';
  } else if (operation === 'excelreconciliation') {
    const status = String(value?.status || '').toUpperCase();
    category = ['MATCH', 'EXPECTED_INTENTIONAL_DIVERGENCE', 'KNOWN_LEGACY_DEFECT'].includes(status) ? 'EXCEL_HEALTH_RECOVERED' : 'EXCEL_RECONCILIATION_ERROR';
    severity = category === 'EXCEL_HEALTH_RECOVERED' ? 'INFO' : 'ERROR'; message = category === 'EXCEL_HEALTH_RECOVERED' ? `DailySystem reconciliation ${status || 'recovered'}` : `DailySystem reconciliation ${status || 'failed'}`;
    if (value) { const saved = this._saveReconciliationResultLocal({ ...value, projectId, correlationId }); if (!saved.ok) return saved; }
  }
  const identity = input.evidenceId || [operation, value?.workId || value?.assetId || error?.code || 'result', value?.status || error?.code || (input.ok ? 'ok' : 'error'), value?.mismatchCount || 0, value?.errorCount || 0].join(':');
  const evidenceKey = String(identity).slice(0, 180);
  const event = { schemaVersion: 1, eventId: `dailysystem:${projectId}:${evidenceKey}`, project: projectId, environment: this.projects.get(projectId).environment || 'dev', severity, category, timestamp: nowIso(this), message, source: 'dailysystem-adapter', correlationId, context: redact({ operation, code: error?.code || null, retryable: error?.retryable === true, status: value?.status || null, workId: value?.workId || null, mismatchCount: value?.mismatchCount || 0, errorCount: value?.errorCount || 0, diagnostics: value?.diagnostics || error?.message || null }) };
  const ingested = this._ingestEventLocal(event);
  return { ok: ingested.ok, duplicate: ingested.duplicate === true, event: ingested.event || event, reconciliation: value && operation === 'excelreconciliation' ? this.reconciliationResults.get(value.workId) || null : null };
}

const operationalMethods = { initOperationalState, saveOperationalConfig, upsertDesktopProfile, upsertExcelAsset, upsertProviderOperation, createArchive, updateArchiveRestore, updateArchiveStatus, createBackup, saveRetentionCandidate, saveReconciliationResult, recordDailySystemEvidence };
module.exports = { operationalMethods };
