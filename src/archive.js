const crypto = require('node:crypto');
const { normalizeProjectKey, redact } = require('./controlPlane/contracts');

const ARCHIVE_STATUSES = Object.freeze(['PENDING', 'STORED', 'FAILED']);
const RESTORE_STATUSES = Object.freeze(['NOT_REQUESTED', 'RESTORE_REQUESTED', 'RESTORED']);
const ARTIFACT_TYPES = Object.freeze(['EXCEL_BACKUP', 'EXCEL_DIAGNOSTIC_SNAPSHOT', 'HISTORICAL_PROJECT_ARTIFACT', 'COLD_DATA_EXPORT']);

function text(value, max = 500) { return String(value == null ? '' : value).trim().slice(0, max); }
function iso(value) { const d = new Date(value == null ? Date.now() : value); return Number.isNaN(d.getTime()) ? null : d.toISOString(); }
function hash(value) { return /^[a-f0-9]{64}$/i.test(String(value || '')) ? String(value).toLowerCase() : crypto.createHash('sha256').update(Buffer.isBuffer(value) ? value : String(value || '')).digest('hex'); }
function validateArchiveRecord(input = {}) {
  const projectId = normalizeProjectKey(input.projectId || input.project);
  const artifactType = text(input.artifactType, 60).toUpperCase();
  const createdAt = iso(input.createdAt);
  if (!projectId) return { ok: false, error: 'archive projectId is invalid or missing' };
  if (!ARTIFACT_TYPES.includes(artifactType)) return { ok: false, error: 'archive artifactType is unsupported' };
  if (!createdAt) return { ok: false, error: 'archive createdAt is invalid' };
  if (!text(input.contentHash, 128)) return { ok: false, error: 'archive contentHash is required' };
  if (input.size != null && (!Number.isSafeInteger(Number(input.size)) || Number(input.size) < 0)) return { ok: false, error: 'archive size is invalid' };
  if (input.status != null && !ARCHIVE_STATUSES.includes(String(input.status).toUpperCase())) return { ok: false, error: 'archive status is unsupported' };
  if (input.restoreStatus != null && !RESTORE_STATUSES.includes(String(input.restoreStatus).toUpperCase())) return { ok: false, error: 'archive restoreStatus is unsupported' };
  return { ok: true, value: { archiveId: text(input.archiveId, 160), projectId, artifactType, createdAt, sourceTimestamp: iso(input.sourceTimestamp), contentHash: hash(input.contentHash), size: input.size == null ? null : Number(input.size), providerReference: text(input.providerReference, 200) || null, telegramChannelId: text(input.telegramChannelId || input.channelId, 100) || null, telegramMessageId: text(input.telegramMessageId || input.messageId, 100) || null, telegramFileId: text(input.telegramFileId || input.fileId, 200) || null, sourceReference: text(input.sourceReference, 500) || null, retentionReason: text(input.retentionReason, 300) || null, status: String(input.status || 'PENDING').toUpperCase(), restoreStatus: String(input.restoreStatus || 'NOT_REQUESTED').toUpperCase(), restoredAt: iso(input.restoredAt), correlationId: text(input.correlationId, 160) || null, safeMetadata: redact(input.safeMetadata && typeof input.safeMetadata === 'object' ? input.safeMetadata : {}) } };
}

class InMemoryArchiveProvider {
  constructor() { this.records = new Map(); }
  async put(record, operationKey = null) { const key = operationKey || record.archiveId; const prior = this.records.get(key); if (prior) return { ok: true, duplicate: true, record: { ...prior } }; const stored = { ...record, status: 'STORED' }; this.records.set(key, stored); return { ok: true, duplicate: false, record: { ...stored } }; }
  async get(archiveId) { return this.records.get(String(archiveId)) || null; }
  async list(projectId = null) { return [...this.records.values()].filter((record) => !projectId || record.projectId === normalizeProjectKey(projectId)); }
}

class TelegramArchiveAdapter {
  constructor(options = {}) { this.channelId = text(options.channelId || options.chatId, 100) || null; this.enabled = options.enabled === true; this.send = options.send || null; }
  async put() { if (!this.enabled) return { ok: false, error: 'Telegram archive adapter is disabled' }; if (typeof this.send !== 'function') return { ok: false, error: 'Telegram archive transport is not configured' }; return { ok: false, error: 'Telegram transport requires an approved adapter implementation' }; }
}

class ArchiveStore {
  constructor(options = {}) { this.provider = options.provider || new InMemoryArchiveProvider(); this.records = new Map(); this.operationKeys = new Map(); this.events = []; this.now = options.now || (() => new Date()); this.channel = options.channel || null; }
  async create(input, operationKey = null) { if (operationKey && this.operationKeys.has(operationKey)) return { ok: true, duplicate: true, record: { ...this.records.get(this.operationKeys.get(operationKey)) } }; const result = validateArchiveRecord({ ...input, archiveId: input.archiveId || `arc_${crypto.randomUUID()}`, createdAt: input.createdAt || this.now() }); if (!result.ok) return result; const stored = await this.provider.put(result.value, operationKey || result.value.archiveId); if (!stored.ok) return stored; this.records.set(stored.record.archiveId, stored.record); if (operationKey) this.operationKeys.set(operationKey, stored.record.archiveId); this.events.push({ category: 'ARCHIVE_CREATED', project: stored.record.projectId, archiveId: stored.record.archiveId, timestamp: stored.record.createdAt }); return { ok: true, duplicate: stored.duplicate, record: { ...stored.record } }; }
  async requestRestore(archiveId, projectId) { const record = this.records.get(String(archiveId)); if (!record || record.projectId !== normalizeProjectKey(projectId)) return { ok: false, error: 'archive record not found in project scope' }; if (record.restoreStatus === 'RESTORED') return { ok: true, duplicate: true, record: { ...record } }; record.restoreStatus = 'RESTORE_REQUESTED'; this.events.push({ category: 'ARCHIVE_RESTORED', project: record.projectId, archiveId: record.archiveId, timestamp: new Date(this.now()).toISOString() }); return { ok: true, duplicate: false, record: { ...record } }; }
  async markRestored(archiveId, projectId, restoredAt = this.now()) { const record = this.records.get(String(archiveId)); if (!record || record.projectId !== normalizeProjectKey(projectId) || record.restoreStatus !== 'RESTORE_REQUESTED') return { ok: false, error: 'restore request is not pending' }; record.restoreStatus = 'RESTORED'; record.restoredAt = iso(restoredAt); return { ok: true, record: { ...record } }; }
  list(projectId = null) { return [...this.records.values()].filter((record) => !projectId || record.projectId === normalizeProjectKey(projectId)); }
}

function archiveCandidate(input = {}, now = new Date()) { const unusedSince = iso(input.unusedSince); const ageMs = unusedSince ? new Date(now).getTime() - Date.parse(unusedSince) : 0; const eligible = ageMs >= 30 * 24 * 60 * 60 * 1000 && Number(input.activeStorageBytes || 0) > 0; return { candidate: eligible, destructiveAction: false, reason: eligible ? 'unused_for_approximately_month_and_storage_burden_present' : 'criteria_not_met', unusedSince, activeStorageBytes: Math.max(0, Number(input.activeStorageBytes) || 0) }; }

module.exports = { ARCHIVE_STATUSES, RESTORE_STATUSES, ARTIFACT_TYPES, hash, validateArchiveRecord, InMemoryArchiveProvider, TelegramArchiveAdapter, ArchiveStore, archiveCandidate };
