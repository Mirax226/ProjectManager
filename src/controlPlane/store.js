const { JOB_STATUSES, JOB_TYPES, normalizeProjectKey, classifyJob, validateOperationalEvent, redact } = require('./contracts');
const { operationalMethods } = require('./operationalState');
function randomId() { return globalThis.crypto?.randomUUID?.() || `id_${Date.now()}_${Math.random().toString(36).slice(2)}`; }

const DEFAULT_PROJECTS = [{
  id: 'daily-system', key: 'daily-system', name: 'DailySystem', environment: 'dev',
  runnerProfile: 'daily-system', capabilities: ['windows', 'git', 'tests', 'excel-diagnostics', 'codex'],
}];

function iso(now) { return new Date(now()).toISOString(); }

class ControlPlaneStore {
  constructor(options = {}) {
    this.now = options.now || (() => Date.now());
    this.runnerStaleMs = Number(options.runnerStaleMs || 90_000);
    this.runnerOfflineMs = Number(options.runnerOfflineMs || 300_000);
    this.projects = new Map((options.projects || DEFAULT_PROJECTS).map((project) => [project.id, { ...project }]));
    this.events = new Map(); this.eventOrder = []; this.jobs = new Map(); this.runners = new Map(); this.alerts = new Map();
    this.initOperationalState();
    this._ingestEventLocal = (event) => ControlPlaneStore.prototype.ingestEvent.call(this, event);
  }
  listProjects() { return [...this.projects.values()].map((project) => ({ ...project })); }
  getProject(id) { return this.projects.get(normalizeProjectKey(id)) || null; }
  status() {
    const runners = [...this.runners.values()].map((runner) => this.runnerView(runner));
    return { controlPlane: 'HEALTHY', projects: this.listProjects(), runners, openAlerts: [...this.alerts.values()].filter((a) => !a.acknowledged || a.status === 'RECOVERED'), latestEvents: this.eventOrder.slice(-20).reverse().map((id) => this.events.get(id)) };
  }
  ingestEvent(input) {
    const validation = validateOperationalEvent(input, { nowMs: this.now() });
    if (!validation.ok) return { ok: false, status: 400, error: validation.error };
    const event = validation.value;
    if (!this.projects.has(event.project)) return { ok: false, status: 403, error: 'project is not registered' };
    const prior = this.events.get(event.eventId);
    if (prior) return { ok: true, duplicate: true, event: prior };
    this.events.set(event.eventId, event); this.eventOrder.push(event.eventId);
    if (this.eventOrder.length > 2000) this.events.delete(this.eventOrder.shift());
    if (event.severity === 'ERROR' || event.severity === 'CRITICAL') {
      const key = `${event.project}:${event.category}:${event.environment}`;
      const existing = this.alerts.get(key);
      if (!existing || existing.acknowledged) this.alerts.set(key, { id: randomId(), key, project: event.project, category: event.category, environment: event.environment, message: event.message, eventId: event.eventId, createdAt: event.timestamp, acknowledged: false, status: 'ACTIVE' });
    }
    const recoveryCategories = { EXCEL_HEALTH_RECOVERED: ['EXCEL_FILE_NOT_FOUND', 'EXCEL_FINGERPRINT_MISMATCH', 'EXCEL_DESKTOP_UNAVAILABLE', 'EXCEL_OPEN_FAILED', 'EXCEL_AGENT_ERROR', 'EXCEL_RECONCILIATION_ERROR'], HEALTHCHECK_RECOVERED: ['HEALTHCHECK_FAILED'], BACKUP_COMPLETED: ['BACKUP_FAILED'], RUNNER_RECOVERED: ['RUNNER_OFFLINE'] };
    for (const category of recoveryCategories[event.category] || []) { const alert = this.alerts.get(`${event.project}:${category}:${event.environment}`); if (alert) { alert.acknowledged = true; alert.status = 'RECOVERED'; alert.recoveredAt = event.timestamp; } }
    return { ok: true, duplicate: false, event };
  }
  createJob(input, requestedBy = 'api') {
    const projectId = normalizeProjectKey(input?.projectId || input?.project);
    const type = String(input?.type || '').toUpperCase();
    if (!projectId || !this.projects.has(projectId)) return { ok: false, status: 400, error: 'unknown projectId' };
    if (!JOB_TYPES.includes(type)) return { ok: false, status: 400, error: 'unsupported job type' };
    const risk = classifyJob(type);
    if (!risk || risk === 'DANGEROUS') return { ok: false, status: 403, error: 'job type is disabled' };
    if (input.payload != null && (typeof input.payload !== 'object' || Array.isArray(input.payload))) return { ok: false, status: 400, error: 'payload must be an object' };
    const now = iso(this.now); const idempotencyKey = String(input.idempotencyKey || '').slice(0, 160);
    if (idempotencyKey) { const prior = [...this.jobs.values()].find((job) => job.projectId === projectId && job.idempotencyKey === idempotencyKey); if (prior) return { ok: true, duplicate: true, job: { ...prior } }; }
    const job = { id: randomId(), projectId, type, risk, status: 'PENDING', createdAt: now, updatedAt: now, requestedBy: String(requestedBy).slice(0, 120), payload: redact(input.payload || {}), attemptCount: 0, leaseOwner: null, leaseExpiresAt: null, result: null, error: null, idempotencyKey: idempotencyKey || null };
    this.jobs.set(job.id, job); return { ok: true, duplicate: false, job: { ...job } };
  }
  getJob(id, projectId = null) { const job = this.jobs.get(String(id)); return job && (!projectId || job.projectId === projectId) ? { ...job } : null; }
  listJobs(projectId = null) { return [...this.jobs.values()].filter((job) => !projectId || job.projectId === projectId).sort((a, b) => b.createdAt.localeCompare(a.createdAt)).map((job) => ({ ...job })); }
  claimJob(runnerId, projectId = null, leaseMs = 120_000) {
    const nowMs = this.now();
    const candidate = [...this.jobs.values()].filter((job) => (!projectId || job.projectId === projectId) && (job.status === 'PENDING' || (job.status === 'CLAIMED' && Date.parse(job.leaseExpiresAt || 0) <= nowMs))).sort((a, b) => a.createdAt.localeCompare(b.createdAt))[0];
    if (!candidate) return null;
    candidate.status = 'CLAIMED'; candidate.leaseOwner = String(runnerId); candidate.leaseExpiresAt = new Date(nowMs + leaseMs).toISOString(); candidate.attemptCount += 1; candidate.updatedAt = new Date(nowMs).toISOString();
    return { ...candidate };
  }
  resultJob(id, runnerId, result = {}) {
    const job = this.jobs.get(String(id)); if (!job) return { ok: false, status: 404, error: 'job not found' };
    if (['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(job.status)) return { ok: true, duplicate: true, job: { ...job } };
    if (job.leaseOwner !== String(runnerId) || (job.leaseExpiresAt && Date.parse(job.leaseExpiresAt) < this.now())) return { ok: false, status: 409, error: 'lease is not owned by runner' };
    const retryable = result.retryable === true && (result.ok === false || result.error);
    job.status = retryable ? 'PENDING' : (result.ok === false || result.error ? 'FAILED' : 'SUCCEEDED'); job.result = redact(result.result || result); job.error = result.error ? String(result.error).slice(0, 1000) : null; job.updatedAt = iso(this.now); job.leaseOwner = null; job.leaseExpiresAt = null;
    if (job.status === 'SUCCEEDED' && result.result?.operation && ['BACKUP', 'SNAPSHOT'].includes(result.result.operation)) {
      const backup = this._createBackupLocal({ backupId: result.result.backupId, projectId: job.projectId, assetId: result.result.assetId, sourceHash: result.result.sourceHash, backupHash: result.result.backupHash, createdAt: result.result.createdAt, localBackupReference: result.result.localBackupReference, verificationStatus: result.result.verificationStatus, retentionClass: 'CURRENT_TODAY' }, job.idempotencyKey || job.id);
      if (backup.ok) job.result = { ...job.result, backupRecordId: backup.backup.backupId };
    }
    if (job.status === 'SUCCEEDED' && result.result?.status && ['MATCH', 'MISMATCH', 'ERROR', 'EXPECTED_INTENTIONAL_DIVERGENCE', 'KNOWN_LEGACY_DEFECT'].includes(String(result.result.status).toUpperCase())) this._saveReconciliationResultLocal(result.result);
    const approvedResultEvents = new Set(['BACKUP_COMPLETED', 'BACKUP_FAILED', 'EXCEL_AGENT_ERROR', 'EXCEL_FILE_NOT_FOUND', 'EXCEL_FINGERPRINT_MISMATCH', 'EXCEL_DESKTOP_UNAVAILABLE', 'EXCEL_OPEN_FAILED', 'EXCEL_HEALTH_RECOVERED', 'EXCEL_BACKUP_HASH_MISMATCH', 'EXCEL_RECONCILIATION_ERROR']);
    if (approvedResultEvents.has(result.eventCategory) && this.projects.has(job.projectId)) this.ingestEvent({ schemaVersion: 1, eventId: `job:${job.id}:${job.attemptCount}:${result.eventCategory}`, project: job.projectId, environment: this.projects.get(job.projectId).environment || 'dev', severity: result.ok === false || result.error ? 'ERROR' : 'INFO', category: result.eventCategory, timestamp: job.updatedAt, message: result.ok === false || result.error ? `Typed job ${job.type} failed` : `Typed job ${job.type} completed`, source: 'runner', correlationId: job.id, context: { jobId: job.id, attemptCount: job.attemptCount, diagnosticCode: result.diagnosticCode || null } });
    return { ok: true, duplicate: false, job: { ...job } };
  }
  heartbeat(input) {
    const id = String(input?.runnerId || '').trim(); if (!id) return { ok: false, status: 400, error: 'runnerId is required' };
    const prior = this.runners.get(id); const nowIso = iso(this.now);
    const runner = { runnerId: id, projectId: normalizeProjectKey(input.projectId) || null, status: 'ONLINE', lastSeenAt: nowIso, version: String(input.version || '').slice(0, 80), hostLabel: String(input.hostLabel || '').slice(0, 80), capabilities: Array.isArray(input.capabilities) ? input.capabilities.slice(0, 30).map((x) => String(x).slice(0, 60)) : [] };
    this.runners.set(id, runner);
    if (prior && prior.status === 'OFFLINE') this.ingestEvent({ schemaVersion: 1, eventId: `runner-recovered:${id}:${nowIso.slice(0, 13)}`, project: runner.projectId || 'daily-system', environment: 'control-plane', severity: 'INFO', category: 'RUNNER_RECOVERED', timestamp: nowIso, message: `Runner ${id} recovered`, source: 'control-plane' });
    return { ok: true, runner: this.runnerView(runner) };
  }
  runnerView(runner) { const age = this.now() - Date.parse(runner.lastSeenAt); return { ...runner, status: age > this.runnerOfflineMs ? 'OFFLINE' : age > this.runnerStaleMs ? 'STALE' : 'ONLINE' }; }
  refreshRunnerStates() { for (const runner of this.runners.values()) { const view = this.runnerView(runner); if (view.status === 'OFFLINE' && runner.status !== 'OFFLINE') { runner.status = 'OFFLINE'; this.ingestEvent({ schemaVersion: 1, eventId: `runner-offline:${runner.runnerId}:${runner.lastSeenAt}`, project: runner.projectId || 'daily-system', environment: 'control-plane', severity: 'ERROR', category: 'RUNNER_OFFLINE', timestamp: new Date(this.now()).toISOString(), message: `Runner ${runner.runnerId} is offline`, source: 'control-plane' }); } } }
  acknowledgeAlert(id) { for (const alert of this.alerts.values()) if (alert.id === id) { alert.acknowledged = true; return { ...alert }; } return null; }
}

module.exports = { ControlPlaneStore, DEFAULT_PROJECTS };

Object.assign(ControlPlaneStore.prototype, operationalMethods);
