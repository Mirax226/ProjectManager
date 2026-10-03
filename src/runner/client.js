class RunnerClient {
  constructor(options = {}) { this.baseUrl = String(options.baseUrl || process.env.PG_CONTROL_PLANE_URL || '').replace(/\/$/, ''); this.token = String(options.token || process.env.PG_RUNNER_TOKEN || ''); this.runnerId = options.runnerId || process.env.PG_RUNNER_ID || 'windows-runner'; this.projectId = options.projectId || process.env.PG_PROJECT_ID || 'daily-system'; this.fetch = options.fetch || globalThis.fetch; }
  async request(path, method, body) { if (!this.baseUrl || !this.token) throw new Error('runner control-plane configuration is incomplete'); const response = await this.fetch(`${this.baseUrl}${path}`, { method, headers: { authorization: `Bearer ${this.token}`, 'content-type': 'application/json' }, body: body == null ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10000) }); const payload = await response.json(); if (!response.ok) throw new Error(payload.error || `control plane ${response.status}`); return payload; }
  heartbeat(extra = {}) { return this.request('/api/v1/runners/heartbeat', 'POST', { runnerId: this.runnerId, projectId: this.projectId, version: extra.version || process.env.npm_package_version || 'local', hostLabel: extra.hostLabel || process.env.PG_RUNNER_HOST_LABEL || undefined, capabilities: this.projectId === 'zj' ? ['git', 'tests', 'typecheck', 'release-evidence', 'staging-health'] : ['git', 'tests', 'typecheck', 'excel-diagnostics', 'codex'] }); }
  claim() { return this.request(`/api/v1/jobs/claim?projectId=${encodeURIComponent(this.projectId)}`, 'POST', { projectId: this.projectId }); }
  result(jobId, result) { return this.request(`/api/v1/jobs/${encodeURIComponent(jobId)}/result`, 'POST', result); }
}

module.exports = { RunnerClient };
