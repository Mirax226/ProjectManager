const { normalizeProjectKey, redact } = require('./controlPlane/contracts');

const HEALTH_STATES = Object.freeze(['HEALTHY', 'DEGRADED', 'DOWN', 'UNKNOWN']);

function safeNumber(value) { return Math.max(0, Number(value) || 0); }
function createDeterministicProviderProbe(options = {}) {
  return async function probe(provider = {}) {
    const started = Date.now();
    if (options.delayMs) await new Promise((resolve) => setTimeout(resolve, Math.min(5000, safeNumber(options.delayMs))));
    if (options.timeout) return { ok: false, health: 'DOWN', error: 'provider probe timed out', latencyMs: Date.now() - started };
    if (options.error) return { ok: false, health: options.degraded ? 'DEGRADED' : 'DOWN', error: String(options.error).slice(0, 200), latencyMs: Date.now() - started };
    return { ok: true, health: options.degraded ? 'DEGRADED' : 'HEALTHY', latencyMs: Date.now() - started, providerId: provider.providerId || provider.id || null };
  };
}

async function probeProvider(provider, probe = createDeterministicProviderProbe()) {
  const project = normalizeProjectKey(provider.project || provider.projectId);
  if (!project) return { ok: false, error: 'provider project is invalid' };
  const result = await probe(provider);
  const health = HEALTH_STATES.includes(String(result.health || '').toUpperCase()) ? String(result.health).toUpperCase() : 'UNKNOWN';
  const now = new Date().toISOString();
  return { ok: result.ok === true, value: {
    providerId: String(provider.providerId || provider.id || provider.name || '').slice(0, 120), project, name: String(provider.name || provider.providerId || provider.id || 'provider').slice(0, 120), serviceType: String(provider.serviceType || provider.type || 'OTHER').toUpperCase(), status: provider.enabled === false ? 'DISABLED' : 'ENABLED', enabled: provider.enabled !== false,
    health, lastSuccess: result.ok === true ? now : provider.lastSuccess || null, lastFailure: result.ok === true ? provider.lastFailure || null : now,
    latencyMs: safeNumber(result.latencyMs), requestCount: safeNumber(provider.requestCount) + 1, errorCount: safeNumber(provider.errorCount) + (result.ok === true ? 0 : 1),
    limits: redact(provider.limits || {}), error: result.ok === true ? null : String(result.error || 'provider probe failed').slice(0, 200),
  } };
}

module.exports = { HEALTH_STATES, createDeterministicProviderProbe, probeProvider };
