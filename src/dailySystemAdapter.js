const DEFAULT_TIMEOUT_MS = 5000;
const MAX_BODY_BYTES = 32 * 1024;
const OPERATIONS = Object.freeze({
  health: { method: 'GET', path: '/healthz' },
  excelSyncDiagnostic: { method: 'POST', path: '/api/v1/ops/excel-sync-diagnostic' },
  excelReconciliation: { method: 'POST', path: '/api/v1/ops/excel-reconciliation' },
  projectStatus: { method: 'GET', path: '/api/v1/status' },
});
const SAFE_DIAGNOSTIC_KEYS = Object.freeze(['code', 'message', 'component', 'source', 'reference', 'retryable']);
const SAFE_HASH_KEYS = Object.freeze(['sourceHash', 'destinationHash', 'fingerprint', 'sourceFingerprint']);

function text(value, max = 240) { return String(value == null ? '' : value).trim().slice(0, max); }

function safeMessage(value) {
  return text(value, 240)
    .replace(/((?:token|secret|password|authorization|api[-_]?key|cookie|dsn|private[-_]?key)\s*[:=]\s*)[^\s,;]+/gi, '$1[REDACTED]')
    .replace(/Bearer\s+[^\s]+/gi, 'Bearer [REDACTED]')
    .replace(/(postgres(?:ql)?:\/\/)[^\s@]+@/gi, '$1[REDACTED]@');
}

function correlationId(value) {
  const id = text(value, 120);
  return /^[A-Za-z0-9][A-Za-z0-9._:-]{0,119}$/.test(id) ? id : `ds_${Date.now().toString(36)}`;
}

function safeObject(value, keys, depth = 0) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || depth > 2) return {};
  return Object.fromEntries(keys
    .filter((key) => Object.prototype.hasOwnProperty.call(value, key))
    .map((key) => [key, typeof value[key] === 'object' ? safeObject(value[key], keys, depth + 1) : (typeof value[key] === 'string' ? text(value[key], 500) : value[key])]));
}

function sanitizeDiagnostics(value) { return safeObject(value, SAFE_DIAGNOSTIC_KEYS); }
function sanitizeHashes(value) { return safeObject(value, SAFE_HASH_KEYS); }

function sanitizeRequest(operation, input = {}) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  if (operation === 'excelReconciliation') {
    return {
      workId: text(source.workId, 160),
      destination: text(source.destination, 200),
      status: text(source.status, 60).toUpperCase(),
    };
  }
  if (operation === 'excelSyncDiagnostic') return { assetId: text(source.assetId, 160), mode: 'read_only' };
  return {};
}

function sanitizeResponse(operation, body, correlation) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return null;
  const source = body.result && typeof body.result === 'object' ? body.result : body;
  if (operation === 'excelReconciliation') {
    const status = text(source.status, 60).toUpperCase();
    if (!text(source.workId, 160) || !['MATCH', 'MISMATCH', 'ERROR', 'EXPECTED_INTENTIONAL_DIVERGENCE', 'KNOWN_LEGACY_DEFECT'].includes(status)) return null;
    return {
      workId: text(source.workId, 160), destination: text(source.destination, 200), status,
      mismatchCount: Math.max(0, Number(source.mismatchCount) || 0), errorCount: Math.max(0, Number(source.errorCount) || 0),
      retryable: source.retryable === true, hashes: sanitizeHashes(source.hashes || source), diagnostics: sanitizeDiagnostics(source.diagnostics), correlationId: correlation,
    };
  }
  if (operation === 'excelSyncDiagnostic') {
    const status = text(source.status || (source.ok === true ? 'HEALTHY' : ''), 60).toUpperCase();
    if (!status) return null;
    return { status, assetId: text(source.assetId, 160), sourceHash: text(source.sourceHash, 128) || null, destination: text(source.destination, 200), diagnostics: sanitizeDiagnostics(source.diagnostics), correlationId: correlation };
  }
  const status = text(source.status || (source.ok === true ? 'HEALTHY' : ''), 60).toUpperCase();
  if (!status) return null;
  return { status, service: text(source.service || source.project || 'daily-system', 120), version: text(source.version, 80), diagnostics: sanitizeDiagnostics(source.diagnostics), correlationId: correlation };
}

function classifyFailure(error = {}) {
  const status = Number(error.status || 0);
  if (error.code === 'TIMEOUT' || error.name === 'AbortError') return { code: 'TIMEOUT', retryable: true };
  if (status === 401 || status === 403) return { code: 'UNAUTHORIZED', retryable: false };
  if (status === 404) return { code: 'UNAVAILABLE', retryable: true };
  if (status === 408 || status === 429 || status >= 500) return { code: 'HTTP_ERROR', retryable: true };
  if (error.code === 'MALFORMED_RESPONSE') return { code: 'MALFORMED_RESPONSE', retryable: false };
  if (error.code === 'NETWORK_ERROR' || error.name === 'TypeError') return { code: 'UNAVAILABLE', retryable: true };
  return { code: 'HTTP_ERROR', retryable: false };
}

function safeError(error, classification, correlation) {
  return { code: classification.code, retryable: classification.retryable, message: safeMessage(error?.message || classification.code), correlationId: correlation };
}

async function readJsonBody(response) {
  if (typeof response.text === 'function') {
    const raw = await response.text();
    const bytes = typeof Buffer !== 'undefined' ? Buffer.byteLength(raw, 'utf8') : new TextEncoder().encode(raw).length;
    if (bytes > MAX_BODY_BYTES) throw Object.assign(new Error('DailySystem response exceeded the bounded body limit'), { code: 'MALFORMED_RESPONSE' });
    try { return JSON.parse(raw); } catch (_) { throw Object.assign(new Error('DailySystem returned malformed JSON'), { code: 'MALFORMED_RESPONSE' }); }
  }
  if (typeof response.json !== 'function') throw Object.assign(new Error('DailySystem response body was unavailable'), { code: 'MALFORMED_RESPONSE' });
  const body = await response.json();
  let bytes = 0;
  try { bytes = typeof Buffer !== 'undefined' ? Buffer.byteLength(JSON.stringify(body), 'utf8') : new TextEncoder().encode(JSON.stringify(body)).length; } catch (_) { bytes = MAX_BODY_BYTES + 1; }
  if (bytes > MAX_BODY_BYTES) throw Object.assign(new Error('DailySystem response exceeded the bounded body limit'), { code: 'MALFORMED_RESPONSE' });
  return body;
}

function createDailySystemAdapter(options = {}) {
  const baseUrl = String(options.baseUrl || '').replace(/\/$/, '');
  const fetchImpl = options.fetch || globalThis.fetch;
  const timeoutMs = Math.min(30_000, Math.max(100, Number(options.timeoutMs || DEFAULT_TIMEOUT_MS)));
  const token = String(options.token || '');
  if (!baseUrl) throw new Error('DailySystem adapter baseUrl is required');
  if (typeof fetchImpl !== 'function') throw new Error('DailySystem adapter fetch is required');

  async function call(operation, input = {}, requestCorrelationId = null) {
    const definition = OPERATIONS[operation];
    if (!definition) return { ok: false, operation, correlationId: correlationId(requestCorrelationId), error: { code: 'UNSUPPORTED_OPERATION', retryable: false, message: 'unsupported DailySystem operation' } };
    const corr = correlationId(requestCorrelationId);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const headers = { accept: 'application/json', 'x-correlation-id': corr };
    if (token) headers.authorization = `Bearer ${token}`;
    const request = { method: definition.method, headers, signal: controller.signal };
    if (definition.method !== 'GET') { headers['content-type'] = 'application/json'; request.body = JSON.stringify(sanitizeRequest(operation, input)); }
    try {
      const response = await fetchImpl(`${baseUrl}${definition.path}`, request);
      if (!response || typeof response.status !== 'number') throw Object.assign(new Error('DailySystem response was unavailable'), { code: 'NETWORK_ERROR' });
      if (!response.ok) {
        const classification = classifyFailure({ status: response.status });
        return { ok: false, operation, correlationId: corr, error: safeError({ message: `DailySystem HTTP ${response.status}`, status: response.status }, classification, corr) };
      }
      let body;
      try { body = await readJsonBody(response); } catch (error) { const classification = classifyFailure({ code: 'MALFORMED_RESPONSE' }); return { ok: false, operation, correlationId: corr, error: safeError(error, classification, corr) }; }
      const value = sanitizeResponse(operation, body, corr);
      if (!value) { const classification = classifyFailure({ code: 'MALFORMED_RESPONSE' }); return { ok: false, operation, correlationId: corr, error: safeError({ message: 'DailySystem response shape was invalid' }, classification, corr) }; }
      return { ok: true, operation, correlationId: corr, value };
    } catch (error) {
      const classification = classifyFailure(error);
      return { ok: false, operation, correlationId: corr, error: safeError(error, classification, corr) };
    } finally { clearTimeout(timer); }
  }
  return { call, health: (corr) => call('health', {}, corr), excelSyncDiagnostic: (input, corr) => call('excelSyncDiagnostic', input, corr), excelReconciliation: (input, corr) => call('excelReconciliation', input, corr), projectStatus: (corr) => call('projectStatus', {}, corr) };
}

function createFakeDailySystemAdapter(fixtures = {}) {
  const call = async (operation, input, correlation) => { const fixture = typeof fixtures[operation] === 'function' ? await fixtures[operation](input, correlation) : fixtures[operation]; return fixture || { ok: false, operation, correlationId: correlationId(correlation), error: { code: 'UNAVAILABLE', retryable: true, message: 'fake DailySystem endpoint unavailable', correlationId: correlationId(correlation) } }; };
  return { call, health: (corr) => call('health', {}, corr), excelSyncDiagnostic: (input, corr) => call('excelSyncDiagnostic', input, corr), excelReconciliation: (input, corr) => call('excelReconciliation', input, corr), projectStatus: (corr) => call('projectStatus', {}, corr) };
}

module.exports = { DEFAULT_TIMEOUT_MS, MAX_BODY_BYTES, OPERATIONS, sanitizeRequest, sanitizeResponse, classifyFailure, createDailySystemAdapter, createFakeDailySystemAdapter };
