const http = require('http');
const https = require('https');
const crypto = require('crypto');

const RENDER_API_BASE_URL = 'https://api.render.com/v1';
const DEFAULT_TIMEOUT_MS = 8000;
const DEFAULT_RETRIES = 2;

function createRequestId() {
  return crypto.randomBytes(6).toString('hex');
}

function toRequestPath(path, query = {}) {
  const url = new URL(path, RENDER_API_BASE_URL);
  Object.entries(query || {}).forEach(([key, value]) => {
    if (value == null || value === '') return;
    url.searchParams.set(key, String(value));
  });
  return `${url.pathname}${url.search}`;
}

function requestRender({ method, path, apiKey, body, timeoutMs, requestId }) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, RENDER_API_BASE_URL);
    const isHttps = url.protocol === 'https:';
    const lib = isHttps ? https : http;
    const payload = body != null ? JSON.stringify(body) : null;
    const headers = {
      Authorization: `Bearer ${apiKey}`,
      Accept: 'application/json',
      'X-Request-Id': requestId,
      ...(payload
        ? {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(payload),
          }
        : {}),
    };
    const options = {
      method,
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: `${url.pathname}${url.search}`,
      headers,
    };
    const startedAt = Date.now();
    const req = lib.request(options, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        let parsed = null;
        if (data) {
          try {
            parsed = JSON.parse(data);
          } catch (error) {
            parsed = null;
          }
        }
        resolve({
          status: Number(res.statusCode) || 0,
          body: parsed,
          durationMs: Date.now() - startedAt,
        });
      });
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error('Render request timed out'));
    });
    if (payload) req.write(payload);
    req.end();
  });
}

function extractDataArray(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.deploys)) return payload.deploys;
  return [];
}

function parseDeploy(deploy) {
  if (!deploy || typeof deploy !== 'object') {
    return {
      deployId: null,
      status: null,
      updatedAt: null,
      commitId: null,
      trigger: null,
      image: null,
    };
  }
  return {
    deployId: deploy.id || deploy.deployId || deploy.deploy?.id || null,
    status: deploy.status || deploy.state || deploy.result || null,
    updatedAt:
      deploy.updatedAt || deploy.updated_at || deploy.finishedAt || deploy.finished_at || deploy.createdAt || null,
    commitId:
      deploy.commit?.id || deploy.commitId || deploy.commit_id || deploy.trigger?.commitId || deploy.trigger?.commit_id || null,
    trigger: deploy.trigger?.type || deploy.trigger || deploy.createdBy || null,
    image: deploy.image || deploy.imageUrl || deploy.artifact || null,
  };
}

async function requestWithRetry({ method, path, query, body, apiKey, timeoutMs, retries }) {
  if (!apiKey) {
    throw new Error('RENDER_API_KEY not configured.');
  }
  const requestId = createRequestId();
  const endpoint = toRequestPath(path, query);
  let lastError = null;
  const attempts = Math.max(0, Number(retries)) + 1;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await requestRender({
        method,
        path: endpoint,
        body,
        apiKey,
        requestId,
        timeoutMs: timeoutMs || DEFAULT_TIMEOUT_MS,
      });
      if (response.status >= 200 && response.status < 300) {
        return { ...response, endpoint, requestId, refId: `${requestId}-${attempt}` };
      }
      if (response.status >= 500 && attempt < attempts) {
        continue;
      }
      const error = new Error(`Render API failed (${response.status || 'unknown'}).`);
      error.httpStatus = response.status;
      error.endpoint = endpoint;
      error.requestId = requestId;
      error.refId = `${requestId}-${attempt}`;
      throw error;
    } catch (error) {
      lastError = error;
      if (attempt >= attempts) break;
    }
  }
  throw lastError || new Error('Render API request failed.');
}

async function listServiceDeploys(serviceId, options = {}) {
  const response = await requestWithRetry({
    method: 'GET',
    path: `/services/${serviceId}/deploys`,
    query: {
      limit: options.limit || 5,
      cursor: options.cursor || undefined,
    },
    timeoutMs: options.timeoutMs,
    retries: options.retries == null ? DEFAULT_RETRIES : options.retries,
    apiKey: options.apiKey,
  });
  const deploys = extractDataArray(response.body);
  return {
    deploys,
    diagnostics: {
      endpoint: response.endpoint,
      httpStatus: response.status,
      durationMs: response.durationMs,
      returnedCount: deploys.length,
      requestId: response.requestId,
      refId: response.refId,
    },
  };
}

async function getService(serviceId, options = {}) {
  const response = await requestWithRetry({
    method: 'GET',
    path: `/services/${serviceId}`,
    timeoutMs: options.timeoutMs,
    retries: options.retries == null ? DEFAULT_RETRIES : options.retries,
    apiKey: options.apiKey,
  });
  return {
    service: response.body?.service || response.body?.data || response.body || null,
    diagnostics: {
      endpoint: response.endpoint,
      httpStatus: response.status,
      durationMs: response.durationMs,
      returnedCount: response.body ? 1 : 0,
      requestId: response.requestId,
      refId: response.refId,
    },
  };
}

module.exports = {
  listServiceDeploys,
  getService,
  parseDeploy,
};
