const fetch = require('node-fetch');
const { createPmLogger, createPmFetch } = require('../pmLogger');

const CRON_API_TOKEN =
  process.env.CRON_API_TOKEN || process.env.CRON_API_KEY || process.env.CRONJOB_API_KEY;
const CRON_API_BASE = process.env.CRONJOB_API_BASE || 'https://api.cron-job.org';
const pmLogger = createPmLogger();
const pmFetch = createPmFetch(fetch, pmLogger);

function assertCronApiKey() {
  if (!CRON_API_TOKEN) {
    throw new Error('CRON_API_TOKEN not configured');
  }
}

function buildCronError({ method, path, status, body, message }) {
  const summary = message || `Cron API ${method} ${path} failed`;
  const error = new Error(summary);
  error.status = status;
  error.body = body;
  error.method = method;
  error.path = path;
  return error;
}

async function callCronApi(method, path, body) {
  assertCronApiKey();
  const url = `${CRON_API_BASE}${path}`;
  const requestBody = body ?? null;
  const options = {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${CRON_API_TOKEN}`,
    },
  };
  if (body) {
    const payload = ['POST', 'PUT', 'PATCH'].includes(method) ? { job: body } : body;
    options.body = JSON.stringify(payload);
  }

  let response;
  let text = '';
  try {
    response = await pmFetch(url, options);
    text = await response.text();
  } catch (error) {
    throw buildCronError({
      method,
      path,
      message: `Cron API ${method} ${path} failed: ${error.message}`,
    });
  }

  if (!response.ok) {
    const excerpt = text ? text.slice(0, 200) : '';
    if (response.status === 429) {
      throw buildCronError({ method, path, status: response.status, body: text, message: 'Cron API rate limited (429)' });
    }
    throw buildCronError({
      method,
      path,
      status: response.status,
      body: text,
      message: `Cron API ${method} ${path} failed (${response.status}): ${excerpt || 'request failed'}`,
    });
  }

  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (_error) {
    throw buildCronError({ method, path, status: response.status, body: text, message: `Cron API ${method} ${path} returned invalid JSON.` });
  }
}

async function listJobs() {
  const data = await callCronApi('GET', '/jobs');
  return {
    jobs: Array.isArray(data?.jobs) ? data.jobs : [],
    someFailed: data?.someFailed === true,
  };
}

async function getJob(jobId) {
  const data = await callCronApi('GET', `/jobs/${jobId}`);
  return data?.jobDetails || data?.job || data;
}

async function createJob(payload) {
  const attempts = [
    { method: 'POST', path: '/jobs' },
    { method: 'PUT', path: '/jobs' },
  ];
  let lastError = null;
  for (let index = 0; index < attempts.length; index += 1) {
    const attempt = attempts[index];
    try {
      const data = await callCronApi(attempt.method, attempt.path, payload);
      const id = data?.jobId || data?.job?.jobId || data?.job?.id || data?.id;
      return { id: id != null ? String(id) : null };
    } catch (error) {
      lastError = error;
      const canFallback = index < attempts.length - 1 && (error?.status === 404 || error?.status === 405);
      if (!canFallback) throw error;
    }
  }
  throw lastError;
}

async function updateJob(jobId, patch) {
  await callCronApi('PATCH', `/jobs/${jobId}`, patch);
}

async function deleteJob(jobId) {
  await callCronApi('DELETE', `/jobs/${jobId}`);
}

async function ping() {
  await callCronApi('GET', '/jobs');
  return { ok: true, provider: 'cronjob_org' };
}

module.exports = {
  name: 'cronjob_org',
  supportsTest: false,
  triggerTest: null,
  CRON_API_TOKEN,
  CRON_API_BASE,
  callCronApi,
  listJobs,
  getJob,
  createJob,
  updateJob,
  deleteJob,
  ping,
};
