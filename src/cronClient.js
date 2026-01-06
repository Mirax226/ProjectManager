const fetch = require('node-fetch');

const CRON_API_TOKEN =
  process.env.CRON_API_TOKEN || process.env.CRON_API_KEY || process.env.CRONJOB_API_KEY;
const CRON_API_BASE = process.env.CRONJOB_API_BASE || 'https://api.cron-job.org';

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
    response = await fetch(url, options);
    text = await response.text();
  } catch (error) {
    console.error('[cronClient] Network error', { method, path, error: error.message });
    throw buildCronError({
      method,
      path,
      message: `Cron API ${method} ${path} failed: ${error.message}`,
    });
  }

  if (!response.ok) {
    const excerpt = text ? text.slice(0, 200) : '';
    const message = `Cron API ${method} ${path} failed (${response.status}): ${
      excerpt || 'request failed'
    }`;
    console.error('[cronClient] Request failed', {
      method,
      path,
      status: response.status,
      body: text,
    });
    throw buildCronError({
      method,
      path,
      status: response.status,
      body: text,
      message,
    });
  }

  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (error) {
    console.error('[cronClient] Failed to parse response JSON', {
      method,
      path,
      error: error.message,
    });
    throw buildCronError({
      method,
      path,
      status: response.status,
      body: text,
      message: `Cron API ${method} ${path} returned invalid JSON.`,
    });
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
  try {
    const data = await callCronApi('GET', `/jobs/${jobId}`);
    return data?.jobDetails || data?.job || data;
  } catch (error) {
    if (error?.status === 404) {
      throw new Error('Cron job not found or invalid request');
    }
    throw error;
  }
}

async function createJob(payload) {
  const data = await callCronApi('PUT', '/jobs', payload);
  const id = data?.jobId || data?.job?.jobId || data?.job?.id || data?.id;
  return { id: id != null ? String(id) : null };
}

async function updateJob(jobId, patch) {
  try {
    await callCronApi('PATCH', `/jobs/${jobId}`, patch);
  } catch (error) {
    if (error?.status === 404) {
      throw new Error('Cron job not found or invalid request');
    }
    throw error;
  }
}

async function toggleJob(jobId, enabled) {
  return updateJob(jobId, { enabled });
}

async function deleteJob(jobId) {
  try {
    await callCronApi('DELETE', `/jobs/${jobId}`);
  } catch (error) {
    if (error?.status === 404) {
      throw new Error('Cron job not found or invalid request');
    }
    throw error;
  }
}

module.exports = {
  CRON_API_TOKEN,
  CRON_API_BASE,
  callCronApi,
  listJobs,
  getJob,
  createJob,
  updateJob,
  toggleJob,
  deleteJob,
};
