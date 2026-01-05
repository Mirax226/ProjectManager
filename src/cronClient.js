const fetch = require('node-fetch');

const CRON_API_KEY = process.env.CRONJOB_API_KEY;
const CRON_API_BASE = process.env.CRONJOB_API_BASE || 'https://api.cron-job.org';

function assertCronApiKey() {
  if (!CRON_API_KEY) {
    throw new Error('CRONJOB_API_KEY not configured');
  }
}

async function cronRequest(method, path, body) {
  assertCronApiKey();
  const url = `${CRON_API_BASE}${path}`;
  const options = {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${CRON_API_KEY}`,
    },
  };
  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url, options);
  const text = await response.text();

  if (!response.ok) {
    const excerpt = text ? text.slice(0, 200) : '';
    const error = new Error(`Cron API ${response.status}: ${excerpt || 'request failed'}`);
    console.error('[cronClient] Request failed', error.message);
    throw error;
  }

  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (error) {
    console.error('[cronClient] Failed to parse response JSON', error);
    return null;
  }
}

async function listJobs() {
  const data = await cronRequest('GET', '/jobs');
  if (!data) return [];
  return Array.isArray(data.jobs) ? data.jobs : data.jobs?.jobs || [];
}

async function getJob(jobId) {
  const data = await cronRequest('GET', `/jobs/${jobId}`);
  return data?.job || data;
}

async function createJob(payload) {
  const data = await cronRequest('POST', '/jobs', { job: payload });
  const id = data?.jobId || data?.job?.jobId || data?.job?.id || data?.id;
  return { id: id != null ? String(id) : null };
}

async function updateJob(jobId, payload) {
  await cronRequest('PUT', `/jobs/${jobId}`, { job: payload });
}

async function deleteJob(jobId) {
  await cronRequest('DELETE', `/jobs/${jobId}`);
}

module.exports = {
  CRON_API_KEY,
  CRON_API_BASE,
  cronRequest,
  listJobs,
  getJob,
  createJob,
  updateJob,
  deleteJob,
};
