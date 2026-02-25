const { loadCronSettings } = require('../settingsStore');
const { resolveCronProvider } = require('./cronProviders');

async function getActiveProvider() {
  const settings = await loadCronSettings();
  const providerName = settings?.provider || 'none';
  return resolveCronProvider(providerName);
}

function normalizeNotFound(error) {
  if (error?.status === 404) {
    throw new Error('Cron job not found or invalid request');
  }
  throw error;
}

const CRON_API_TOKEN =
  process.env.CRON_API_TOKEN || process.env.CRON_API_KEY || process.env.CRONJOB_API_KEY;
const CRON_API_BASE = process.env.CRONJOB_API_BASE || 'https://api.cron-job.org';

async function callCronApi(method, path, body) {
  const provider = await getActiveProvider();
  if (typeof provider.callCronApi !== 'function') {
    throw new Error('Active provider does not support raw API calls');
  }
  return provider.callCronApi(method, path, body);
}

async function listJobs() {
  const provider = await getActiveProvider();
  return provider.listJobs();
}

async function getJob(jobId) {
  const provider = await getActiveProvider();
  try {
    return await provider.getJob(jobId);
  } catch (error) {
    normalizeNotFound(error);
  }
}

async function createJob(payload) {
  const provider = await getActiveProvider();
  return provider.createJob(payload);
}

async function updateJob(jobId, patch) {
  const provider = await getActiveProvider();
  try {
    return await provider.updateJob(jobId, patch);
  } catch (error) {
    normalizeNotFound(error);
  }
}

async function toggleJob(jobId, enabled) {
  return updateJob(jobId, { enabled });
}

async function deleteJob(jobId) {
  const provider = await getActiveProvider();
  try {
    return await provider.deleteJob(jobId);
  } catch (error) {
    normalizeNotFound(error);
  }
}

async function pingProvider() {
  const provider = await getActiveProvider();
  return provider.ping();
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
  pingProvider,
};
