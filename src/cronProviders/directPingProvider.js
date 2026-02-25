async function blockedResponse() {
  return { ok: false, blocked: true, reason: 'provider_not_configured' };
}

module.exports = {
  name: 'direct_ping',
  supportsTest: true,
  async listJobs() {
    return { jobs: [], someFailed: false, blocked: true, reason: 'provider_not_configured' };
  },
  async getJob() {
    throw new Error('Cron provider is not configured.');
  },
  async createJob() {
    throw new Error('Cron provider is not configured. Enable cronjob_org or use local scheduler mode.');
  },
  async updateJob() {
    throw new Error('Cron provider is not configured.');
  },
  async deleteJob() {
    throw new Error('Cron provider is not configured.');
  },
  async ping() {
    return blockedResponse();
  },
  async triggerTest(url, deps = {}) {
    if (!deps || typeof deps.runDirectTest !== 'function') {
      throw new Error('Direct ping dependency missing.');
    }
    return deps.runDirectTest(url);
  },
};
