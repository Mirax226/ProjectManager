const { createDeployEvent, updateDeployEvent, getDeploySettings } = require('./deployStore');
const { getProvider } = require('./providers');
const { ingestLog } = require('../logsHubStore');

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

async function verifyHealth(baseUrl, settings, deps = {}) {
  const requestUrl = deps.requestUrl;
  if (!baseUrl || settings.healthMode === 'off' || !requestUrl) {
    return { ok: true, skipped: true, statusCode: null, latencyMs: null, path: null };
  }
  const paths = settings.healthPath
    ? [settings.healthPath]
    : settings.healthMode === 'auto'
      ? ['/healthz', '/health']
      : ['/healthz'];
  const deadline = Date.now() + settings.healthTimeoutSec * 1000;
  while (Date.now() < deadline) {
    for (const path of paths) {
      const url = `${String(baseUrl).replace(/\/$/, '')}${path.startsWith('/') ? path : `/${path}`}`;
      const start = Date.now();
      try {
        const response = await requestUrl('GET', url);
        const latency = Date.now() - start;
        if (response.status >= 200 && response.status <= 399) {
          return { ok: true, statusCode: response.status, latencyMs: latency, path, url };
        }
      } catch (_error) {}
    }
    await sleep(settings.healthPollIntervalSec * 1000);
  }
  return { ok: false, statusCode: null, latencyMs: null, path: paths[0] || null };
}

async function runDeploy({ project, trigger = 'manual', timeline, deps = {} }) {
  const settings = await getDeploySettings(project.id);
  const provider = getProvider(settings.provider);
  const deploy = await createDeployEvent({
    projectId: project.id,
    provider: settings.provider,
    providerServiceId: settings.renderServiceId,
    trigger,
    message_short: 'Deploy requested',
    healthUrl: project.renderServiceUrl || null,
    healthPath: settings.healthPath,
  });
  timeline?.append?.({
    scope: 'project',
    projectId: project.id,
    type: 'DEPLOY_REQUESTED',
    severity: 'info',
    title: 'Deploy requested',
    detailsMasked: `ref=${deploy.refId}`,
    refId: deploy.refId,
  });

  try {
    const started = await provider.triggerDeploy({ ...project, ...settings }, deps);
    await updateDeployEvent(deploy.id, { status: 'deploying', providerDeployId: started.providerDeployId, meta_json: started.meta || null, message_short: 'Deploy started' });
    const health = await verifyHealth(project.renderServiceUrl, settings, deps);
    const finishedAt = new Date().toISOString();
    if (health.ok) {
      const live = await updateDeployEvent(deploy.id, {
        status: 'live',
        finishedAt,
        healthStatusCode: health.statusCode,
        healthLatencyMs: health.latencyMs,
        healthPath: health.path,
        message_short: 'Deploy live',
      });
      timeline?.append?.({ scope: 'project', projectId: project.id, type: 'DEPLOY_LIVE', severity: 'info', title: 'Health verified', refId: deploy.refId, detailsMasked: `status=${health.statusCode || 'skipped'}` });
      return { ok: true, deploy: live };
    }
    const failed = await updateDeployEvent(deploy.id, {
      status: 'failed',
      finishedAt,
      error_short: 'Health verification timeout',
      error_full: 'Health endpoint did not return success within timeout.',
      healthPath: health.path,
    });
    await ingestLog({
      projectId: project.id,
      level: 'error',
      category: 'DEPLOY_ERROR',
      message_short: 'Deploy health verification failed',
      message_full: `Deploy ${deploy.refId} failed health verification`,
      meta_json: { refId: deploy.refId, projectId: project.id },
    });
    timeline?.append?.({ scope: 'project', projectId: project.id, type: 'DEPLOY_FAILED', severity: 'error', title: 'Health failed', refId: deploy.refId, detailsMasked: 'health verification timeout' });
    return { ok: false, deploy: failed };
  } catch (error) {
    const failed = await updateDeployEvent(deploy.id, {
      status: 'failed',
      finishedAt: new Date().toISOString(),
      error_short: String(error?.message || 'Deploy failed').slice(0, 300),
      error_full: String(error?.stack || error?.message || 'Deploy failed').slice(0, 4000),
    });
    await ingestLog({
      projectId: project.id,
      level: 'error',
      category: 'DEPLOY_ERROR',
      message_short: 'Deploy trigger failed',
      message_full: String(error?.message || 'Deploy trigger failed'),
      meta_json: { refId: deploy.refId, projectId: project.id },
    });
    return { ok: false, deploy: failed };
  }
}

module.exports = { runDeploy, verifyHealth };
