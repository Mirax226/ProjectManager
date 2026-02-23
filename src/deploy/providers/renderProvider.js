async function triggerDeploy(project, deps = {}) {
  const requestUrl = deps.requestUrl;
  if (!project?.deployHookUrl || !requestUrl) {
    return { providerDeployId: null, startedAt: new Date().toISOString(), meta: { mode: 'noop', reason: 'hook_or_request_missing' } };
  }
  const response = await requestUrl('POST', project.deployHookUrl);
  return {
    providerDeployId: null,
    startedAt: new Date().toISOString(),
    meta: {
      mode: 'hook',
      status: response.status,
      body: String(response.body || '').slice(0, 200),
    },
  };
}

async function getDeployStatus(_project, _providerDeployId, _deps = {}) {
  return { status: 'unknown', details: { reason: 'render_api_not_configured' } };
}

async function getRecentDeploys(_project, _limit, _deps = {}) {
  return [];
}

async function rollback(project, _toDeployId, deps = {}) {
  if (!project?.deployHookUrl || !deps.requestUrl) {
    return { ok: false, mode: 'manual', instruction: 'Render API key required for rollback automation.' };
  }
  return { ok: false, mode: 'manual', instruction: 'Use Render dashboard rollback for this service.' };
}

function validateConfig(project = {}) {
  const missing = [];
  if (!project.deployHookUrl) missing.push('deployHookUrl');
  return {
    ok: missing.length === 0,
    missing,
    suggestions: missing.length ? ['Provide Render deploy hook URL in project deploy settings.'] : [],
  };
}

module.exports = { triggerDeploy, getDeployStatus, getRecentDeploys, rollback, validateConfig };
