const { boundedJson } = require('./requestBody');
const { createAuthenticator } = require('./auth');

function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data), { status, headers: { 'content-type': 'application/json; charset=utf-8', ...headers } });
}

function corsHeaders(origin) {
  const allowed = globalThis.process?.env?.PG_CONTROL_PLANE_CORS_ORIGIN || '';
  return { 'access-control-allow-origin': allowed && origin === allowed ? origin : 'null', 'access-control-allow-headers': 'authorization,content-type,x-request-id', 'access-control-allow-methods': 'GET,POST,OPTIONS', vary: 'Origin' };
}

function createControlPlaneHandler({ store, authenticator, tokens, logger = console } = {}) {
  if (!store) throw new Error('store is required');
  const auth = authenticator || createAuthenticator(tokens || {});
  return async function handle(request) {
    if (store.ready) await store.ready;
    const url = new URL(request.url);
    const headers = corsHeaders(request.headers.get('origin'));
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers });
    if (request.method === 'GET' && url.pathname === '/healthz') return json({ ok: true, service: 'pg-control-plane', timestamp: new Date().toISOString() }, 200, headers);
    if (!url.pathname.startsWith('/api/v1/')) return json({ ok: false, error: 'not_found' }, 404, headers);
    const required = url.pathname === '/api/v1/ops/events' ? 'any' : (url.pathname === '/api/v1/runners/heartbeat' || url.pathname === '/api/v1/jobs/claim' || url.pathname.endsWith('/result') ? 'runner' : 'any');
    const identity = auth.authenticate(request, required);
    if (!identity.ok) return json({ ok: false, error: identity.error }, identity.status, headers);
    if (identity.role === 'runner' && !identity.project) return json({ ok: false, error: 'runner_project_required' }, 403, headers);
    if (request.method === 'GET' && url.pathname === '/api/v1/status') {
      const status = await store.status();
      if (identity.role === 'project') status.projects = status.projects.filter((project) => project.id === identity.project);
      if (identity.role === 'runner') {
        status.projects = status.projects.filter((project) => project.id === (identity.project || project.id));
        status.runners = status.runners.filter((runner) => runner.runnerId === identity.runnerId);
        status.latestEvents = status.latestEvents.filter((event) => event.project === identity.project);
        status.openAlerts = status.openAlerts.filter((alert) => alert.project === identity.project);
      }
      return json({ ok: true, ...status }, 200, headers);
    }
    if (request.method === 'GET' && url.pathname === '/api/v1/projects') return json({ ok: true, projects: (identity.role === 'admin' ? await store.listProjects() : (await store.listProjects()).filter((p) => p.id === (identity.project || p.id))) }, 200, headers);
    if (request.method === 'GET' && url.pathname === '/api/v1/jobs') return json({ ok: true, jobs: await store.listJobs(identity.role === 'admin' ? url.searchParams.get('projectId') : identity.project) }, 200, headers);
    if (request.method === 'GET' && /^\/api\/v1\/jobs\/[^/]+$/.test(url.pathname)) {
      const job = await store.getJob(url.pathname.split('/').pop(), identity.role === 'admin' ? null : identity.project);
      return job ? json({ ok: true, job }, 200, headers) : json({ ok: false, error: 'not_found' }, 404, headers);
    }
    let payload = {};
    if (request.method === 'POST') {
      try { payload = await boundedJson(request); } catch (error) { return json({ ok: false, error: error.message === 'body_limit' ? 'body_limit' : 'invalid_json' }, error.message === 'body_limit' ? 413 : 400, headers); }
    }
    if (url.pathname === '/api/v1/config') {
      if (identity.role !== 'admin') return json({ ok:false,error:'admin_required' },403,headers);
      if (request.method === 'GET') return json({ ok:true,config:store.operationalConfig },200,headers);
      if (request.method === 'POST') { const saved=await store.saveOperationalConfig(payload.config,'control-plane-admin',payload.operationKey); return json(saved,saved.ok?200:400,headers); }
    }
    if (request.method === 'POST' && url.pathname === '/api/v1/ops/events') {
      if (identity.role === 'project' && String(payload.project || payload.projectId).toLowerCase() !== identity.project) return json({ ok: false, error: 'project_scope_denied' }, 403, headers);
      if (identity.role === 'runner' && identity.project && String(payload.project || payload.projectId).toLowerCase() !== identity.project) return json({ ok: false, error: 'project_scope_denied' }, 403, headers);
      const result = await store.ingestEvent(payload); return json(result, result.status || 200, headers);
    }
    if (request.method === 'POST' && url.pathname === '/api/v1/runners/heartbeat') {
      if (identity.role === 'runner' && payload.runnerId && payload.runnerId !== identity.runnerId) return json({ ok: false, error: 'runner_scope_denied' }, 403, headers);
      if (identity.role === 'runner' && payload.projectId && String(payload.projectId).toLowerCase() !== identity.project) return json({ ok: false, error: 'project_scope_denied' }, 403, headers);
      const result = await store.heartbeat({ ...payload, runnerId: identity.role === 'runner' ? identity.runnerId : payload.runnerId, projectId: identity.role === 'runner' ? identity.project : payload.projectId }); return json(result, result.status || 200, headers);
    }
    if (request.method === 'POST' && url.pathname === '/api/v1/jobs') {
      if (identity.role === 'project' && String(payload.projectId || payload.project).toLowerCase() !== identity.project) return json({ ok: false, error: 'project_scope_denied' }, 403, headers);
      if (identity.role === 'runner' && identity.project && String(payload.projectId || payload.project).toLowerCase() !== identity.project) return json({ ok: false, error: 'project_scope_denied' }, 403, headers);
      const result = await store.createJob(payload, identity.role === 'admin' ? 'admin' : 'api'); return json(result, result.status || 200, headers);
    }
    if (request.method === 'POST' && url.pathname === '/api/v1/jobs/claim') {
      const projectId = identity.role === 'runner' ? identity.project : (payload.projectId || url.searchParams.get('projectId'));
      const job = await store.claimJob(identity.runnerId, projectId); return json({ ok: true, job }, 200, headers);
    }
    if (request.method === 'POST' && /^\/api\/v1\/jobs\/[^/]+\/result$/.test(url.pathname)) {
      const id = url.pathname.split('/')[4];
      if (identity.role === 'runner') {
        const job = await store.getJob(id);
        if (!job || job.projectId !== identity.project) return json({ ok: false, error: 'project_scope_denied' }, 403, headers);
      }
      if (identity.role === 'runner' && !Number.isInteger(payload.attemptCount)) return json({ ok: false, error: 'attempt_count_required' }, 400, headers);
      const result = await store.resultJob(id, identity.runnerId, payload); return json(result, result.status || 200, headers);
    }
    if (request.method === 'POST' && url.pathname === '/api/v1/alerts/ack') {
      if (identity.role !== 'admin') return json({ ok: false, error: 'admin_required' }, 403, headers);
      const alert = await store.acknowledgeAlert(payload.id); return alert ? json({ ok: true, alert }, 200, headers) : json({ ok: false, error: 'not_found' }, 404, headers);
    }
    logger.debug?.('[control-plane] route not found', { method: request.method, path: url.pathname });
    return json({ ok: false, error: 'not_found' }, 404, headers);
  };
}

module.exports = { createControlPlaneHandler, json };
