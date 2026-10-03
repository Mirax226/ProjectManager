const { createAuthenticator } = require('./auth');
const { createControlPlaneHandler, json } = require('./handler');
const { D1ControlPlaneStore } = require('./d1Store');
const { telegramWebhook } = require('./telegramWebhook');
function parseMap(value) { try { const parsed = JSON.parse(value || '{}'); return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}; } catch (_) { return {}; } }
function getStore(env) { if (!env.CONTROL_PLANE_DB) throw new Error('d1_required'); return new D1ControlPlaneStore(env.CONTROL_PLANE_DB, { zjEnabled: env.PJ_ZJ_ENABLED === 'true' }); }
async function fetch(request, env = {}) {
  const url = new URL(request.url);
  if (request.method === 'GET' && ['/health', '/healthz'].includes(url.pathname)) {
    try { await env.CONTROL_PLANE_DB.prepare('SELECT 1 FROM cp_projects LIMIT 1').all(); return json({ ok: true, runtime: 'cloudflare', service: 'projectmanager', d1: 'available', version: 'PJ-ZJ-001' }); }
    catch (_) { return json({ ok: false, runtime: 'cloudflare', service: 'projectmanager', d1: 'unavailable', version: 'PJ-ZJ-001' }, 503); }
  }
  try {
    const store = getStore(env); await store.ready;
    if (url.pathname === '/telegram/webhook') return request.method === 'POST' ? await telegramWebhook(request, env, store) : json({ ok: false, error: 'method_not_allowed' }, 405);
    if (url.pathname.startsWith('/api/control-plane/')) { url.pathname = url.pathname.replace('/api/control-plane/', '/api/v1/'); request = new Request(url, request); }
    const handler = createControlPlaneHandler({ store, authenticator: createAuthenticator({ adminToken: env.PG_CONTROL_PLANE_ADMIN_TOKEN || '', runnerTokens: parseMap(env.PG_RUNNER_TOKENS_JSON), projectTokens: parseMap(env.PG_PROJECT_TOKENS_JSON) }) });
    return await handler(request);
  } catch (_) { return json({ ok: false, error: 'operational_storage_unavailable' }, 503); }
}
module.exports = { fetch, getStore };
