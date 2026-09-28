const { ControlPlaneStore } = require('./store');
const { createAuthenticator } = require('./auth');
const { createControlPlaneHandler } = require('./handler');
const { D1ControlPlaneStore } = require('./d1Store');

let memoryStore;
let d1Store;
function parseMap(value) { try { const parsed = JSON.parse(value || '{}'); return parsed && typeof parsed === 'object' ? parsed : {}; } catch (_) { return {}; } }
function getStore(env) {
  // The store is intentionally process-local until a D1 binding is configured. The SQL schema
  // in migrations/20260924_control_plane.sql is the persistence boundary for deployment.
  if (env.CONTROL_PLANE_DB) { if (!d1Store) d1Store = new D1ControlPlaneStore(env.CONTROL_PLANE_DB); return d1Store; }
  if (!memoryStore) memoryStore = new ControlPlaneStore();
  return memoryStore;
}

function handlerFor(env) {
  return createControlPlaneHandler({
    store: getStore(env),
    tokens: {
      adminToken: env.PG_CONTROL_PLANE_ADMIN_TOKEN || '',
      runnerTokens: parseMap(env.PG_RUNNER_TOKENS_JSON),
      projectTokens: parseMap(env.PG_PROJECT_TOKENS_JSON),
    },
    authenticator: createAuthenticator({
      adminToken: env.PG_CONTROL_PLANE_ADMIN_TOKEN || '',
      runnerTokens: parseMap(env.PG_RUNNER_TOKENS_JSON),
      projectTokens: parseMap(env.PG_PROJECT_TOKENS_JSON),
    }),
  });
}

async function fetch(request, env) { return handlerFor(env || {}).call(null, request); }
module.exports = { fetch, getStore };
