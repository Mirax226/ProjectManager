function bearer(request) {
  const value = request?.headers?.get ? request.headers.get('authorization') : request?.headers?.authorization;
  const match = String(value || '').match(/^Bearer\s+([^\s]+)$/i);
  return match ? match[1] : null;
}

function equalSecret(left, right) {
  if (!left || !right) return false;
  const a = Buffer.from(String(left));
  const b = Buffer.from(String(right));
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i += 1) diff |= a[i] ^ b[i];
  return diff === 0;
}

function createAuthenticator({ adminToken = '', runnerTokens = {}, projectTokens = {} } = {}) {
  const runnerEntries = Object.entries(runnerTokens).map(([id, value]) => ({ id, token: String(value && typeof value === 'object' ? value.token || '' : value || ''), project: value && typeof value === 'object' ? String(value.project || '') : '' }));
  const projectEntries = Object.entries(projectTokens).map(([project, token]) => ({ project, token: String(token) }));
  function authenticate(request, required = 'any') {
    const token = bearer(request);
    if (!token) return { ok: false, status: 401, error: 'unauthorized' };
    if ((required === 'admin' || required === 'any') && equalSecret(token, adminToken)) return { ok: true, role: 'admin' };
    if (required !== 'admin' && required !== 'project') {
      const matches = runnerEntries.filter((entry) => equalSecret(token, entry.token));
      // A credential must identify exactly one runner and an explicit project.
      if (matches.length === 1 && matches[0].project) return { ok: true, role: 'runner', runnerId: matches[0].id, project: matches[0].project };
    }
    if (required !== 'admin' && required !== 'runner') {
      const project = projectEntries.find((entry) => equalSecret(token, entry.token));
      if (project) return { ok: true, role: 'project', project: project.project };
    }
    return { ok: false, status: 401, error: 'unauthorized' };
  }
  return { authenticate };
}

module.exports = { bearer, equalSecret, createAuthenticator };
