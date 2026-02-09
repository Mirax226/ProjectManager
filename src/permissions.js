const GUEST_ALLOWED = new Set(['projects', 'logs', 'ops', 'timeline', 'health', 'runbooks', 'help']);

function resolveRole(userId, settings = {}) {
  const id = String(userId || '');
  if (!id) return 'guest';
  if (String(settings.ownerId || '') === id) return 'owner';
  if (Array.isArray(settings.adminIds) && settings.adminIds.map(String).includes(id)) return 'admin';
  if (Array.isArray(settings.guestIds) && settings.guestIds.map(String).includes(id)) return 'guest';
  return 'guest';
}

function canAccess(role, action) {
  if (role === 'owner' || role === 'admin') return true;
  return GUEST_ALLOWED.has(String(action || '').toLowerCase());
}

module.exports = { resolveRole, canAccess };
