const crypto = require('crypto');

function createRing(limit) {
  return { limit: Math.max(1, Number(limit) || 2000), items: [], next: 0, size: 0 };
}

function ringPush(ring, item) {
  if (ring.size < ring.limit) {
    ring.items.push(item);
    ring.size += 1;
    ring.next = ring.size % ring.limit;
    return;
  }
  ring.items[ring.next] = item;
  ring.next = (ring.next + 1) % ring.limit;
}

function ringListNewest(ring, offset = 0, pageSize = 20) {
  const total = ring.size;
  const safeOffset = Math.max(0, Number(offset) || 0);
  const limit = Math.max(1, Math.min(100, Number(pageSize) || 20));
  const out = [];
  for (let i = safeOffset; i < total && out.length < limit; i += 1) {
    const idx = (ring.next - 1 - i + ring.limit) % ring.limit;
    out.push(ring.items[idx]);
  }
  return { items: out, total, offset: safeOffset, pageSize: limit };
}

function normalizeSeverity(value) {
  const v = String(value || 'info').toLowerCase();
  return ['info', 'warn', 'error', 'critical'].includes(v) ? v : 'info';
}

function createOpsTimelineStore(options = {}) {
  const globalLimit = Math.max(1, Number(options.globalLimit) || 2000);
  const projectLimit = Math.max(1, Number(options.projectLimit) || 2000);
  const maxProjects = Math.max(10, Number(options.maxProjects) || 100);
  const globalRing = createRing(globalLimit);
  const projectRings = new Map();

  function ensureProjectRing(projectId) {
    if (!projectRings.has(projectId)) {
      if (projectRings.size >= maxProjects) {
        const oldest = projectRings.keys().next().value;
        projectRings.delete(oldest);
      }
      projectRings.set(projectId, createRing(projectLimit));
    }
    return projectRings.get(projectId);
  }

  function append(input = {}) {
    const scope = input.scope === 'project' ? 'project' : 'global';
    const projectId = scope === 'project' ? String(input.projectId || '').trim() || null : null;
    const event = {
      id: input.id || crypto.randomUUID(),
      ts: input.ts || new Date().toISOString(),
      scope,
      projectId,
      type: String(input.type || 'general').slice(0, 80),
      severity: normalizeSeverity(input.severity),
      title: String(input.title || '').slice(0, 200),
      detailsMasked: String(input.detailsMasked || '').slice(0, 2000),
      refId: input.refId || null,
      tags: Array.isArray(input.tags) ? input.tags.slice(0, 20) : [],
      correlationId: input.correlationId || null,
    };
    ringPush(globalRing, event);
    if (scope === 'project' && projectId) {
      ringPush(ensureProjectRing(projectId), event);
    }
    return event;
  }

  function query(filters = {}) {
    const scope = filters.scope === 'project' ? 'project' : 'global';
    const severity = filters.severity ? normalizeSeverity(filters.severity) : null;
    const type = filters.type ? String(filters.type).toLowerCase() : null;
    const projectId = filters.projectId ? String(filters.projectId) : null;
    const source = scope === 'project' && projectId ? (projectRings.get(projectId) || createRing(1)) : globalRing;
    const page = ringListNewest(source, filters.offset, filters.pageSize);
    page.items = page.items.filter((item) => {
      if (severity && item.severity !== severity) return false;
      if (type && item.type.toLowerCase() !== type) return false;
      return true;
    });
    return page;
  }

  return { append, query };
}

module.exports = { createOpsTimelineStore };
