const crypto = require('crypto');

function stableJson(value) {
  if (!value || typeof value !== 'object') return JSON.stringify(value ?? null);
  if (Array.isArray(value)) return `[${value.map((item) => stableJson(item)).join(',')}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`;
}

function buildRequestSignature({ headers, body }) {
  return crypto.createHash('sha256').update(`${stableJson(headers || {})}|${stableJson(body || {})}`).digest('hex');
}

function buildCronFingerprint({ projectId, type, schedule, targetUrl, provider, headers, body }) {
  const source = [
    String(projectId || '').trim().toLowerCase(),
    String(type || '').trim().toLowerCase(),
    String(schedule || '').trim().toLowerCase(),
    String(targetUrl || '').trim().toLowerCase(),
    String(provider || 'cronjob_org').trim().toLowerCase(),
    buildRequestSignature({ headers, body }),
  ].join('|');
  return crypto.createHash('sha256').update(source).digest('hex');
}

function slugify(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 24) || 'job';
}

function buildCronDisplayName({ projectName, type, schedule, projectId }) {
  const short = `[${projectName || projectId || 'Project'}] • ${type || 'job'} • ${schedule || 'custom'}`;
  const stable = slugify(`${projectId || 'global'}-${type || 'job'}-${schedule || 'custom'}`);
  return `${short} • ${stable}`;
}

module.exports = { buildCronFingerprint, buildRequestSignature, buildCronDisplayName };
