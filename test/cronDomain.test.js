const assert = require('node:assert/strict');
const test = require('node:test');

const { buildCronFingerprint, buildCronDisplayName } = require('../src/cronDomain');

test('buildCronFingerprint is stable for equivalent input', () => {
  const left = buildCronFingerprint({
    projectId: 'p1',
    type: 'keepalive',
    schedule: '*/5 * * * *',
    targetUrl: 'https://example.com/keep-alive/p1',
    provider: 'cronjob_org',
    headers: { Authorization: 'a', 'X-Tenant': 't' },
    body: { ping: true },
  });
  const right = buildCronFingerprint({
    projectId: 'p1',
    type: 'keepalive',
    schedule: '*/5 * * * *',
    targetUrl: 'https://example.com/keep-alive/p1',
    provider: 'cronjob_org',
    headers: { 'X-Tenant': 't', Authorization: 'a' },
    body: { ping: true },
  });
  assert.equal(left, right);
});

test('buildCronDisplayName includes project and type and schedule', () => {
  const name = buildCronDisplayName({ projectName: 'MyProj', projectId: 'p1', type: 'keepalive', schedule: 'every 5 minutes' });
  assert.match(name, /^\[MyProj\] • keepalive • every 5 minutes • /);
});
