const assert = require('node:assert/strict');
const crypto = require('crypto');
const test = require('node:test');

const { __test } = require('../bot');

function buildInitData(botToken, userId = 1) {
  const user = JSON.stringify({ id: userId, first_name: 'Admin' });
  const authDate = String(Math.floor(Date.now() / 1000));
  const pairs = [
    ['auth_date', authDate],
    ['query_id', 'AAEAAAE'],
    ['user', user],
  ];
  const dataCheckString = pairs
    .slice()
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join('\n');
  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(botToken).digest();
  const hash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
  const params = new URLSearchParams();
  pairs.forEach(([k, v]) => params.set(k, v));
  params.set('hash', hash);
  return params.toString();
}

function createRes() {
  return {
    statusCode: 0,
    headers: null,
    body: '',
    writeHead(code, headers) {
      this.statusCode = code;
      this.headers = headers;
    },
    end(body) {
      this.body = body || '';
    },
  };
}

test('mini api rejects unauthenticated health request', async () => {
  const req = { method: 'GET', headers: {}, url: '/api/mini/health' };
  const res = createRes();
  const url = new URL('http://localhost/api/mini/health');

  const handled = await __test.handleMiniApiRequest(req, res, url);
  assert.equal(handled, true);
  assert.equal(res.statusCode, 401);
});

test('mini api health and logs smoke', async () => {
  const initData = buildInitData('TEST', 1);

  const reqHealth = { method: 'GET', headers: { 'x-telegram-init-data': initData }, url: '/api/mini/health' };
  const resHealth = createRes();
  const urlHealth = new URL('http://localhost/api/mini/health');
  await __test.handleMiniApiRequest(reqHealth, resHealth, urlHealth);
  assert.equal(resHealth.statusCode, 200);
  const healthPayload = JSON.parse(resHealth.body);
  assert.equal(healthPayload.ok, true);

  const reqLogs = { method: 'GET', headers: { 'x-telegram-init-data': initData }, url: '/api/mini/logs' };
  const resLogs = createRes();
  const urlLogs = new URL('http://localhost/api/mini/logs');
  await __test.handleMiniApiRequest(reqLogs, resLogs, urlLogs);
  assert.equal(resLogs.statusCode, 200);
  const logsPayload = JSON.parse(resLogs.body);
  assert.equal(logsPayload.ok, true);
  assert.ok(Array.isArray(logsPayload.rows));
});
