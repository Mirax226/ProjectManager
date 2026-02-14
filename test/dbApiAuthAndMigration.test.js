const assert = require('node:assert/strict');
const crypto = require('crypto');
const test = require('node:test');
const { Readable } = require('node:stream');

const { __test } = require('../bot');

function buildInitData(botToken, userId = 1) {
  const user = JSON.stringify({ id: userId, first_name: 'Admin' });
  const authDate = String(Math.floor(Date.now() / 1000));
  const pairs = [['auth_date', authDate], ['query_id', 'AAEAAAE'], ['user', user]];
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
    body: '',
    writeHead(code) { this.statusCode = code; },
    end(body) { this.body = body || ''; },
  };
}

test('db api requires auth', async () => {
  const req = { method: 'GET', headers: {}, url: '/api/db/connections' };
  const res = createRes();
  const url = new URL('http://localhost/api/db/connections');
  const handled = await __test.handleMiniApiRequest(req, res, url);
  assert.equal(handled, true);
  assert.equal(res.statusCode, 401);
});

test('db api validates request and migration job updates', async () => {
  const initData = buildInitData('TEST', 1);
  const req = { method: 'GET', headers: { 'x-telegram-init-data': initData }, url: '/api/db/connections' };
  const res = createRes();
  const url = new URL('http://localhost/api/db/connections');
  await __test.handleMiniApiRequest(req, res, url);
  assert.equal(res.statusCode, 400);

  const reqStart = Readable.from([Buffer.from('{}')]);
  reqStart.method = 'POST';
  reqStart.headers = { 'x-telegram-init-data': initData };
  reqStart.url = '/api/mini/db/migrate/start';
  const resStart = createRes();
  const urlStart = new URL('http://localhost/api/mini/db/migrate/start');
  await __test.handleMiniApiRequest(reqStart, resStart, urlStart);
  assert.equal(resStart.statusCode, 202);
  const jobId = JSON.parse(resStart.body).jobId;

  const reqStatus = { method: 'GET', headers: { 'x-telegram-init-data': initData }, url: `/api/mini/db/migrate/status?jobId=${jobId}` };
  const resStatus = createRes();
  const urlStatus = new URL(`http://localhost/api/mini/db/migrate/status?jobId=${jobId}`);
  await __test.handleMiniApiRequest(reqStatus, resStatus, urlStatus);
  assert.equal(resStatus.statusCode, 200);
  const payload = JSON.parse(resStatus.body);
  assert.equal(payload.ok, true);
  assert.equal(['running', 'done', 'failed', 'aborted'].includes(payload.job.status), true);
});
