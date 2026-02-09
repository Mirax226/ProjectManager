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

test('verifyTelegramWebAppInitData validates valid payload', () => {
  const initData = buildInitData('TEST', 1);
  const result = __test.verifyTelegramWebAppInitData(initData);
  assert.equal(result.ok, true);
  assert.equal(result.userId, '1');
});

test('verifyTelegramWebAppInitData rejects invalid hash', () => {
  const initData = new URLSearchParams(buildInitData('TEST', 1));
  initData.set('hash', 'deadbeef');
  const result = __test.verifyTelegramWebAppInitData(initData.toString());
  assert.equal(result.ok, false);
});
