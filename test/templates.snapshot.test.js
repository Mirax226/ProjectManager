const test = require('node:test');
const assert = require('node:assert/strict');
const { listTemplates } = require('../src/projectTemplates');

test('built-in templates snapshot', () => {
  const snapshot = JSON.stringify(listTemplates(), null, 2);
  assert.match(snapshot, /Node Telegram Bot/);
  assert.match(snapshot, /Cron-only job/);
});
