const test = require('node:test');
const assert = require('node:assert/strict');
const { __test } = require('../bot');

test('config DB warmup retry logic halts after configured cap', () => {
  const cap = Number(__test.configDbMaxRetriesPerBoot);
  __test.setConfigDbFailureStreakForTests(Math.max(0, cap - 1));
  assert.equal(__test.shouldHaltConfigDbRetries(), false);
  __test.setConfigDbFailureStreakForTests(cap);
  assert.equal(__test.shouldHaltConfigDbRetries(), cap > 0);
  __test.setConfigDbFailureStreakForTests(0);
});
