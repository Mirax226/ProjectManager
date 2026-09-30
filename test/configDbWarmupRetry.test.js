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

test('Config DB transient retries use exponential backoff bounded to one minute plus jitter', () => {
  assert.ok(__test.computeConfigDbBackoff(1) >= 500 && __test.computeConfigDbBackoff(1) <= 750);
  assert.ok(__test.computeConfigDbBackoff(4) >= 4000 && __test.computeConfigDbBackoff(4) <= 4250);
  assert.ok(__test.computeConfigDbBackoff(100) >= 60000 && __test.computeConfigDbBackoff(100) <= 60250);
});
