const test = require('node:test');
const assert = require('node:assert/strict');
const { buildProjectSnapshot, calculateDrift } = require('../src/driftDetector');

test('drift detector reports repo/env changes', () => {
  const baseline = buildProjectSnapshot({ id: 'p1', repoUrl: 'a', baseBranch: 'main' }, { envKeys: ['A'] });
  const current = buildProjectSnapshot({ id: 'p1', repoUrl: 'b', baseBranch: 'dev' }, { envKeys: ['A', 'B'] });
  const drift = calculateDrift(baseline, current);
  assert.equal(drift.changed, true);
  assert.ok(drift.summary.some((line) => line.includes('Repo changed')));
  assert.ok(drift.summary.some((line) => line.includes('Env keys added')));
});
