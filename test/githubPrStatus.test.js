const test = require('node:test');
const assert = require('node:assert/strict');

const { parsePrStatus, createPrWatcher } = require('../src/githubPrStatus');

test('parsePrStatus summarizes checks and mergeability', () => {
  const parsed = parsePrStatus({
    pr: { state: 'open', head: { sha: 'abc' }, updated_at: '2024-01-01T00:00:00.000Z' },
    mergeable: true,
    checkRuns: [
      { conclusion: 'success' },
      { status: 'in_progress' },
      { conclusion: 'failure' },
    ],
    requiredReviewsMet: false,
  });

  assert.deepEqual(parsed.checks, { total: 3, success: 1, failure: 1, pending: 1 });
  assert.equal(parsed.mergeable, true);
  assert.equal(parsed.headSha, 'abc');
  assert.equal(parsed.overall, 'failure');
});

test('watcher only emits on transitions and pauses in safe mode', async () => {
  const statuses = [
    { state: 'open', mergeable: null, overall: 'warning', checks: { failure: 0, pending: 1 } },
    { state: 'open', mergeable: true, overall: 'success', checks: { failure: 0, pending: 0 } },
    { state: 'open', mergeable: true, overall: 'success', checks: { failure: 0, pending: 0 } },
  ];
  let idx = 0;
  const transitions = [];
  const watcher = createPrWatcher({
    fetchStatus: async () => statuses[idx++],
    onTransition: (payload) => transitions.push(payload),
    pollMs: 60_000,
    safeModeGetter: () => false,
  });

  const id = watcher.start('acme/repo', 9);
  await watcher.refresh(id);
  await watcher.refresh(id);
  await watcher.refresh(id);

  assert.equal(transitions.length, 1);

  const pausedWatcher = createPrWatcher({
    fetchStatus: async () => statuses[0],
    safeModeGetter: () => true,
    pollMs: 60_000,
  });
  const pausedId = pausedWatcher.start('acme/repo', 10);
  const paused = await pausedWatcher.refresh(pausedId);
  assert.equal(paused.paused, true);
});
