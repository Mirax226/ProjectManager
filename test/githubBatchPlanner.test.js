const test = require('node:test');
const assert = require('node:assert/strict');

const { createGithubBatchQueueStore } = require('../src/githubBatchQueue');
const { planBatch } = require('../src/githubBatchPlanner');

test('batch queue enforces max item limit', () => {
  const store = createGithubBatchQueueStore();
  for (let i = 0; i < 10; i += 1) {
    store.addItem('p1', { refId: `R-${i}`, sourceType: 'routine', title: 'x' });
  }
  assert.throws(() => {
    store.addItem('p1', { refId: 'R-11', sourceType: 'routine', title: 'x' });
  }, /batch_limit_exceeded/);
});

test('planner detects overlap conflicts and deterministic ordering', () => {
  const plan = planBatch([
    {
      refId: 'A',
      sourceType: 'routine',
      ruleId: 'HEALTH_MISCONFIG',
      patchOperations: [{ file: 'app.js', startLine: 10, endLine: 20, linesChanged: 5 }],
    },
    {
      refId: 'B',
      sourceType: 'repo_inspection',
      ruleId: 'REPO_INSPECTION_MISSING_FEATURES',
      patchOperations: [{ file: 'app.js', startLine: 18, endLine: 22, linesChanged: 4 }],
    },
  ]);

  assert.equal(plan.orderedItems[0].refId, 'B');
  assert.equal(plan.conflictCount, 1);
  assert.equal(plan.conflicts[0].type, 'line_overlap');
  assert.deepEqual(plan.files, ['app.js']);
});
