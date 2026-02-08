const assert = require('node:assert/strict');
const test = require('node:test');

const { resolveConflictLastWriteWins, batchRows, syncPair } = require('../src/dbSyncEngine');

test('resolveConflictLastWriteWins picks newer updated_at', () => {
  const winner = resolveConflictLastWriteWins(
    { id: 1, updated_at: '2024-01-01T00:00:00Z', value: 'old' },
    { id: 1, updated_at: '2024-01-02T00:00:00Z', value: 'new' },
  );
  assert.equal(winner.value, 'new');
});

test('batchRows batches with fixed size', () => {
  const batches = batchRows([1, 2, 3, 4, 5], 2);
  assert.deepEqual(batches, [[1, 2], [3, 4], [5]]);
});

test('syncPair returns upserts for missing and stale rows', async () => {
  const upserts = await syncPair({
    sourceRows: [
      { id: 1, updated_at: '2024-01-02T00:00:00Z', value: 'a2' },
      { id: 2, updated_at: '2024-01-01T00:00:00Z', value: 'b1' },
    ],
    targetRows: [{ id: 1, updated_at: '2024-01-01T00:00:00Z', value: 'a1' }],
  });

  assert.equal(upserts.length, 2);
});
