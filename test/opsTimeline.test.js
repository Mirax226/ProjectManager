const test = require('node:test');
const assert = require('node:assert/strict');
const { createOpsTimelineStore } = require('../src/opsTimeline');

test('timeline retention stays bounded and newest first', () => {
  const store = createOpsTimelineStore({ globalLimit: 3, projectLimit: 2 });
  store.append({ type: 'a', title: '1' });
  store.append({ type: 'b', title: '2' });
  store.append({ type: 'c', title: '3' });
  store.append({ type: 'd', title: '4' });
  const page = store.query({ pageSize: 10 });
  assert.equal(page.total, 3);
  assert.deepEqual(page.items.map((i) => i.title), ['4', '3', '2']);
});
