const assert = require('node:assert/strict');
const test = require('node:test');

const {
  pushSnapshot,
  getStack,
  setStack,
  clearStack,
  resetForTests,
} = require('../navigationStackStore');

test('navigation stack push/pop behavior', async () => {
  resetForTests();
  await clearStack(1, 101);
  await pushSnapshot(1, 101, { routeId: 'route:database', timestamp: Date.now() - 1000 });
  await pushSnapshot(1, 101, { routeId: 'cb:dbmenu:open:demo', timestamp: Date.now() });
  const stack = await getStack(1, 101);
  assert.equal(stack.length, 2);
  assert.equal(stack[0].routeId, 'route:database');
  assert.equal(stack[1].routeId, 'cb:dbmenu:open:demo');

  await setStack(1, 101, stack.slice(0, 1));
  const updated = await getStack(1, 101);
  assert.equal(updated.length, 1);
  assert.equal(updated[0].routeId, 'route:database');
});

test('navigation stack ttl cleanup removes expired snapshots', async () => {
  resetForTests();
  await clearStack(2, 101);
  await pushSnapshot(2, 101, { routeId: 'route:database', timestamp: Date.now() - 10_000 }, { ttlMs: 1000 });
  await pushSnapshot(2, 101, { routeId: 'cb:dbmenu:list', timestamp: Date.now() }, { ttlMs: 1000 });
  const stack = await getStack(2, 101, { ttlMs: 1000 });
  assert.equal(stack.length, 1);
  assert.equal(stack[0].routeId, 'cb:dbmenu:list');
});
