const test = require('node:test');
const assert = require('node:assert/strict');

const configStore = require('../configStore');
const {
  createCodexTask,
  listCodexTasks,
  markCodexTaskDone,
  clearDoneCodexTasks,
} = require('../src/codexTasksStore');

test('codex task dedupe keeps one pending by fingerprint+project', async () => {
  await configStore.saveJson('codexTasks', []);
  const one = await createCodexTask({
    sourceType: 'manual',
    projectId: 'p1',
    title: 'T1',
    body: 'same body',
  });
  assert.equal(one.created, true);
  const two = await createCodexTask({
    sourceType: 'manual',
    projectId: 'p1',
    title: 'T1b',
    body: 'same body',
  });
  assert.equal(two.created, false);
  const pending = await listCodexTasks({ status: 'pending' });
  assert.equal(pending.length, 1);
});

test('codex task done transition and clear done', async () => {
  await configStore.saveJson('codexTasks', []);
  const created = await createCodexTask({
    sourceType: 'manual',
    projectId: 'p2',
    title: 'T2',
    body: 'body 2',
  });
  const done = await markCodexTaskDone(created.task.id);
  assert.equal(done.status, 'done');
  assert.ok(done.doneAt);
  const removed = await clearDoneCodexTasks();
  assert.equal(removed, 1);
  const tasks = await listCodexTasks();
  assert.equal(tasks.length, 0);
});
