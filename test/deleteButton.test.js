const assert = require('node:assert/strict');
const test = require('node:test');

const { __test } = require('../bot');

function createCtx(userId = 10) {
  const calls = { deleteMessage: [], answer: [] };
  const telegram = {
    deleteMessage: async (...args) => calls.deleteMessage.push(args),
  };
  return {
    ctx: {
      from: { id: userId },
      callbackQuery: { id: 'cb1' },
      telegram,
      answerCallbackQuery: async (payload) => calls.answer.push(payload || {}),
    },
    calls,
  };
}

test('delete button returns invalid target for malformed payload', async () => {
  const { ctx, calls } = createCtx(11);
  const result = await __test.handleDeleteMessageCallback(ctx, 'msg:delete:pending');
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'invalid_target');
  assert.equal(calls.deleteMessage.length, 0);
});

test('delete button deletes target message', async () => {
  const { ctx, calls } = createCtx(10);
  const result = await __test.handleDeleteMessageCallback(ctx, 'msg:delete:1:100');
  assert.equal(result.ok, true);
  assert.equal(result.mode, 'deleted');
  assert.deepEqual(calls.deleteMessage[0], [1, 100]);
});

test('delete button treats not found as success', async () => {
  const { ctx, calls } = createCtx(11);
  ctx.telegram.deleteMessage = async () => {
    throw new Error('Bad Request: message to delete not found');
  };
  const result = await __test.handleDeleteMessageCallback(ctx, 'msg:delete:1:100');
  assert.equal(result.ok, true);
  assert.equal(result.mode, 'already_deleted');
  assert.equal(calls.answer.length, 1);
});
