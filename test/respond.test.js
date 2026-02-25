const assert = require('node:assert/strict');
const test = require('node:test');

const { respond, __test } = require('../bot');

test('respond edits message for callback queries', async () => {
  let edited = false;
  let replied = false;
  const ctx = {
    callbackQuery: {
      message: {
        message_id: 123,
        chat: { id: 456 },
      },
    },
    editMessageText: async (text) => {
      edited = text === 'hello';
    },
    reply: async () => {
      replied = true;
    },
  };

  await respond(ctx, 'hello', {});

  assert.equal(edited, true);
  assert.equal(replied, false);
});

test('respond replies in message context', async () => {
  let replied = false;
  const ctx = {
    reply: async (text) => {
      replied = text === 'hi';
    },
  };

  await respond(ctx, 'hi', {});

  assert.equal(replied, true);
});

test('ephemeral guard phrases are forced even with inline keyboard unless overridden', () => {
  const inline = { reply_markup: { inline_keyboard: [[{ text: 'x', callback_data: 'x' }]] } };
  assert.equal(__test.shouldUseEphemeralForRespond('Completed (100%)', inline), true);
  assert.equal(__test.shouldUseEphemeralForRespond('Failed at step build', inline), true);
  assert.equal(__test.shouldUseEphemeralForRespond('Reason: network', inline), true);
  assert.equal(__test.shouldUseEphemeralForRespond('Completed (100%)', { ...inline, forcePersistent: true }), false);
});
