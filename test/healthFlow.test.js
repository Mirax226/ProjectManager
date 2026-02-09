const assert = require('node:assert/strict');
const test = require('node:test');

const { handleGlobalCommand, __test } = require('../bot');

test('global /health command sends endpoint guidance with button', async () => {
  const calls = { sendMessage: [], editMessageReplyMarkup: [], reply: [] };
  __test.setBotApiForTests({
    sendMessage: async (...args) => {
      calls.sendMessage.push(args);
      return { chat: { id: 1 }, message_id: 100 };
    },
    editMessageReplyMarkup: async (...args) => {
      calls.editMessageReplyMarkup.push(args);
    },
  });

  const ctx = {
    from: { id: 10 },
    chat: { id: 1 },
    message: { chat: { id: 1 }, message_id: 1 },
    reply: async (...args) => {
      calls.reply.push(args);
      return { chat: { id: 1 }, message_id: 100 };
    },
  };

  const handled = await handleGlobalCommand(ctx, '/health', '');
  assert.equal(handled, true);
  const textPayload = calls.reply[0]?.[0] || calls.sendMessage[0]?.[1] || '';
  assert.match(textPayload, /This is a web endpoint\. Open:/);
  assert.equal(calls.editMessageReplyMarkup.length, 1);
});
