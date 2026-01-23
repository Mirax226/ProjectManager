const fetch = require('node-fetch');

function createTelegramRateLimiter(minIntervalMs = 350) {
  let lastCallAt = 0;
  let queue = Promise.resolve();

  return async function schedule(fn) {
    queue = queue.then(async () => {
      const now = Date.now();
      const waitMs = Math.max(0, minIntervalMs - (now - lastCallAt));
      if (waitMs) {
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
      lastCallAt = Date.now();
      return fn();
    });
    return queue;
  };
}

const scheduleTelegramCall = createTelegramRateLimiter();

async function callTelegram(token, method, payload) {
  const url = `https://api.telegram.org/bot${token}/${method}`;
  const response = await scheduleTelegramCall(() =>
    fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {}),
    }),
  );
  const json = await response.json();
  if (!json.ok) {
    const error = new Error(json.description || 'Telegram API error');
    error.response = json;
    throw error;
  }
  return json.result;
}

async function setWebhook(token, url, dropPending = false) {
  return callTelegram(token, 'setWebhook', {
    url,
    drop_pending_updates: dropPending,
  });
}

async function getWebhookInfo(token) {
  return callTelegram(token, 'getWebhookInfo', {});
}

async function sendMessage(token, chatId, text) {
  return callTelegram(token, 'sendMessage', {
    chat_id: chatId,
    text,
  });
}

function sanitizeTelegramText(text) {
  if (!text) return '';
  return String(text).replace(/[\u0000-\u001F\u007F]/g, ' ');
}

async function sendSafeMessage(token, chatId, text) {
  return sendMessage(token, chatId, sanitizeTelegramText(text));
}

module.exports = {
  setWebhook,
  getWebhookInfo,
  sendMessage,
  sendSafeMessage,
  sanitizeTelegramText,
};
