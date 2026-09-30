const { json } = require('./handler');
const { dispatchTelegramUpdate } = require('../telegramApplication');
const { boundedJson } = require('./requestBody');

function validUpdate(update) {
  if (!update || !Number.isSafeInteger(update.update_id) || update.update_id < 0) return false;
  const message = update.message || update.callback_query?.message;
  const sender = update.callback_query?.from || message?.from;
  return Boolean(message && Number.isSafeInteger(message.chat?.id) && Number.isSafeInteger(sender?.id) && (typeof message.text === 'string' || (typeof update.callback_query?.data === 'string' && typeof update.callback_query?.id === 'string')));
}

async function telegramCall(token, method, payload, fetchImpl = fetch) {
  try {
    const response = await fetchImpl(`https://api.telegram.org/bot${token}/${method}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload), signal: AbortSignal.timeout(10000) });
    const data = await response.json();
    if (!response.ok || data.ok !== true) throw new Error('telegram_delivery_failed');
    return data.result;
  } catch (_) { throw new Error('telegram_delivery_failed'); }
}

async function telegramWebhook(request, env, store, fetchImpl) {
  if (!env.TELEGRAM_WEBHOOK_SECRET || !env.TELEGRAM_BOT_TOKEN) return json({ ok: false, error: 'telegram_not_configured' }, 503);
  if (request.headers.get('X-Telegram-Bot-Api-Secret-Token') !== env.TELEGRAM_WEBHOOK_SECRET) return json({ ok: false, error: 'unauthorized' }, 401);
  let update;
  try { update = await boundedJson(request); } catch (error) { return json({ ok: false, error: error.message === 'body_limit' ? 'body_limit' : 'invalid_json' }, error.message === 'body_limit' ? 413 : 400); }
  if (!validUpdate(update)) return json({ ok: false, error: 'invalid_update' }, 400);
  const now = Date.now(); const owner = crypto.randomUUID();
  await store.db.prepare('INSERT OR IGNORE INTO cp_telegram_updates (update_id,status,lease_until,owner,created_at) VALUES (?,\'PENDING\',0,\'\',?)').bind(update.update_id, now).run();
  const claim = await store.db.prepare('UPDATE cp_telegram_updates SET status=\'PROCESSING\',owner=?,lease_until=? WHERE update_id=? AND status!=\'DONE\' AND lease_until<=?').bind(owner, now + 60000, update.update_id, now).run();
  if (!claim.meta.changes) {
    const row = await store.db.prepare('SELECT status FROM cp_telegram_updates WHERE update_id=?').bind(update.update_id).first();
    return json({ ok: row?.status === 'DONE', duplicate: true }, row?.status === 'DONE' ? 200 : 503);
  }
  try {
    const output = await dispatchTelegramUpdate(update, { store, env });
    if (output.authorized) {
      await telegramCall(env.TELEGRAM_BOT_TOKEN, 'sendMessage', { chat_id: output.chatId, text: output.text, ...(output.replyMarkup ? { reply_markup: output.replyMarkup } : {}) }, fetchImpl);
      if (output.callbackId) await telegramCall(env.TELEGRAM_BOT_TOKEN, 'answerCallbackQuery', { callback_query_id: output.callbackId }, fetchImpl);
    }
    await store.db.prepare('UPDATE cp_telegram_updates SET status=\'DONE\',lease_until=0 WHERE update_id=? AND owner=?').bind(update.update_id, owner).run();
    return json({ ok: true });
  } catch (_) {
    await store.db.prepare('UPDATE cp_telegram_updates SET status=\'PENDING\',lease_until=0 WHERE update_id=? AND owner=?').bind(update.update_id, owner).run();
    return json({ ok: false, error: 'update_processing_failed' }, 503);
  }
}
module.exports = { telegramWebhook, boundedJson, validUpdate, telegramCall };
