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
    if (!response.ok || data.ok !== true) {
      const error = new Error('telegram_delivery_failed');
      error.telegramCode = data.error_code || response.status;
      error.telegramDescription = String(data.description || '');
      throw error;
    }
    return data.result;
  } catch (error) {
    if (error.message === 'telegram_delivery_failed') throw error;
    throw new Error('telegram_delivery_failed', { cause: error });
  }
}

function editOutcome(error) {
  const description = String(error.telegramDescription || '').toLowerCase();
  if (error.telegramCode !== 400) return 'error';
  if (description.includes('message is not modified')) return 'unchanged';
  if (['message to edit not found', "message can't be edited", 'message cannot be edited', 'message is inaccessible', 'message to edit is unavailable'].some((part) => description.includes(part))) return 'fallback';
  return 'error';
}

async function deliverTelegramOutput(token, output, fetchImpl) {
  const payload = { chat_id: output.chatId, text: output.text, ...(output.replyMarkup ? { reply_markup: output.replyMarkup } : {}) };
  if (output.callbackId && Number.isSafeInteger(output.messageId)) {
    try {
      await telegramCall(token, 'editMessageText', { ...payload, message_id: output.messageId }, fetchImpl);
      return;
    } catch (error) {
      const outcome = editOutcome(error);
      if (outcome === 'unchanged') return;
      if (outcome !== 'fallback') throw error;
    }
  }
  await telegramCall(token, 'sendMessage', payload, fetchImpl);
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
    if (update.callback_query?.id) await telegramCall(env.TELEGRAM_BOT_TOKEN, 'answerCallbackQuery', { callback_query_id: update.callback_query.id }, fetchImpl);
    const output = await dispatchTelegramUpdate(update, { store, env });
    if (output.authorized) await deliverTelegramOutput(env.TELEGRAM_BOT_TOKEN, output, fetchImpl);
    await store.db.prepare('UPDATE cp_telegram_updates SET status=\'DONE\',lease_until=0 WHERE update_id=? AND owner=?').bind(update.update_id, owner).run();
    return json({ ok: true });
  } catch (error) {
    console.error('[telegram] update delivery failed', { code: error.telegramCode || 'transport', updateId: update.update_id });
    await store.db.prepare('UPDATE cp_telegram_updates SET status=\'PENDING\',lease_until=0 WHERE update_id=? AND owner=?').bind(update.update_id, owner).run();
    return json({ ok: false, error: 'update_processing_failed' }, 503);
  }
}
module.exports = { telegramWebhook, boundedJson, validUpdate, telegramCall, deliverTelegramOutput, editOutcome };
