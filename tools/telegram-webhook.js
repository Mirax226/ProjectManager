// Tokens are read only from environment; never accept them as CLI arguments.
const { telegramCall } = require('../src/controlPlane/telegramWebhook');
function webhookUrl(input) {
  const url = new URL(input);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || !['', '/'].includes(url.pathname)) throw new Error('Use an explicit HTTPS Worker origin without credentials or query');
  url.pathname = '/telegram/webhook'; return url.href;
}
async function configureWebhook({ mode, origin, token, secret, fetchImpl }) {
  if (!token) throw new Error('TELEGRAM_BOT_TOKEN is required');
  const expected = webhookUrl(origin);
  if (mode === 'set') {
    if (!/^[A-Za-z0-9_-]{1,256}$/.test(secret || '')) throw new Error('TELEGRAM_WEBHOOK_SECRET must use Telegram allowed characters');
    await telegramCall(token, 'setWebhook', { url: expected, secret_token: secret, allowed_updates: ['message', 'callback_query'], drop_pending_updates: false, max_connections: 1 }, fetchImpl);
  } else if (mode !== 'info') throw new Error('Use --set or --info');
  const info = await telegramCall(token, 'getWebhookInfo', {}, fetchImpl);
  return { ok: true, webhookMatchesExpected: info.url === expected, pendingUpdateCount: Number(info.pending_update_count) || 0, lastErrorPresent: Boolean(info.last_error_date), customCertificate: Boolean(info.has_custom_certificate) };
}
if (require.main === module) configureWebhook({ mode: process.argv.includes('--set') ? 'set' : 'info', origin: process.env.PJ_WORKER_URL, token: process.env.TELEGRAM_BOT_TOKEN, secret: process.env.TELEGRAM_WEBHOOK_SECRET }).then((status) => { console.log(JSON.stringify(status)); if (!status.webhookMatchesExpected) process.exitCode = 1; }).catch(() => { console.error('Webhook setup/verification failed. Check required environment names and Worker origin; credentials were not printed.'); process.exitCode = 1; });
module.exports = { configureWebhook, webhookUrl };
