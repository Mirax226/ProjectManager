// Tokens are read only from environment; never accept them as CLI arguments.
function inputDiagnostics({ origin, token, secret }) {
  return { inputs: Object.entries({ PJ_WORKER_URL: origin, TELEGRAM_BOT_TOKEN: token, TELEGRAM_WEBHOOK_SECRET: secret }).map(([name, value]) => ({ name, present: value != null, nonEmpty: typeof value === 'string' && value.length > 0 })), webhookSecretFormatValid: typeof secret === 'string' && /^[A-Za-z0-9_-]{1,256}$/.test(secret) };
}
function failure(details) { const error = new Error(details.stage); error.diagnostic = { ...details, ok: false }; return error; }
// Untrusted API text is mapped to fixed labels; no description fragments are returned.
function classification(data, status) {
  const text = typeof data?.description === 'string' ? data.description.toLowerCase() : '';
  if (/secret.*token/.test(text)) return 'WEBHOOK_SECRET_REJECTED';
  if (/resolve|ip address|dns/.test(text)) return 'WEBHOOK_DNS_REJECTED';
  if (/certificate|ssl|tls/.test(text)) return 'WEBHOOK_TLS_REJECTED';
  if (/url|https|port/.test(text)) return 'WEBHOOK_URL_REJECTED';
  if (status === 401 || data?.error_code === 401) return 'TELEGRAM_AUTH_REJECTED';
  if (status === 429 || data?.error_code === 429) return 'TELEGRAM_RATE_LIMITED';
  return 'TELEGRAM_REQUEST_REJECTED';
}
async function setupCall(method, payload, { token, fetchImpl, preflight, setAccepted }) {
  const isSet = method === 'setWebhook';
  const base = { ...preflight, operation: method, setAccepted, requestSent: false };
  const httpStage = isSet ? 'SETWEBHOOK_HTTP_FAILED' : (setAccepted ? 'POST_SET_VERIFY_FAILED' : 'GETWEBHOOKINFO_HTTP_FAILED');
  let response;
  try { response = await fetchImpl(`https://api.telegram.org/bot${token}/${method}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload), signal: AbortSignal.timeout(10000) }); }
  catch (_) { throw failure({ ...base, requestSent: null, requestAttempted: true, stage: httpStage, classification: 'TRANSPORT_OR_REQUEST_FAILURE', httpStatus: null, telegramOk: null, telegramErrorCode: null }); }
  const httpStatus = Number.isInteger(response.status) ? response.status : null;
  let data;
  try { data = await response.json(); }
  catch (_) { throw failure({ ...base, requestSent: true, stage: httpStage, classification: 'NON_JSON_RESPONSE', httpStatus, telegramOk: null, telegramErrorCode: null }); }
  const meta = { ...base, requestSent: true, httpStatus, telegramOk: data?.ok === true, telegramErrorCode: Number.isSafeInteger(data?.error_code) ? data.error_code : null };
  if (!response.ok || data?.ok !== true) throw failure({ ...meta, stage: isSet && data?.ok === false ? 'SETWEBHOOK_TELEGRAM_REJECTED' : httpStage, classification: classification(data, httpStatus) });
  if ((isSet && data.result !== true) || (!isSet && (!data.result || typeof data.result !== 'object' || Array.isArray(data.result)))) throw failure({ ...meta, stage: httpStage, classification: 'INVALID_API_RESULT' });
  return { result: data.result, meta };
}
function webhookUrl(input) {
  const url = new URL(input);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || !['', '/'].includes(url.pathname)) throw new Error('Use an explicit HTTPS Worker origin without credentials or query');
  url.pathname = '/telegram/webhook'; return url.href;
}
async function configureWebhook({ mode, origin, token, secret, fetchImpl = globalThis.fetch }) {
  const preflight = inputDiagnostics({ origin, token, secret });
  const invalid = (classification, variable) => failure({ ...preflight, stage: 'INPUT_PRECHECK_FAILED', classification, ...(variable ? { variable } : {}), requestSent: false, setAccepted: false });
  for (const [name, value] of [['PJ_WORKER_URL', origin], ['TELEGRAM_BOT_TOKEN', token], ...(mode === 'set' ? [['TELEGRAM_WEBHOOK_SECRET', secret]] : [])]) {
    if (typeof value !== 'string' || !value.length) throw invalid('MISSING_REQUIRED_INPUT', name);
  }
  let expected;
  try { expected = webhookUrl(origin); } catch (_) { throw invalid('INVALID_WORKER_ORIGIN', 'PJ_WORKER_URL'); }
  if (!['set', 'info'].includes(mode)) throw invalid('INVALID_MODE');
  const context = { token, fetchImpl, preflight, setAccepted: false };
  let setHttpStatus = null;
  if (mode === 'set') {
    if (!preflight.webhookSecretFormatValid) throw invalid('INVALID_WEBHOOK_SECRET_FORMAT', 'TELEGRAM_WEBHOOK_SECRET');
    const accepted = await setupCall('setWebhook', { url: expected, secret_token: secret, allowed_updates: ['message', 'callback_query'], drop_pending_updates: false, max_connections: 1 }, context);
    context.setAccepted = true; setHttpStatus = accepted.meta.httpStatus;
  }
  const verified = await setupCall('getWebhookInfo', {}, context); const info = verified.result;
  const output = { ...preflight, ok: true, stage: 'SUCCESS', setAccepted: context.setAccepted, stages: context.setAccepted ? ['SETWEBHOOK_ACCEPTED', 'SUCCESS'] : ['SUCCESS'], httpStatus: verified.meta.httpStatus, telegramOk: true, telegramErrorCode: null, ...(context.setAccepted ? { setHttpStatus } : {}), webhookMatchesExpected: info.url === expected, pendingUpdateCount: Number.isSafeInteger(info.pending_update_count) && info.pending_update_count >= 0 ? info.pending_update_count : 0, lastErrorPresent: Boolean(info.last_error_date), customCertificate: Boolean(info.has_custom_certificate), allowedUpdates: Array.isArray(info.allowed_updates) ? info.allowed_updates.filter((value) => ['message', 'callback_query'].includes(value)) : [] };
  if (context.setAccepted && !output.webhookMatchesExpected) throw failure({ ...output, stage: 'POST_SET_VERIFY_MISMATCH', stages: ['SETWEBHOOK_ACCEPTED', 'POST_SET_VERIFY_MISMATCH'], classification: 'EXPECTED_WEBHOOK_NOT_OBSERVED' });
  return output;
}
if (require.main === module) configureWebhook({ mode: process.argv.includes('--set') ? 'set' : 'info', origin: process.env.PJ_WORKER_URL, token: process.env.TELEGRAM_BOT_TOKEN, secret: process.env.TELEGRAM_WEBHOOK_SECRET }).then((status) => { console.log(JSON.stringify(status)); if (!status.webhookMatchesExpected) process.exitCode = 1; }).catch((error) => { console.error(JSON.stringify(error.diagnostic || { ok: false, stage: 'UNEXPECTED_TOOL_FAILURE', classification: 'INTERNAL_ERROR' })); process.exitCode = 1; });
module.exports = { configureWebhook, webhookUrl, inputDiagnostics };
