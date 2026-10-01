const test = require('node:test');
const assert = require('node:assert/strict');
const { configureWebhook, webhookUrl, inputDiagnostics } = require('../tools/telegram-webhook');
const origin = 'https://projectmanager-control-plane.amirhoseinsalmani194728095.workers.dev';
const expected = `${origin}/telegram/webhook`;
const fixture = { mode: 'set', origin, token: '123456:private-fixture-token', secret: 'private_fixture_secret' };
const reject = (stage, classification, variable) => (error) => { assert.equal(error.diagnostic.stage, stage); if (classification) assert.equal(error.diagnostic.classification, classification); if (variable) assert.equal(error.diagnostic.variable, variable); return true; };
for (const [key, variable] of [['origin', 'PJ_WORKER_URL'], ['token', 'TELEGRAM_BOT_TOKEN'], ['secret', 'TELEGRAM_WEBHOOK_SECRET']]) {
  test(`missing ${variable} fails before HTTP without exposing values`, async () => {
    let calls = 0;
    await assert.rejects(configureWebhook({ ...fixture, [key]: undefined, fetchImpl: async () => { calls++; } }), reject('INPUT_PRECHECK_FAILED', 'MISSING_REQUIRED_INPUT', variable));
    assert.equal(calls, 0);
  });
}
test('malformed and unrelated-path Worker origins fail before mutation', async () => {
  for (const value of ['invalid', 'http://pj.example', `${origin}/health`, `${origin}/telegram/webhook`, 'https://user:password@pj.example', `${origin}?private=query`]) {
    await assert.rejects(configureWebhook({ ...fixture, origin: value }), reject('INPUT_PRECHECK_FAILED', 'INVALID_WORKER_ORIGIN', 'PJ_WORKER_URL'));
  }
  assert.equal(webhookUrl(origin), expected); assert.equal(webhookUrl(`${origin}/`), expected);
});
test('secret format validates only allowed characters and 1..256 length', async () => {
  for (const secret of ['bad space', 'a'.repeat(257), 'bad\nsecret']) await assert.rejects(configureWebhook({ ...fixture, secret }), reject('INPUT_PRECHECK_FAILED', 'INVALID_WEBHOOK_SECRET_FORMAT'));
  assert.equal(inputDiagnostics({ ...fixture, secret: 'A_z-9'.repeat(50) }).webhookSecretFormatValid, true);
});
test('set accepted then verified uses same secret and exact endpoint with safe output', async () => {
  const calls = [];
  const result = await configureWebhook({ ...fixture, fetchImpl: async (url, options) => {
    const body = JSON.parse(options.body); calls.push(body);
    return Response.json({ ok: true, result: url.endsWith('setWebhook') ? true : { url: expected, allowed_updates: ['message', 'callback_query'], pending_update_count: 2 } });
  } });
  assert.equal(calls[0].url, expected); assert.equal(calls[0].secret_token, fixture.secret);
  assert.deepEqual(calls[0].allowed_updates, ['message', 'callback_query']);
  assert.equal(result.stage, 'SUCCESS'); assert.equal(result.setAccepted, true); assert.equal(result.pendingUpdateCount, 2);
  assert.deepEqual(result.stages, ['SETWEBHOOK_ACCEPTED', 'SUCCESS']);
  const encoded = JSON.stringify(result); assert.equal(encoded.includes(fixture.token), false); assert.equal(encoded.includes(fixture.secret), false);
});
test('Telegram rejection exposes code/status and fixed classification, never raw description', async () => {
  let count = 0;
  await assert.rejects(configureWebhook({ ...fixture, fetchImpl: async () => { count++; return Response.json({ ok: false, error_code: 400, description: `Bad Request: secret token ${fixture.secret} ${fixture.token}` }, { status: 400 }); } }), (error) => {
    assert.equal(error.diagnostic.stage, 'SETWEBHOOK_TELEGRAM_REJECTED'); assert.equal(error.diagnostic.classification, 'WEBHOOK_SECRET_REJECTED');
    assert.equal(error.diagnostic.httpStatus, 400); assert.equal(error.diagnostic.telegramOk, false); assert.equal(error.diagnostic.telegramErrorCode, 400); assert.equal(error.diagnostic.setAccepted, false);
    const encoded = JSON.stringify(error.diagnostic); assert.equal(encoded.includes(fixture.token), false); assert.equal(encoded.includes(fixture.secret), false); assert.equal(encoded.includes('Bad Request'), false); return true;
  });
  assert.equal(count, 1);
});
test('accepted mutation followed by mismatched verification is explicitly distinguished', async () => {
  await assert.rejects(configureWebhook({ ...fixture, fetchImpl: async (url) => Response.json({ ok: true, result: url.endsWith('setWebhook') ? true : { url: 'https://other.invalid/private-fragment' } }) }), (error) => {
    assert.equal(error.diagnostic.stage, 'POST_SET_VERIFY_MISMATCH'); assert.equal(error.diagnostic.setAccepted, true); assert.equal(JSON.stringify(error.diagnostic).includes('private-fragment'), false); return true;
  });
});
test('transport and non-JSON HTTP failures do not echo token-bearing exceptions', async () => {
  await assert.rejects(configureWebhook({ ...fixture, fetchImpl: async () => { throw new Error(`https://api.telegram.org/bot${fixture.token}/setWebhook`); } }), (error) => { assert.equal(error.diagnostic.stage, 'SETWEBHOOK_HTTP_FAILED'); assert.equal(JSON.stringify(error.diagnostic).includes(fixture.token), false); return true; });
  await assert.rejects(configureWebhook({ ...fixture, fetchImpl: async () => new Response(fixture.token, { status: 502 }) }), reject('SETWEBHOOK_HTTP_FAILED', 'NON_JSON_RESPONSE'));
});
test('accepted mutation with verification transport failure retains acceptance evidence', async () => {
  await assert.rejects(configureWebhook({ ...fixture, fetchImpl: async (url) => { if (url.endsWith('setWebhook')) return Response.json({ ok: true, result: true }); throw new Error(fixture.secret); } }), (error) => { assert.equal(error.diagnostic.stage, 'POST_SET_VERIFY_FAILED'); assert.equal(error.diagnostic.setAccepted, true); return true; });
});
test('info mode works without webhook secret and does not call setWebhook', async () => {
  const result = await configureWebhook({ ...fixture, mode: 'info', secret: undefined, fetchImpl: async (url) => { assert.equal(url.endsWith('getWebhookInfo'), true); return Response.json({ ok: true, result: { url: '' } }); } });
  assert.equal(result.setAccepted, false); assert.equal(result.webhookMatchesExpected, false); assert.equal(result.inputs[2].present, false);
});
