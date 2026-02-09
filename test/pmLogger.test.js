const test = require('node:test');
const assert = require('node:assert/strict');

const { createPmLogger } = require('../src/pmLogger');

test('pm logger uses PATH_APPLIER_URL and PM_TOKEN fallbacks', async () => {
  const calls = [];
  const fetchStub = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200 };
  };

  const logger = createPmLogger({
    env: {
      PATH_APPLIER_URL: 'https://pm.example.com',
      PM_TOKEN: 'fallback-token',
      PM_TEST_ENABLED: 'true',
      PM_TEST_TOKEN: 'test-token',
    },
    fetch: fetchStub,
  });

  const result = await logger.info('hello', { secretToken: 'abc' });
  assert.equal(result.ok, true);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'https://pm.example.com/api/logs');
  const body = JSON.parse(calls[0].options.body);
  assert.equal(body.meta.secretToken, '[MASKED]');
});

test('pm logger retries ingest path with /api/pm/logs on 404', async () => {
  const urls = [];
  const fetchStub = async (url) => {
    urls.push(url);
    if (url.endsWith('/api/logs')) {
      return { ok: false, status: 404 };
    }
    return { ok: true, status: 200 };
  };

  const logger = createPmLogger({
    env: {
      PM_URL: 'https://pm.example.com',
      PM_INGEST_TOKEN: 'ingest-token',
    },
    fetch: fetchStub,
  });

  const result = await logger.error('boom');
  assert.equal(result.ok, true);
  assert.deepEqual(urls, ['https://pm.example.com/api/logs', 'https://pm.example.com/api/pm/logs']);
});
