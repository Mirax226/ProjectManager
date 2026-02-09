const test = require('node:test');
const assert = require('node:assert/strict');

const { __test } = require('../bot');

test('default token logic returns no token message on invalid token', async () => {
  const result = await __test.resolveReposFromGithubTokenInput('1', 'bad', {
    fetchRepos: async () => {
      const err = new Error('invalid');
      err.code = 'INVALID_TOKEN';
      throw err;
    },
    isDefault: true,
  });
  assert.equal(result.ok, false);
  assert.match(result.errorMessage, /invalid\/expired/i);
});

test('default token logic resolves repos on valid token', async () => {
  const result = await __test.resolveReposFromGithubTokenInput('1', 'good', {
    fetchRepos: async () => [{ full_name: 'a/b' }],
  });
  assert.equal(result.ok, true);
  assert.equal(result.repos.length, 1);
});
