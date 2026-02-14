const test = require('node:test');
const assert = require('node:assert/strict');

function reloadEnvVaultStore() {
  delete require.cache[require.resolve('../envVaultCrypto')];
  delete require.cache[require.resolve('../envVaultStore')];
  return require('../envVaultStore');
}

test('env vault upsert/read-back for DATABASE_URL', async () => {
  process.env.ENV_VAULT_MASTER_KEY = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
  const {
    ensureDefaultEnvVarSet,
    upsertEnvVar,
    getEnvVarValue,
  } = reloadEnvVaultStore();

  const projectId = `proj-${Date.now()}`;
  const envSetId = await ensureDefaultEnvVarSet(projectId);
  const dsn = 'postgresql://user:pa%23ss@localhost:5432/app_main';

  await upsertEnvVar(projectId, 'DATABASE_URL', dsn, envSetId);
  const readBack = await getEnvVarValue(projectId, 'DATABASE_URL', envSetId);

  assert.equal(readBack, dsn);
});
