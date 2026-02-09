const { getConfigDbPool } = require('./configDb');
const { encryptSecret, decryptSecret } = require('./envVaultCrypto');

let tableReady = false;
const memoryTokens = new Map();

async function ensureTable(db) {
  if (!db || tableReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS user_github_tokens (
      user_id TEXT PRIMARY KEY,
      token_enc TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  tableReady = true;
}

async function getDb() {
  const db = await getConfigDbPool();
  if (db) await ensureTable(db);
  return db;
}

async function setDefaultGithubToken(userId, token) {
  const key = String(userId);
  const tokenEnc = encryptSecret(token);
  const db = await getDb();
  if (!db) {
    memoryTokens.set(key, tokenEnc);
    return true;
  }
  await db.query(
    `
      INSERT INTO user_github_tokens (user_id, token_enc)
      VALUES ($1, $2)
      ON CONFLICT (user_id)
      DO UPDATE SET token_enc = EXCLUDED.token_enc, updated_at = NOW()
    `,
    [key, tokenEnc],
  );
  return true;
}

async function getDefaultGithubToken(userId) {
  const key = String(userId);
  const db = await getDb();
  if (!db) {
    const value = memoryTokens.get(key);
    return value ? decryptSecret(value) : null;
  }
  const { rows } = await db.query(
    'SELECT token_enc FROM user_github_tokens WHERE user_id = $1 LIMIT 1',
    [key],
  );
  if (!rows.length || !rows[0].token_enc) return null;
  return decryptSecret(rows[0].token_enc);
}

module.exports = {
  setDefaultGithubToken,
  getDefaultGithubToken,
};
