const crypto = require('crypto');

let cachedKey = null;

function parseMasterKey() {
  const raw = process.env.ENV_VAULT_MASTER_KEY;
  if (!raw) {
    throw new Error('ENV_VAULT_MASTER_KEY is required for Env Vault');
  }

  let buffer = null;
  const trimmed = raw.trim();
  if (/^[0-9a-fA-F]{64}$/.test(trimmed)) {
    buffer = Buffer.from(trimmed, 'hex');
  } else {
    try {
      buffer = Buffer.from(trimmed, 'base64');
    } catch (error) {
      buffer = null;
    }
  }

  if (!buffer || buffer.length !== 32) {
    throw new Error('ENV_VAULT_MASTER_KEY must decode to 32 bytes (base64 or hex).');
  }

  return buffer;
}

function getMasterKey() {
  if (!cachedKey) {
    cachedKey = parseMasterKey();
  }
  return cachedKey;
}

function encryptSecret(plainText) {
  const key = getMasterKey();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(String(plainText), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return JSON.stringify({
    v: 1,
    iv: iv.toString('base64'),
    tag: tag.toString('base64'),
    data: encrypted.toString('base64'),
  });
}

function decryptSecret(payload) {
  if (!payload) return '';
  const key = getMasterKey();
  const parsed = typeof payload === 'string' ? JSON.parse(payload) : payload;
  if (!parsed?.iv || !parsed?.tag || !parsed?.data) {
    throw new Error('Encrypted payload is invalid.');
  }
  const iv = Buffer.from(parsed.iv, 'base64');
  const tag = Buffer.from(parsed.tag, 'base64');
  const data = Buffer.from(parsed.data, 'base64');
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  const decrypted = Buffer.concat([decipher.update(data), decipher.final()]);
  return decrypted.toString('utf8');
}

module.exports = {
  encryptSecret,
  decryptSecret,
};
