const crypto = require('crypto');

let cachedKey = null;
let cachedError = null;

const MASTER_KEY_ERROR_MESSAGE =
  'Env Vault unavailable: invalid master key. Provide 64-char hex or 32-byte base64.';

function parseMasterKey() {
  const raw = process.env.ENV_VAULT_MASTER_KEY;
  if (!raw) {
    return { key: null, error: 'missing' };
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
    return { key: null, error: 'invalid' };
  }

  return { key: buffer, error: null };
}

function getMasterKeyStatus() {
  if (cachedKey || cachedError) {
    return { ok: Boolean(cachedKey), error: cachedError };
  }
  const { key, error } = parseMasterKey();
  cachedKey = key;
  cachedError = error;
  return { ok: Boolean(key), error };
}

function getMasterKey() {
  const status = getMasterKeyStatus();
  if (!status.ok) {
    return null;
  }
  return cachedKey;
}

function encryptSecret(plainText) {
  const key = getMasterKey();
  if (!key) {
    throw new Error(MASTER_KEY_ERROR_MESSAGE);
  }
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
  if (!key) {
    throw new Error(MASTER_KEY_ERROR_MESSAGE);
  }
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
  getMasterKeyStatus,
  MASTER_KEY_ERROR_MESSAGE,
};
