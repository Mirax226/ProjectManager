const crypto = require('crypto');

const ALGO = 'aes-256-gcm';

function getKmsKey() {
  const key = process.env.PM_KMS_KEY || '';
  if (!key) return null;
  return crypto.createHash('sha256').update(key).digest();
}

function encryptSecret(value) {
  const key = getKmsKey();
  if (!key) {
    const error = new Error('PM_KMS_KEY missing');
    error.code = 'PM_KMS_KEY_MISSING';
    throw error;
  }
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(ALGO, key, iv);
  const encrypted = Buffer.concat([cipher.update(String(value || ''), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64')}.${tag.toString('base64')}.${encrypted.toString('base64')}`;
}

function decryptSecret(payload) {
  const key = getKmsKey();
  if (!key) {
    const error = new Error('PM_KMS_KEY missing');
    error.code = 'PM_KMS_KEY_MISSING';
    throw error;
  }
  const [ivB64, tagB64, ciphertextB64] = String(payload || '').split('.');
  const decipher = crypto.createDecipheriv(ALGO, key, Buffer.from(ivB64, 'base64'));
  decipher.setAuthTag(Buffer.from(tagB64, 'base64'));
  return Buffer.concat([
    decipher.update(Buffer.from(ciphertextB64, 'base64')),
    decipher.final(),
  ]).toString('utf8');
}

module.exports = {
  encryptSecret,
  decryptSecret,
  getKmsKey,
};
