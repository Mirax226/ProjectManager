const crypto = require('crypto');

function parsePostgresDsn(dsn) {
  const input = String(dsn || '').trim();
  const normalizedInput = input.startsWith('postgres://') ? `postgresql://${input.slice('postgres://'.length)}` : input;
  const parsed = new URL(normalizedInput);
  return {
    protocol: parsed.protocol,
    user: decodeURIComponent(parsed.username || ''),
    password: decodeURIComponent(parsed.password || ''),
    host: parsed.hostname || '',
    port: parsed.port ? Number(parsed.port) : 5432,
    db: parsed.pathname.replace(/^\//, ''),
    params: Object.fromEntries(parsed.searchParams.entries()),
  };
}

function normalizePostgresDsn(dsn) {
  const raw = String(dsn || '').trim();
  if (!raw) return { dsn: '', warnings: ['DSN is empty.'] };
  const withProtocol = raw.startsWith('postgres://')
    ? `postgresql://${raw.slice('postgres://'.length)}`
    : (raw.startsWith('postgresql://') ? raw : `postgresql://${raw}`);

  let rebuilt = withProtocol;
  const warnings = [];
  try {
    const parsed = new URL(withProtocol);
    const safeUser = encodeURIComponent(decodeURIComponent(parsed.username || ''));
    const safePassword = encodeURIComponent(decodeURIComponent(parsed.password || ''));
    const auth = safeUser ? `${safeUser}${safePassword ? `:${safePassword}` : ''}@` : '';
    if ((parsed.password || '').match(/[\s@:/?#\[\]]/)) {
      warnings.push('Password had reserved characters and was normalized with percent-encoding.');
    }
    rebuilt = `postgresql://${auth}${parsed.host}${parsed.pathname}${parsed.search}`;
  } catch (_error) {
    warnings.push('DSN could not be fully parsed; kept best-effort normalized protocol.');
  }
  return { dsn: rebuilt, warnings };
}

function validatePostgresDsn(dsn) {
  const errors = [];
  const warnings = [];
  try {
    const parsed = parsePostgresDsn(dsn);
    if (!parsed.host) errors.push('DSN host is required.');
    if (!parsed.db) errors.push('DSN database name is required.');
  } catch (error) {
    errors.push(`Invalid Postgres DSN: ${error.message}`);
  }
  const normalized = normalizePostgresDsn(dsn);
  warnings.push(...normalized.warnings);
  return { ok: errors.length === 0, errors, warnings, normalizedDsn: normalized.dsn };
}

function maskPostgresDsn(dsn) {
  if (!dsn) return null;
  try {
    const parsed = new URL(dsn);
    const user = parsed.username ? `${decodeURIComponent(parsed.username)}:` : '';
    return `${parsed.protocol}//${user}***@${parsed.host}${parsed.pathname}${parsed.search}`;
  } catch (_error) {
    return 'postgresql://***';
  }
}

function fingerprintPostgresDsn(dsn) {
  try {
    const parsed = new URL(normalizePostgresDsn(dsn).dsn);
    parsed.password = '';
    const canonical = `${parsed.protocol}//${parsed.username}@${parsed.host}${parsed.pathname}${parsed.search}`;
    return crypto.createHash('sha256').update(canonical).digest('hex');
  } catch (_error) {
    return crypto.createHash('sha256').update(String(dsn || '')).digest('hex');
  }
}

module.exports = {
  parsePostgresDsn,
  normalizePostgresDsn,
  validatePostgresDsn,
  maskPostgresDsn,
  fingerprintPostgresDsn,
};
