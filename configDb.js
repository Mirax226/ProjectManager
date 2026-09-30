const { Pool } = require('pg');
const { isIP } = require('node:net');
const { classifyDbError, classifyConfigDbError, sanitizeDbErrorMessage } = require('./configDbErrors');

let pool = null;
let sslWarningEmitted = false;
let dsnAutoFixApplied = false;
const DB_POOL_MAX = 3;
const DB_IDLE_TIMEOUT_MS = 30_000;
const DB_CONNECTION_TIMEOUT_MS = 15_000;
const DB_STATEMENT_TIMEOUT_MS = 5000;
const DB_OPERATION_TIMEOUT_MS = 5000;
const ALLOW_INSECURE_TLS_FOR_TESTS = process.env.ALLOW_INSECURE_TLS_FOR_TESTS === 'true';

function getConfigDbEnvSource() {
  if (process.env.DATABASE_URL_PM) {
    return { envVar: 'DATABASE_URL_PM', dsn: process.env.DATABASE_URL_PM };
  }
  if (process.env.PATH_APPLIER_CONFIG_DSN) {
    return { envVar: 'PATH_APPLIER_CONFIG_DSN', dsn: process.env.PATH_APPLIER_CONFIG_DSN };
  }
  return { envVar: null, dsn: null };
}

function inspectConfigDbDsn(dsn) {
  const value = String(dsn || '');
  const base = { configured: Boolean(value), valid: false, scheme: null, hostname: null, port: null, databaseNamePresent: false, usernamePresent: false, hostValid: false };
  if (!value) return { ...base, category: 'CONFIG_DB_DSN_MISSING' };
  let parsed;
  try { parsed = new URL(value); } catch (_error) { return { ...base, category: 'CONFIG_DB_DSN_INVALID' }; }
  const scheme = String(parsed.protocol || '').toLowerCase();
  const hostname = String(parsed.hostname || '');
  const ipHost = hostname.replace(/^\[|\]$/g, '');
  const hostValid = Boolean(isIP(ipHost)) || (hostname.length <= 253 && hostname.split('.').every((label) => /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/.test(label)));
  const validScheme = scheme === 'postgres:' || scheme === 'postgresql:';
  const databaseNamePresent = String(parsed.pathname || '').replace(/^\/+/, '').length > 0;
  const result = { ...base, valid: validScheme && hostValid && databaseNamePresent, scheme: scheme || null, hostname: hostname || null, port: parsed.port ? Number(parsed.port) : 5432, databaseNamePresent, usernamePresent: Boolean(parsed.username), hostValid };
  if (!validScheme) return { ...result, category: 'CONFIG_DB_DSN_INVALID' };
  // pg-connection-string query parameters can override URI authority fields.
  // Reject ambiguous destinations rather than validating one host and dialing another.
  if (['host', 'port'].some((key) => parsed.searchParams.has(key)) || (parsed.port && (Number(parsed.port) < 1 || Number(parsed.port) > 65535))) return { ...result, category: 'CONFIG_DB_DSN_INVALID' };
  try { decodeURIComponent(parsed.username); decodeURIComponent(parsed.password); decodeURI(parsed.pathname); } catch (_) { return { ...result, category: 'CONFIG_DB_DSN_INVALID' }; }
  if (!hostValid) return { ...result, category: 'CONFIG_DB_HOST_INVALID' };
  if (!databaseNamePresent) return { ...result, category: 'CONFIG_DB_DSN_INVALID' };
  return { ...result, category: null };
}

function getConfigDbSourceMetadata() {
  const source = getConfigDbEnvSource();
  const inspected = inspectConfigDbDsn(tryFixPostgresDsn(source.dsn).dsn);
  return { envVar: source.envVar, configured: inspected.configured, category: inspected.category, scheme: inspected.scheme, hostname: inspected.hostname, port: inspected.port, databaseNamePresent: inspected.databaseNamePresent, usernamePresent: inspected.usernamePresent };
}

function createConfigDbError(category, message, details = {}) {
  const error = new Error(message || category);
  error.code = category;
  error.configDbCategory = category;
  error.configDbDetails = details;
  return error;
}

function maskDsn(dsn) {
  if (!dsn) return null;
  return 'postgresql://[REDACTED]';
}

function tryFixPostgresDsn(dsn) {
  const dsnText = String(dsn || '');
  if (!dsnText) {
    return { dsn: dsnText, fixed: false };
  }

  try {
    new URL(dsnText);
    return { dsn: dsnText, fixed: false };
  } catch (error) {
    const schemeMatch = dsnText.match(/^(postgres(?:ql)?:\/\/)(.+)$/i);
    if (!schemeMatch) {
      return { dsn: dsnText, fixed: false, error };
    }

    const scheme = schemeMatch[1];
    const remainder = schemeMatch[2];
    const atIndex = remainder.lastIndexOf('@');
    if (atIndex <= 0 || atIndex >= remainder.length - 1) {
      return { dsn: dsnText, fixed: false, error };
    }

    const userInfo = remainder.slice(0, atIndex);
    const hostPart = remainder.slice(atIndex + 1);
    const separatorIndex = userInfo.indexOf(':');

    let rebuilt;
    if (separatorIndex >= 0) {
      const user = userInfo.slice(0, separatorIndex);
      const password = userInfo.slice(separatorIndex + 1);
      const encodedUser = encodeURIComponent(user);
      const encodedPassword = encodeURIComponent(password);
      rebuilt = `${scheme}${encodedUser}:${encodedPassword}@${hostPart}`;
    } else {
      const encodedUser = encodeURIComponent(userInfo);
      rebuilt = `${scheme}${encodedUser}@${hostPart}`;
    }

    try {
      new URL(rebuilt);
      return { dsn: rebuilt, fixed: true };
    } catch (validationError) {
      return { dsn: dsnText, fixed: false, error: validationError };
    }
  }
}

async function forwardConfigDbWarning(message, context) {
  try {
    const { forwardSelfLog } = require('./logger');
    if (typeof forwardSelfLog === 'function') {
      await forwardSelfLog('warn', message, { context });
      return;
    }
  } catch (_error) {
    // noop fallback to console only
  }
}

async function getConfigDbPool() {
  const { envVar, dsn: rawDsn } = getConfigDbEnvSource();
  if (!rawDsn) {
    console.warn('[configDb] DATABASE_URL_PM/PATH_APPLIER_CONFIG_DSN not set; using in-memory config only.');
    return null;
  }

  let dsn = rawDsn;
  {
    const fixResult = tryFixPostgresDsn(rawDsn);
    if (fixResult.fixed) {
      dsn = fixResult.dsn;
      const shouldWarn = !dsnAutoFixApplied;
      dsnAutoFixApplied = true;
      const warningMessage = `[configDb] Auto-fixed malformed Postgres DSN from ${envVar} (detected unescaped special characters in username/password). Please update ENV with encoded credentials.`;
      const context = {
        envVar,
        detected: 'Invalid URL caused by unescaped special characters in username/password',
        fixHint: 'Set encoded DSN in ENV (encode username/password only).',
      };
      if (shouldWarn) { console.warn(warningMessage, context); await forwardConfigDbWarning(warningMessage, context); }
    }
  }

  const dsnInspection = inspectConfigDbDsn(dsn);
  if (dsnInspection.category) {
    throw createConfigDbError(dsnInspection.category, `Config DB DSN preflight failed: ${dsnInspection.category}`, { envVar, ...dsnInspection });
  }

  if (!pool) {
    if (!sslWarningEmitted) {
      const sslMode = new URL(dsn).search.toLowerCase();
      if (sslMode.includes('sslmode=require') && !sslMode.includes('uselibpqcompat=true')) {
        console.warn(
          '[configDb] SSL warning: add uselibpqcompat=true or use direct 5432 Supabase host to avoid SSL chain errors.',
        );
        sslWarningEmitted = true;
      }
    }
    const sslMode = new URL(dsn).search.toLowerCase();
    const sslRequired = sslMode.includes('sslmode=require') || sslMode.includes('ssl=true');
    const ssl = sslRequired
      ? { rejectUnauthorized: !ALLOW_INSECURE_TLS_FOR_TESTS }
      : undefined;
    pool = new Pool({
      connectionString: dsn,
      max: DB_POOL_MAX,
      idleTimeoutMillis: DB_IDLE_TIMEOUT_MS,
      connectionTimeoutMillis: DB_CONNECTION_TIMEOUT_MS,
      options: `-c statement_timeout=${DB_STATEMENT_TIMEOUT_MS}`,
      keepAlive: true,
      ssl,
    });
  }

  return pool;
}

function isDsnAutoFixApplied() {
  return dsnAutoFixApplied;
}

async function withDbTimeout(promise, context) {
  if (!promise || typeof promise.then !== 'function') return promise;
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error('DB_TIMEOUT');
      error.code = 'DB_TIMEOUT';
      error.context = context;
      reject(error);
    }, DB_OPERATION_TIMEOUT_MS);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

async function testConfigDbConnection() {
  try {
    const source = getConfigDbEnvSource();
    if (!source.dsn) return { ok: false, category: 'CONFIG_DB_DSN_MISSING', message: 'Config DB DSN is not configured', configured: false, source: source.envVar };
    const db = await getConfigDbPool();
    if (!db) return { ok: false, category: 'CONFIG_DB_DSN_MISSING', message: 'Config DB DSN is not configured', configured: false, source: source.envVar };
    await withDbTimeout(db.query('SELECT 1'), 'config_db_test');
    return { ok: true, configured: true, source: source.envVar };
  } catch (error) {
    let category = error.configDbCategory || classifyConfigDbError(error);
    if (category === 'UNKNOWN_DB_ERROR' && /^[0-9A-Z]{5}$/.test(String(error.code || ''))) category = 'CONFIG_DB_QUERY_FAILED';
    const message = sanitizeDbErrorMessage(error?.message) || 'connection failed';
    return { ok: false, category, legacyCategory: classifyDbError(error), message, configured: true };
  }
}

module.exports = {
  getConfigDbPool,
  testConfigDbConnection,
  maskDsn,
  tryFixPostgresDsn,
  getConfigDbEnvSource,
  inspectConfigDbDsn,
  getConfigDbSourceMetadata,
  classifyConfigDbError,
  isDsnAutoFixApplied,
};
