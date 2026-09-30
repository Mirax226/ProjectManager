const test = require('node:test');
const assert = require('node:assert/strict');
const { inspectConfigDbDsn, tryFixPostgresDsn, maskDsn } = require('../configDb');
const { classifyConfigDbError, configDbIncidentFingerprint, isConfigDbRetryable, sanitizeDbErrorMessage } = require('../configDbErrors');
const { appendEvent, shouldRouteEvent, resolveConfigDbIncidents, setDbHealthSnapshot, getDbHealthSnapshot, shouldNotifyRecovery } = require('../opsReliability');
const { saveJson } = require('../configStore');
const { setLogStatus, listLogs, autoResolveLogs } = require('../src/logsHubStore');
const { executeJob, codexChildEnvironment, runCommand } = require('../src/runner/jobExecutor');

test('Config DB structural preflight supports IP hosts and rejects invalid labels, ports and overrides', () => {
  for (const host of ['localhost', '127.0.0.1', '[::1]', 'db.example.com']) assert.equal(inspectConfigDbDsn(`postgres://user:pass@${host}/db`).category, null);
  for (const host of ['bad_host', 'bad-.example', '-bad.example', 'bad..example', 'x'.repeat(64)]) assert.equal(inspectConfigDbDsn(`postgres://user:pass@${host}/db`).category, 'CONFIG_DB_HOST_INVALID');
  for (const suffix of [':0/db', ':65536/db', '/db?host=other.example.com', '/db?port=1234', '/']) assert.equal(inspectConfigDbDsn(`postgres://user:pass@db.example.com${suffix}`).category, 'CONFIG_DB_DSN_INVALID');
  assert.equal(inspectConfigDbDsn('postgres://user:%ZZ@db.example.com/db').category, 'CONFIG_DB_DSN_INVALID');
});

test('Tenant/user provider lookup failure differs from actual DNS ENOTFOUND', () => {
  assert.equal(classifyConfigDbError({ code: 'ENOTFOUND', message: 'tenant/user postgres.bqvyprmlqcbrwepnrwoc not found' }), 'CONFIG_DB_AUTH_FAILED');
  assert.equal(classifyConfigDbError({ code: 'ENOTFOUND', message: 'getaddrinfo ENOTFOUND postgres.bqvyprmlqcbrwepnrwoc' }), 'CONFIG_DB_DNS_FAILED');
});

test('Config DB typed errors survive catch boundaries and classify known SQL/TLS/timeout errors', () => {
  for (const code of ['CONFIG_DB_DSN_MISSING', 'CONFIG_DB_DSN_INVALID', 'CONFIG_DB_HOST_INVALID']) assert.equal(classifyConfigDbError({ code }), code);
  assert.equal(classifyConfigDbError({ code: '28P01' }), 'CONFIG_DB_AUTH_FAILED');
  assert.equal(classifyConfigDbError({ code: '42P01' }), 'CONFIG_DB_QUERY_FAILED');
  assert.equal(classifyConfigDbError({ code: 'ETIMEDOUT' }), 'CONFIG_DB_CONNECT_TIMEOUT');
  assert.equal(classifyConfigDbError({ code: 'SELF_SIGNED_CERT_IN_CHAIN' }), 'CONFIG_DB_TLS_FAILED');
  assert.equal(classifyConfigDbError(new Error('unrecognized failure')), 'UNKNOWN_DB_ERROR');
});

test('Only transient or unknown Config DB errors retry; structural and provider authentication failures halt', () => {
  for (const category of ['CONFIG_DB_DNS_FAILED', 'CONFIG_DB_CONNECT_TIMEOUT', 'UNKNOWN_DB_ERROR']) assert.equal(isConfigDbRetryable(category), true);
  for (const category of ['CONFIG_DB_DSN_MISSING', 'CONFIG_DB_DSN_INVALID', 'CONFIG_DB_HOST_INVALID', 'CONFIG_DB_AUTH_FAILED', 'CONFIG_DB_TLS_FAILED', 'CONFIG_DB_QUERY_FAILED']) assert.equal(isConfigDbRetryable(category), false);
});

test('Repair preserves username/host separation and does not double encode', () => {
  const raw = 'postgres://postgres.bqvyprmlqcbrwepnrwoc:pa?ss@db.example.com:5432/db';
  const fixed = tryFixPostgresDsn(raw);
  assert.equal(fixed.fixed, true);
  assert.equal(new URL(fixed.dsn).username, 'postgres.bqvyprmlqcbrwepnrwoc');
  assert.equal(inspectConfigDbDsn(fixed.dsn).hostname, 'db.example.com');
  assert.equal(tryFixPostgresDsn(fixed.dsn).dsn, fixed.dsn);
  assert.equal(tryFixPostgresDsn(raw).dsn, fixed.dsn);
});

test('DSN diagnostics never expose full URI, password suffix or query credentials', () => {
  const raw = 'postgres://user:secret1234@db.example.com/db?api_key=hidden';
  assert.equal(maskDsn(raw), 'postgresql://[REDACTED]');
  assert.equal(maskDsn('unparseable secret1234'), 'postgresql://[REDACTED]');
  const masked = sanitizeDbErrorMessage(`failed ${raw} password=another token=private`);
  for (const secret of ['secret1234', 'hidden', 'another', 'private', 'user']) assert.equal(masked.includes(secret), false);
});

test('Actual ops alert pathway deduplicates retries, acknowledgment and wrapper changes until recovery', async () => {
  await saveJson('ops_event_log', []);
  const root = { source: 'DATABASE_URL_PM', category: 'CONFIG_DB_DNS_FAILED', hostname: 'db.example.com' };
  const prefs = { user_id: 'pj011b', enable_alerts: true, severity_threshold: 'warn', rate_limits: { max_per_10m: 50, debounce_seconds_per_category: 0 } };
  async function append(message, overrides = {}) {
    const fields = { ...root, ...overrides, message };
    return appendEvent({ level: 'warn', source: 'config-db', category: fields.category, messageShort: message, fingerprint: configDbIncidentFingerprint(fields), meta: { envVar: fields.source, hostname: fields.hostname } });
  }
  const first = await append('warmup failed: getaddrinfo ENOTFOUND db.example.com');
  assert.equal(shouldRouteEvent(prefs, first), true);
  await setLogStatus({ id: first.id, status: 'acknowledged' });
  const repeated = await append('warmup exception: ENOTFOUND db.example.com');
  assert.equal(repeated.id, first.id);
  assert.equal(repeated.occurrence_count, 2);
  assert.equal(shouldRouteEvent(prefs, repeated), false);
  const different = await append('auth failed', { category: 'CONFIG_DB_AUTH_FAILED' });
  assert.equal(shouldRouteEvent(prefs, different), true);
  const otherSource = await append('same failure', { source: 'PATH_APPLIER_CONFIG_DSN' });
  assert.equal(shouldRouteEvent(prefs, otherSource), true);
  const unknown = await append('unrecognized provider failure', { category: 'UNKNOWN_DB_ERROR' });
  assert.equal(shouldRouteEvent(prefs, unknown), true);
  assert.equal((await autoResolveLogs({ now: Date.now() + 3600000 })).changed, 0);
  await resolveConfigDbIncidents('DATABASE_URL_PM');
  assert.equal((await listLogs({})).filter((row) => row.status === 'resolved').length, 3);
  const next = await append('new outage');
  assert.notEqual(next.id, first.id);
  assert.equal(shouldRouteEvent(prefs, next), true);
});

test('Initial outage and subsequent recovery notify once through existing snapshot contract', async () => {
  await setDbHealthSnapshot({ status: 'DOWN', lastErrorCategory: 'CONFIG_DB_DNS_FAILED' });
  await setDbHealthSnapshot({ status: 'HEALTHY', lastErrorCategory: null });
  const snap = getDbHealthSnapshot();
  assert.equal(shouldNotifyRecovery(snap), true);
  await setDbHealthSnapshot({ lastRecoveryNotifiedOutageId: snap.lastOutageId });
  assert.equal(shouldNotifyRecovery(getDbHealthSnapshot()), false);
});

test('CODEX_TASK limits child environment, enforces sandbox and binds project', async () => {
  assert.deepEqual(codexChildEnvironment({ Path: 'safe', HOME: 'home', BOT_TOKEN: 'secret', DATABASE_URL_PM: 'secret', PG_RUNNER_TOKEN: 'secret' }), { Path: 'safe', HOME: 'home' });
  let captured;
  const job = { type: 'CODEX_TASK', projectId: 'daily-system', payload: { task: '--inspect', mode: 'read-only' } };
  const result = await executeJob(job, { profile: { repoPath: process.cwd() }, runCommand: async (...args) => { captured = args; return { ok: true, stdout: 'password=private', stderr: '' }; } });
  assert.equal(result.result.stdout, '[REDACTED]');
  assert.deepEqual(captured[1].slice(0, 4), ['exec', '--sandbox', 'read-only', '--']);
  assert.equal(captured[3].replaceEnv, true);
  assert.equal(captured[3].env.BOT_TOKEN, undefined);
  assert.equal((await executeJob({ ...job, projectId: 'other' }, { profile: { repoPath: process.cwd() } })).ok, false);
  assert.equal((await executeJob({ ...job, payload: { task: 'inspect', mode: 'invalid' } }, { profile: { repoPath: process.cwd() } })).ok, false);
});

test('Runner captures bounded output and replacement environment in a real child process', async () => {
  const result = await runCommand(process.execPath, ['-e', 'process.stdout.write(String(process.env.PJ_SENTINEL)+"x".repeat(100000));process.stderr.write("y".repeat(100000))'], process.cwd(), { replaceEnv: true, env: { PJ_SENTINEL: 'isolated' } });
  assert.equal(result.ok, true);
  assert.equal(result.stdout.startsWith('isolated'), true);
  assert.ok(result.stdout.length <= 12000);
  assert.ok(result.stderr.length <= 12000);
});
