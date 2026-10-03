const fs = require('fs');
const path = require('path');
const os = require('os');
const { sanitizeDbErrorMessage } = require('../../configDbErrors');

const ZJ_REPO = 'C:\\Users\\Amir\\Documents\\GitHub\\ZJ';
const ZJ_PLANS = 'C:\\Users\\Amir\\Documents\\GitHub\\Plans\\ZJ';
const VALIDATION = Object.freeze([
  ['tests', ['test', '--', '--reporter=dot']],
  ['lint', ['run', 'lint']],
  ['typecheck', ['run', 'typecheck']],
  ['build', ['run', 'build']],
  ['credential_scan', ['run', 'security:secrets']],
  ['dependency_audit', ['run', 'security:deps']],
  ['security', ['run', 'security:check']],
  ['verify', ['run', 'verify']],
]);
const ACCEPTED_SCRIPTS = Object.freeze({
  test: 'vitest run', lint: 'eslint .', typecheck: 'tsc --noEmit',
  build: 'wrangler deploy --dry-run --outdir dist',
  'security:secrets': 'node scripts/security-secrets.mjs',
  'security:deps': 'node scripts/security-deps.mjs',
  'security:check': 'npm run security:secrets && npm run security:deps',
  verify: 'npm run security:check && npm run lint && npm run typecheck && npm test -- --reporter=dot && npm run build',
});
function acceptedScripts(pkg) {
  return Object.entries(ACCEPTED_SCRIPTS).every(([name, command]) => pkg.scripts?.[name] === command
    && !pkg.scripts?.[`pre${name}`] && !pkg.scripts?.[`post${name}`]);
}
const RC_DOMAINS = Object.freeze(['registration', 'main_menu', 'weekly_plan', 'plan_edit', 'daily_checkin', 'weekly_report', 'account_profile', 'notifications', 'streak_badges', 'study_tools', 'tickets', 'admin_auth', 'admin_ticket_operations', 'ai_fallback', 'counseling', 'scheduler', 'leases_retries', 'tehran_dates', 'd1_migrations', 'security_redaction', 'cross_user_isolation', 'callback_validation', 'observability', 'correlation_ids', 'ai_metrics', 'release_guards', 'dependency_audit', 'telegram_privacy']);
function validationReport(report, commit, durationMs) {
  const count = (name) => Number.isSafeInteger(report?.[name]) && report[name] >= 0 ? report[name] : null;
  const testCount = count('numTotalTests'); const passCount = count('numPassedTests'); const failCount = count('numFailedTests');
  const suites = Array.isArray(report?.testResults) ? report.testResults : null;
  return { domain: 'all_zj_domains', suite: 'vitest', testFileCount: suites?.length ?? null, testCount, passCount, failCount, durationMs,
    status: report?.success === true && failCount === 0 && testCount != null ? 'PASS' : report?.success === false ? 'FAIL' : 'UNKNOWN',
    residualRisk: 'Local tests do not establish live staging or Telegram acceptance.', timestamp: new Date().toISOString(), commit,
    rcMatrix: RC_DOMAINS.map((domain) => ({ domain, status: 'UNKNOWN', source: 'ZJ repository tests; individual-domain attribution is not inferred from aggregate totals' })) };
}
const safeText = (value, max = 240) => String(sanitizeDbErrorMessage(String(value || '')))
  .replace(/(?:https?:\/\/)?[^\s:@]+:[^\s@]+@[^\s]+/g, '[REDACTED]')
  .replace(/\bBearer\s+[A-Za-z0-9._-]+/gi, '[REDACTED]')
  .replace(/\b\d{8,12}:[A-Za-z0-9_-]{30,}\b/g, '[REDACTED]')
  .replace(/\b(?:sk|ghp|github_pat|cf)_[A-Za-z0-9_-]{20,}\b/gi, '[REDACTED]')
  .slice(0, max);
function readBounded(file, limit = 80_000) {
  const fd = fs.openSync(file, 'r');
  try {
    if (fs.fstatSync(fd).size > limit) throw new Error('evidence file exceeds limit');
    const bytes = Buffer.alloc(limit);
    return bytes.subarray(0, fs.readSync(fd, bytes, 0, limit, 0)).toString('utf8');
  } finally { fs.closeSync(fd); }
}

function paths(profile = {}) {
  return { repo: path.resolve(profile.repoPath || process.env.ZJ_REPO_PATH || ZJ_REPO), plans: path.resolve(profile.plansPath || process.env.ZJ_PLANS_PATH || ZJ_PLANS) };
}

async function git(run, repo, args) {
  const out = await run('git', args, repo, { timeoutMs: 30_000 });
  return out.ok ? String(out.stdout || '').trim() : null;
}

async function repoStatus(run, profile) {
  const { repo } = paths(profile);
  if (!fs.existsSync(path.join(repo, '.git'))) return { ok: false, diagnosticCode: 'REPO_NOT_FOUND', result: { projectId: 'zj', state: 'ERROR' } };
  const [status, branch, head, upstream, aheadBehind, diffCheck, commits] = await Promise.all([
    git(run, repo, ['status', '--porcelain=v1']), git(run, repo, ['branch', '--show-current']),
    git(run, repo, ['rev-parse', 'HEAD']), git(run, repo, ['rev-parse', '@{upstream}']),
    git(run, repo, ['rev-list', '--left-right', '--count', 'HEAD...@{upstream}']),
    run('git', ['diff', '--check'], repo, { timeoutMs: 30_000 }),
    git(run, repo, ['log', '-5', '--format=%h %s']),
  ]);
  const counts = aheadBehind?.match(/^(\d+)\s+(\d+)$/);
  const ahead = counts ? Number(counts[1]) : null; const behind = counts ? Number(counts[2]) : null;
  const state = !head || status === null || branch === null ? 'ERROR' : status ? 'DIRTY' : !branch ? 'DETACHED' : !upstream || !counts ? 'REMOTE_MISMATCH' : ahead && behind ? 'REMOTE_MISMATCH' : ahead ? 'CLEAN_AHEAD' : behind ? 'CLEAN_BEHIND' : 'CLEAN_SYNCED';
  return { ok: state !== 'ERROR', diagnosticCode: state === 'DIRTY' ? 'REPO_DIRTY' : null, result: {
    projectId: 'zj', state, branch: branch || null, head: head || null, upstreamHead: upstream || null,
    ahead, behind, dirty: Boolean(status), diffCheck: diffCheck.ok ? 'PASS' : 'FAIL',
    recentCommits: (commits || '').split('\n').filter(Boolean).slice(0, 5).map((line) => safeText(line, 160)),
    timestamp: new Date().toISOString(),
  } };
}

function marker(text, expression) { return safeText(text.match(expression)?.[1] || 'UNKNOWN', 200); }
function releaseEvidence(profile) {
  const { plans } = paths(profile);
  let latest, recovery;
  try { latest = readBounded(path.join(plans, 'LATEST.md')); recovery = readBounded(path.join(plans, 'RECOVERY_CONTEXT.md')); }
  catch (_) { return { ok: false, diagnosticCode: 'PLANS_NOT_FOUND', result: { projectId: 'zj', status: 'UNKNOWN' } }; }
  const currentRecovery = recovery.split(/\n# Historical\b/i)[0];
  const text = `${latest}\n${currentRecovery}`;
  let releaseChecklist = 'UNKNOWN';
  try { const checklist = readBounded(path.join(paths(profile).repo, 'docs', 'release-checklist.md')); releaseChecklist = /NOT VERIFIED/i.test(checklist) ? 'EXTERNAL_ACCEPTANCE_UNKNOWN' : 'UNKNOWN'; } catch (_) { /* Optional reference remains unknown. */ }
  return { ok: true, result: {
    projectId: 'zj', checkpoint: marker(latest, /Task:\s*\*\*([^*]+)\*\*/i),
    branch: marker(text, /branch\s+`([^`]+)`/i),
    testBaseline: marker(text, /\*\*(\d+ test files\s*\/\s*[\d,]+ tests)\*\*/i),
    stagingWorker: /staging Worker[^\n]*\*\*VERIFIED MISSING\*\*/i.test(text) ? 'MISSING' : 'UNKNOWN',
    stagingD1: /staging D1[^\n]*\*\*VERIFIED MISSING\*\*|distinct staging D1[^\n]*\*\*VERIFIED MISSING\*\*/i.test(text) ? 'MISSING' : 'UNKNOWN',
    ci: /zero registered workflows\/runs|zero.*workflow.*runs/i.test(text) ? 'MISSING' : 'UNKNOWN',
    stagingTelegram: 'UNKNOWN', productionIsolation: 'UNKNOWN',
    releaseChecklist,
    blockers: /RELEASE NOT READY|NOT READY for staging or production/i.test(text) ? ['RELEASE_NOT_READY'] : [],
    recommendedNextStep: marker(latest, /Exact next task:\s*\*\*([^*]+)\*\*/i),
    timestamp: new Date().toISOString(),
  } };
}

async function localValidation(run, profile) {
  const { repo } = paths(profile);
  if (!fs.existsSync(path.join(repo, 'package.json'))) return { ok: false, diagnosticCode: 'REPO_NOT_FOUND', result: { projectId: 'zj', status: 'FAIL', commands: [] } };
  const source = await repoStatus(run, profile);
  if (!source.ok || source.result.state !== 'CLEAN_SYNCED') return { ok: false, diagnosticCode: source.result.state === 'DIRTY' ? 'REPO_DIRTY' : 'REPO_NOT_READY', result: { projectId: 'zj', status: 'FAIL', commands: [], commit: source.result.head || null } };
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-zj-validation-'));
  const copy = path.join(scratch, 'repo');
  const cleanup = () => { if (path.dirname(copy) === scratch && path.dirname(scratch) === path.resolve(os.tmpdir()) && path.basename(scratch).startsWith('pj-zj-validation-')) fs.rmSync(scratch, { recursive: true, force: true }); };
  try {
  const clone = await run('git', ['-c', 'core.autocrlf=false', 'clone', '--local', '--no-hardlinks', '--single-branch', '--branch', source.result.branch, repo, copy], scratch, { timeoutMs: 120_000, outputTail: true });
  if (!clone.ok) return { ok: false, diagnosticCode: 'VALIDATION_PREP_FAILED', result: { projectId: 'zj', status: 'FAIL', commands: [], commit: source.result.head } };
  const cloneHead = await git(run, copy, ['rev-parse', 'HEAD']);
  if (cloneHead !== source.result.head) return { ok: false, diagnosticCode: 'VALIDATION_SOURCE_CHANGED', result: { projectId: 'zj', status: 'FAIL', commands: [], commit: source.result.head } };
  const install = await run('npm.cmd', ['ci', '--ignore-scripts', '--no-audit', '--no-fund'], copy, { timeoutMs: 120_000, outputTail: true });
  if (!install.ok) return { ok: false, diagnosticCode: 'VALIDATION_PREP_FAILED', result: { projectId: 'zj', status: 'FAIL', commands: [{ commandId: 'dependencies', status: 'FAIL', exitCode: install.exitCode, durationMs: null, sanitizedFailureSummary: safeText(install.stderr || install.stdout) }], commit: source.result.head } };
  const pkg = JSON.parse(readBounded(path.join(copy, 'package.json')));
  if (!acceptedScripts(pkg)) return { ok: false, diagnosticCode: 'PRODUCTION_ACTION_FORBIDDEN', result: { projectId: 'zj', status: 'FAIL', commands: [], commit: source.result.head } };
  const commands = [];
  let suiteEvidence = null;
  const validationStarted = Date.now();
  for (const [id, args] of VALIDATION) {
    const remaining = 10 * 60_000 - (Date.now() - validationStarted);
    if (remaining < 1000) { commands.push({ commandId: id, status: 'FAIL', exitCode: null, durationMs: 0, timedOut: true, failureSummary: 'validation deadline exceeded' }); break; }
    const script = args[0] === 'test' ? 'test' : args[1];
    if (!pkg.scripts?.[script]) { commands.push({ commandId: id, status: 'UNKNOWN', exitCode: null, durationMs: 0, failureSummary: 'script is unavailable' }); continue; }
    const reportPath = path.join(scratch, 'vitest-results.json');
    const commandArgs = id === 'tests' ? [...args, '--reporter=json', `--outputFile.json=${reportPath}`] : args;
    const started = Date.now(); const out = await run('npm.cmd', commandArgs, copy, { timeoutMs: Math.min(id === 'verify' ? 240_000 : 120_000, remaining), outputTail: true });
    let testEvidence = null;
    if (id === 'tests') { try { testEvidence = validationReport(JSON.parse(readBounded(reportPath, 8 * 1024 * 1024)), source.result.head, Date.now() - started); } catch (_) { testEvidence = validationReport(null, source.result.head, Date.now() - started); } }
    if (testEvidence) suiteEvidence = testEvidence;
    commands.push({ commandId: id, status: out.ok ? 'PASS' : 'FAIL', exitCode: out.exitCode,
      durationMs: Date.now() - started, timedOut: Boolean(out.timedOut),
      testFiles: testEvidence?.testFileCount ?? null, testCount: testEvidence?.testCount ?? null, passCount: testEvidence?.passCount ?? null, failCount: testEvidence?.failCount ?? null,
      sanitizedFailureSummary: out.ok ? null : safeText(out.stderr || out.stdout),
    });
  }
  const status = commands.some((entry) => entry.status === 'FAIL') ? 'FAIL' : commands.some((entry) => entry.status === 'UNKNOWN') ? 'UNKNOWN' : 'PASS';
  return { ok: status === 'PASS', diagnosticCode: status === 'FAIL' ? 'VALIDATION_FAILED' : null,
    result: { projectId: 'zj', status, commands, testEvidence: suiteEvidence, commit: source.result.head, validationMode: 'DISPOSABLE_CLONE', timestamp: new Date().toISOString() } };
  } finally { cleanup(); }
}

async function stagingHealth(profile, fetchImpl = fetch) {
  const pending = () => ({ ok: false, diagnosticCode: 'STAGING_NOT_CONFIGURED', result: { projectId: 'zj', status: 'PENDING', timestamp: new Date().toISOString() } });
  const configured = String(profile.stagingHealthUrl || process.env.ZJ_STAGING_HEALTH_URL || '').trim();
  if (!configured) return pending();
  let url;
  try { url = new URL(configured); } catch (_) { return pending(); }
  if (url.protocol !== 'https:' || !/^zj-staging\.[a-z0-9.-]+\.workers\.dev$/i.test(url.hostname) || url.port || url.username || url.password || url.pathname !== '/' || url.search || url.hash) return pending();
  const started = Date.now();
  try {
    const response = await fetchImpl(new URL('/healthz', url), { method: 'GET', redirect: 'error', signal: AbortSignal.timeout(8000) });
    const reader = response.body?.getReader();
    if (!reader) throw new Error('health_body_missing');
    const chunks = []; let length = 0;
    try {
      while (true) {
        const { done, value } = await reader.read(); if (done) break;
        length += value.byteLength;
        if (length > 4096) { await reader.cancel(); throw new Error('health_body_limit'); }
        chunks.push(value);
      }
    } finally { reader.releaseLock(); }
    const bytes = new Uint8Array(length); let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    const body = new TextDecoder().decode(bytes);
    let data; try { data = JSON.parse(body); } catch (_) { data = null; }
    const healthy = response.status === 200 && data && data.status === 'healthy' && data.service === 'zj';
    return { ok: healthy, diagnosticCode: healthy ? null : 'REMOTE_HEALTH_BAD_RESPONSE', result: {
      projectId: 'zj', status: healthy ? 'HEALTHY' : 'UNAVAILABLE', httpStatus: response.status,
      latencyMs: Date.now() - started, health: healthy ? { status: data.status, service: data.service } : null,
      timestamp: new Date().toISOString(),
    } };
  } catch (error) { return { ok: false, diagnosticCode: error.name === 'TimeoutError' ? 'REMOTE_HEALTH_TIMEOUT' : 'REMOTE_HEALTH_BAD_RESPONSE', result: { projectId: 'zj', status: 'UNAVAILABLE', latencyMs: Date.now() - started, timestamp: new Date().toISOString() } }; }
}

module.exports = { ZJ_REPO, ZJ_PLANS, VALIDATION, ACCEPTED_SCRIPTS, acceptedScripts, RC_DOMAINS, validationReport, repoStatus, releaseEvidence, localValidation, stagingHealth };
