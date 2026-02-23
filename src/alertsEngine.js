const { ingestLog } = require('./logsHubStore');

const WINDOW_MS = 5 * 60 * 1000;

function shouldEscalateCron(log, allLogs = [], now = Date.now()) {
  if (log.category !== 'CRON_ERROR') return false;
  const lower = now - WINDOW_MS;
  const hits = allLogs.filter((row) => row.fingerprint === log.fingerprint).reduce((sum, row) => {
    const ts = new Date(row.last_seen_at).getTime();
    if (Number.isNaN(ts) || ts < lower) return sum;
    return sum + (row.occurrence_count || 1);
  }, 0);
  return hits >= 3;
}

function resolveAlertLevel(log, allLogs, now) {
  if (['DB_ERROR', 'DRIFT_DETECTED', 'DEPLOY_ERROR'].includes(log.category)) return 'error';
  if (shouldEscalateCron(log, allLogs, now)) return 'error';
  return log.level;
}

async function ingestAlertEvent(input, deps = {}) {
  const now = deps.now || Date.now();
  const listLogs = deps.listLogs || (async () => []);
  const result = await ingestLog(input);
  const logs = await listLogs({});
  const event = { ...result.event, level: resolveAlertLevel(result.event, logs, now) };
  return { ...result, event, shouldNotify: event.level === 'error' || event.level === 'warn' };
}

module.exports = { ingestAlertEvent, resolveAlertLevel, shouldEscalateCron };
