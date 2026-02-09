const { buildRoutineTemplate } = require('./template');

const rules = [
  require('./rules/nonMenuDeleteButton'),
  require('./rules/genericSomethingWrong'),
  require('./rules/workdirOutsideRepo'),
  require('./rules/cronCreate500'),
  require('./rules/invalidDsn'),
  require('./rules/envScanStuck'),
  require('./rules/repoInspectionMissing'),
  require('./rules/schemaRunnerMiniApp'),
  require('./rules/oomLoop'),
  ...require('./rules/expandedCatalog'),
];

const SIGNAL_SYNONYMS = {

  OLD_MENU_NOT_CLEANED: ['old menu messages', 'menu clutter'],
  CRON_4XX_5XX: ['cron 4xx', 'cron 5xx', 'rate limited'],
  DB_TIMEOUTS: ['db timeout', 'statement timeout', 'connection timeout'],
  WIZARD_STUCK: ['wizard stuck', 'flow stuck'],
  HEALTH_MISCONFIG: ['missing /health', 'health misconfigured'],
  TELEGRAM_CALLBACK_ERRORS: ['message not modified', 'message too old'],
  DUPLICATE_NOTIFICATION_SPAM: ['duplicate notifications', 'spam alerts'],
  SAFE_MODE_STATE_BUG: ['safe mode not triggering', 'safe mode not exiting'],
  DRIFT_BASELINE_MISSING: ['drift baseline missing'],
  DUAL_DB_SYNC_CONFLICT: ['dual db sync', 'divergence'],
  SCHEMA_RUNNER_REGRESSION: ['schema runner not in mini app'],
  CRON_CREATE_500: ['cron 500', 'cron-job', 'failed to create cron job'],
  INVALID_DSN: ['invalid url', 'dsn', 'postgres url'],
  WORKDIR_OUTSIDE_REPO: ['outside repo', 'working directory invalid'],
  GENERIC_HANDLER: ['something went wrong'],
  OOM_LOOP: ['heap out of memory', 'reached heap limit'],
  ENVSCAN_STUCK: ['env scan stuck', 'waiting testing'],
  REPO_INSPECTION_MISSING_FEATURES: ['repo inspection missing'],
};

function normalizeText(value) {
  return String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function extractRefId(text) {
  const match = String(text || '').match(/\b((?:CRON|DB|REPO|ENV)-[A-Z0-9-]+)\b/i);
  return match ? match[1].toUpperCase() : null;
}

function detectSignals(text) {
  const signals = new Set();
  for (const [signal, keywords] of Object.entries(SIGNAL_SYNONYMS)) {
    if (keywords.some((keyword) => text.includes(keyword))) {
      signals.add(signal);
    }
  }
  return signals;
}

function buildMatchContext(input = {}) {
  const rawText = String(input.rawText || '');
  const normalizedText = normalizeText(rawText);
  const refId = input.refId || extractRefId(rawText) || null;
  const category = input.category || null;
  return {
    rawText,
    normalizedText,
    refId,
    category,
    signals: detectSignals(normalizedText),
  };
}

function boostConfidence(ruleId, context, confidence) {
  let next = confidence;
  if (context.signals.has(ruleId)) next += 0.1;
  if (context.refId && context.refId.startsWith('CRON-') && ruleId === 'CRON_CREATE_500') next += 0.08;
  if (context.refId && context.refId.startsWith('DB-') && ruleId === 'INVALID_DSN') next += 0.08;
  if (context.refId && context.refId.startsWith('REPO-') && ruleId === 'WORKDIR_OUTSIDE_REPO') next += 0.08;
  if (context.refId && context.refId.startsWith('ENV-') && ruleId === 'ENVSCAN_STUCK') next += 0.08;
  if (context.category === 'INVALID_URL' && ruleId === 'INVALID_DSN') next += 0.08;
  return Math.min(1, next);
}

function rankRules(input) {
  const context = buildMatchContext(input);
  const matches = [];
  for (const rule of rules) {
    const base = rule.match(context);
    if (!base || typeof base.confidence !== 'number') continue;
    matches.push({
      rule,
      confidence: boostConfidence(rule.id, context, base.confidence),
      fields: base.fields || {},
    });
  }
  matches.sort((a, b) => b.confidence - a.confidence);
  return { context, matches };
}

function renderMatch(match) {
  const rendered = match.rule.render(match.fields || {});
  return {
    ruleId: match.rule.id,
    title: match.rule.title,
    confidence: match.confidence,
    diagnosis: rendered.diagnosis,
    steps: rendered.steps,
    task: rendered.task,
    templateText: buildRoutineTemplate({
      diagnosisLines: rendered.diagnosis,
      steps: rendered.steps,
      taskText: rendered.task,
    }),
  };
}

function matchBest(input, threshold = 0.8) {
  const ranked = rankRules(input);
  const best = ranked.matches[0] || null;
  const accepted = best && best.confidence >= threshold ? renderMatch(best) : null;
  return { ...ranked, best, accepted };
}

function listCatalog() {
  return rules.map((rule) => ({ id: rule.id, title: rule.title, triggers: rule.triggers || [] }));
}

function getRuleById(ruleId) {
  return rules.find((rule) => rule.id === ruleId) || null;
}

module.exports = {
  ROUTINE_AUTO_THRESHOLD: 0.8,
  ROUTINE_BUTTON_THRESHOLD: 0.85,
  buildMatchContext,
  rankRules,
  matchBest,
  renderMatch,
  listCatalog,
  getRuleById,
  buildRoutineTemplate,
};
