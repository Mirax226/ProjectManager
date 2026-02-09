const { listCatalog, getRuleById } = require('./routineFixes');

function buildRunbookFromRule(rule) {
  const rendered = rule.render({});
  return {
    id: rule.id,
    title: rule.title,
    triggers: rule.triggers || [],
    symptoms: rendered.diagnosis || [],
    diagnosisSteps: rendered.diagnosis || [],
    fixSteps: rendered.steps || [],
    codexTaskTemplate: rendered.task || '',
    tags: ['routine', ...(rule.tags || [])],
  };
}

function generateRunbooksFromRoutineRules() {
  return listCatalog().map((meta) => {
    const rule = getRuleById(meta.id);
    return buildRunbookFromRule(rule);
  });
}

function searchRunbooks(runbooks, query) {
  const q = String(query || '').trim().toLowerCase();
  if (!q) return runbooks;
  return runbooks.filter((item) => `${item.title} ${item.triggers.join(' ')} ${item.tags.join(' ')}`.toLowerCase().includes(q));
}

module.exports = { buildRunbookFromRule, generateRunbooksFromRoutineRules, searchRunbooks };
