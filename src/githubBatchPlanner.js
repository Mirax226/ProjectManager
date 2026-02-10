const ORDER = Object.freeze({
  REPO_INSPECTION: 1,
  HEALTH: 2,
  CONFIG_TEMPLATE: 3,
  ROUTINE_UI: 4,
  OTHER: 9,
});

function classifyItem(item) {
  const sourceType = String(item.sourceType || '').toLowerCase();
  const ruleId = String(item.ruleId || '').toUpperCase();
  if (sourceType === 'repo_inspection' || ruleId.includes('REPO_INSPECTION')) return ORDER.REPO_INSPECTION;
  if (ruleId.includes('HEALTH')) return ORDER.HEALTH;
  if (sourceType === 'drift' || sourceType === 'template') return ORDER.CONFIG_TEMPLATE;
  if (sourceType === 'routine') return ORDER.ROUTINE_UI;
  return ORDER.OTHER;
}

function rangesOverlap(aStart, aEnd, bStart, bEnd) {
  return aStart <= bEnd && bStart <= aEnd;
}

function detectConflicts(ops = []) {
  const conflicts = [];
  for (let i = 0; i < ops.length; i += 1) {
    for (let j = i + 1; j < ops.length; j += 1) {
      const a = ops[i];
      const b = ops[j];
      if (a.file !== b.file) continue;
      if (a.key && b.key && a.key === b.key) {
        conflicts.push({ type: 'key', file: a.file, key: a.key, refs: [a.refId, b.refId] });
        continue;
      }
      if ([a.startLine, a.endLine, b.startLine, b.endLine].every((n) => Number.isFinite(n))) {
        if (rangesOverlap(a.startLine, a.endLine, b.startLine, b.endLine)) {
          conflicts.push({
            type: 'line_overlap',
            file: a.file,
            rangeA: [a.startLine, a.endLine],
            rangeB: [b.startLine, b.endLine],
            refs: [a.refId, b.refId],
          });
        }
      }
    }
  }
  return conflicts;
}

function planBatch(items = []) {
  const ordered = items
    .map((item, idx) => ({ item, idx, order: classifyItem(item) }))
    .sort((a, b) => (a.order - b.order) || (a.idx - b.idx))
    .map((entry) => entry.item);

  const operations = ordered.flatMap((item) =>
    (Array.isArray(item.patchOperations) ? item.patchOperations : []).map((op) => ({
      ...op,
      refId: item.refId,
    }))
  );

  const conflicts = detectConflicts(operations);
  const files = Array.from(new Set(operations.map((op) => op.file).filter(Boolean))).sort();

  return {
    orderedItems: ordered,
    operations,
    conflicts,
    conflictCount: conflicts.length,
    files,
    estimatedDiffSize: operations.reduce((sum, op) => sum + Math.abs(Number(op.linesChanged) || 0), 0),
  };
}

module.exports = { planBatch, detectConflicts };
