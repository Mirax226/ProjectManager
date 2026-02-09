function planRiskyOperation(operation, payload = {}) {
  const op = String(operation || 'unknown');
  const estimatedRows = Number(payload.estimatedRows) || 0;
  const size = estimatedRows < 100 ? 'small' : estimatedRows < 2000 ? 'medium' : 'large';
  return {
    operation: op,
    operations: Array.isArray(payload.operations) ? payload.operations : [op],
    target: payload.target || null,
    estimatedRows,
    estimateCategory: size,
    rollbackHints: payload.rollbackHints || ['Keep a DB backup before execute.', 'Capture affected IDs before mutation.'],
    requiresConfirm: true,
  };
}

module.exports = { planRiskyOperation };
