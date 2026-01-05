const LOG_LEVELS = ['error', 'warn', 'info'];

function normalizeLogLevel(input) {
  if (!input) return null;
  const normalized = String(input).toLowerCase();
  if (!LOG_LEVELS.includes(normalized)) return null;
  return normalized;
}

module.exports = {
  LOG_LEVELS,
  normalizeLogLevel,
};
