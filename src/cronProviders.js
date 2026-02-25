const cronJobOrgProvider = require('./cronProviders/cronJobOrgProvider');
const directPingProvider = require('./cronProviders/directPingProvider');

function resolveCronProvider(name) {
  const normalized = String(name || '').trim().toLowerCase();
  if (normalized === 'cronjob_org') return cronJobOrgProvider;
  if (normalized === 'local' || normalized === 'none' || !normalized) return directPingProvider;
  return directPingProvider;
}

module.exports = { resolveCronProvider };
