// Run in the bot's actual runtime. No query, network request, or ENV mutation.
const { getConfigDbEnvSource, inspectConfigDbDsn, tryFixPostgresDsn } = require('../configDb');
const source = getConfigDbEnvSource();
const raw = inspectConfigDbDsn(source.dsn);
const repair = tryFixPostgresDsn(source.dsn);
const effective = inspectConfigDbDsn(repair.dsn);
const observed = 'postgres.bqvyprmlqcbrwepnrwoc';
let role = source.dsn ? 'UNPARSEABLE' : 'NOT_CONFIGURED';
try {
  const parsed = new URL(repair.dsn);
  role = decodeURIComponent(parsed.username) === observed ? 'USERNAME_TENANT_IDENTIFIER' : parsed.hostname === observed ? 'HOSTNAME' : 'NOT_PRESENT_IN_URI_AUTHORITY';
} catch (_) {}
console.log(JSON.stringify({
  sourceVariableName: source.envVar,
  configured: Boolean(source.dsn),
  scheme: effective.scheme,
  rawParsedHostname: raw.hostname,
  effectiveParsedHostname: effective.hostname,
  portPresent: (() => { try { return Boolean(new URL(repair.dsn).port); } catch (_) { return false; } })(),
  databaseNamePresent: effective.databaseNamePresent,
  usernamePresent: effective.usernamePresent,
  category: effective.category,
  credentialEncodingRepairApplied: repair.fixed,
  incidentIdentifierRole: role,
}, null, 2));
