const GATES = Object.freeze([
  'LOCAL_REPO', 'LOCAL_TESTS', 'LINT', 'TYPECHECK', 'BUILD', 'SECURITY',
  'STAGING_WORKER', 'STAGING_D1', 'STAGING_HEALTH', 'STAGING_TELEGRAM',
  'CI', 'PRODUCTION_ISOLATION',
]);
function readiness(jobs) {
  const ordered = jobs.filter((job) => job.projectId === 'zj' && job.type !== 'ZJ_RELEASE_READINESS').sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  const recent = (type) => ordered.find((job) => job.type === type);
  const repoJob = recent('ZJ_REPO_STATUS'); const validationJob = recent('ZJ_LOCAL_VALIDATION');
  const evidenceJob = recent('ZJ_RELEASE_EVIDENCE'); const healthJob = recent('ZJ_STAGING_HEALTHCHECK');
  const completed = (job) => ['SUCCEEDED', 'FAILED'].includes(job?.status) ? job.result : null;
  const repo = completed(repoJob); const validation = completed(validationJob);
  const evidence = completed(evidenceJob); const health = completed(healthJob);
  const gates = Object.fromEntries(GATES.map((gate) => [gate, 'UNKNOWN']));
  if (repo) gates.LOCAL_REPO = repo.state === 'CLEAN_SYNCED' ? 'PASS' : repo.state === 'ERROR' ? 'FAIL' : repo.state === 'DIRTY' ? 'FAIL' : 'PENDING';
  else if (repoJob) gates.LOCAL_REPO = 'PENDING';
  const sameCommit = Boolean(repo?.head && validation?.commit === repo.head);
  for (const [gate, id] of [['LOCAL_TESTS', 'tests'], ['LINT', 'lint'], ['TYPECHECK', 'typecheck'], ['BUILD', 'build']]) {
    const command = sameCommit && validation?.commands?.find((entry) => entry.commandId === id);
    if (command && ['PASS', 'FAIL', 'PENDING', 'UNKNOWN', 'NOT_APPLICABLE'].includes(command.status)) gates[gate] = command.status;
    else if (validationJob && !validation) gates[gate] = 'PENDING';
  }
  const security = ['credential_scan', 'dependency_audit'].map((id) => sameCommit && validation?.commands?.find((entry) => entry.commandId === id)?.status || 'UNKNOWN');
  if (security.every((status) => status === 'PASS')) gates.SECURITY = 'PASS';
  else if (security.includes('FAIL')) gates.SECURITY = 'FAIL';
  else if (validationJob && !validation) gates.SECURITY = 'PENDING';
  if (evidence) {
    gates.STAGING_WORKER = evidence.stagingWorker === 'MISSING' ? 'PENDING' : 'UNKNOWN';
    gates.STAGING_D1 = evidence.stagingD1 === 'MISSING' ? 'PENDING' : 'UNKNOWN';
    gates.CI = evidence.ci === 'MISSING' ? 'PENDING' : 'UNKNOWN';
  }
  if (health) { gates.STAGING_HEALTH = health.status === 'HEALTHY' ? 'PASS' : health.status === 'PENDING' ? 'PENDING' : 'FAIL'; if (health.status === 'HEALTHY') gates.STAGING_WORKER = 'PASS'; }
  else if (healthJob) gates.STAGING_HEALTH = 'PENDING';
  gates.STAGING_TELEGRAM = 'PENDING'; // No dedicated staging Telegram acceptance is recorded in PJ.
  gates.PRODUCTION_ISOLATION = 'PASS'; // The ZJ job allowlist contains no production mutation action.
  return { projectId: 'zj', gates, status: Object.values(gates).includes('FAIL') ? 'FAIL' : Object.values(gates).every((value) => ['PASS', 'NOT_APPLICABLE'].includes(value)) ? 'PASS' : 'PENDING', timestamp: ordered[0]?.updatedAt || null, evaluatedAt: new Date().toISOString() };
}
module.exports = { GATES, readiness };
