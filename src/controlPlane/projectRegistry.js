const ZJ_JOBS = Object.freeze([
  'ZJ_REPO_STATUS', 'ZJ_RELEASE_EVIDENCE', 'ZJ_LOCAL_VALIDATION',
  'ZJ_STAGING_HEALTHCHECK', 'ZJ_RELEASE_READINESS',
]);
const DAILY_JOBS = Object.freeze([
  'HEALTHCHECK', 'PROJECT_STATUS', 'GIT_STATUS', 'RUN_TESTS', 'RUN_TYPECHECK',
  'CODEX_TASK', 'DAILYSYSTEM_HEALTHCHECK', 'EXCEL_CONNECTIVITY_TEST',
  'EXCEL_SYNC_DIAGNOSTIC', 'EXCEL_RECONCILIATION_CHECK', 'EXCEL_HEALTHCHECK',
  'EXCEL_SNAPSHOT', 'EXCEL_BACKUP', 'EXCEL_RECONCILIATION', 'EXCEL_SYNC',
]);

const PROJECTS = Object.freeze({
  'daily-system': Object.freeze({
    id: 'daily-system', key: 'daily-system', name: 'DailySystem', displayName: 'DailySystem',
    enabled: true, environment: 'dev', environments: ['dev', 'production'],
    runnerProfile: 'daily-system', runnerRequirements: ['windows'],
    capabilities: ['CONTROL_PLANE_STATUS', 'LOCAL_REPO_STATUS', 'LOCAL_VALIDATION', 'RUNNER_JOB', 'EXCEL_OPERATIONS'],
    allowedJobTypes: DAILY_JOBS, healthCheckMode: 'runner', safeExternalEndpoints: [], metadata: {},
  }),
  zj: Object.freeze({
    id: 'zj', key: 'zj', name: 'ZJ', displayName: 'ZJ', enabled: true,
    environment: 'staging', environments: ['local', 'staging'], runnerProfile: 'zj',
    runnerRequirements: ['windows'],
    capabilities: ['CONTROL_PLANE_STATUS', 'LOCAL_REPO_STATUS', 'LOCAL_VALIDATION', 'REMOTE_HEALTH', 'RELEASE_READINESS', 'RUNNER_JOB', 'PROJECT_RECOVERY_STATUS'],
    allowedJobTypes: ZJ_JOBS, healthCheckMode: 'configured-staging-only', safeExternalEndpoints: [],
    metadata: { productionWorker: 'zj', productionD1: 'zj-db', stagingWorker: 'zj-staging', stagingD1: 'zj-staging-db' },
  }),
});

function registeredProject(id, overrides = {}) {
  const key = String(id || '').toLowerCase();
  const project = Object.hasOwn(PROJECTS, key) ? PROJECTS[key] : null;
  if (!project) return null;
  return { ...project, ...overrides, id: project.id, key: project.key,
    allowedJobTypes: [...project.allowedJobTypes], capabilities: [...project.capabilities] };
}

function projectAllows(project, type) {
  return Boolean(project?.enabled && project.allowedJobTypes?.includes(String(type || '').toUpperCase()));
}

module.exports = { PROJECTS, ZJ_JOBS, DAILY_JOBS, registeredProject, projectAllows };
