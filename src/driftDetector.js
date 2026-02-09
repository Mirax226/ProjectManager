function buildProjectSnapshot(project = {}, options = {}) {
  const deploy = project.deploy || {};
  const cron = Array.isArray(project.cronJobs) ? project.cronJobs.map((job) => `${job.name || ''}:${job.schedule || ''}`) : [];
  const envKeys = Array.isArray(options.envKeys) ? options.envKeys.slice().sort() : [];
  return {
    projectId: project.id,
    repoUrl: project.repoUrl || null,
    branch: project.baseBranch || null,
    workingDir: project.workingDir || null,
    deploy: {
      startCommand: deploy.startCommand || project.startCommand || null,
      testCommand: deploy.testCommand || project.testCommand || null,
      diagnosticCommand: deploy.diagnosticCommand || project.diagnosticCommand || null,
      healthPath: deploy.healthPath || project.healthPath || null,
      portMode: deploy.portMode || project.portMode || null,
    },
    cron,
    envKeys,
    dbSummary: {
      primary: Boolean(project.databaseUrl || options.primaryDsn),
      secondary: Boolean(project.secondaryDatabaseUrl || options.secondaryDsn),
    },
  };
}

function diffArrays(before = [], after = []) {
  const b = new Set(before);
  const a = new Set(after);
  return {
    added: after.filter((item) => !b.has(item)),
    removed: before.filter((item) => !a.has(item)),
  };
}

function calculateDrift(baseline, current) {
  if (!baseline) {
    return { changed: true, summary: ['No baseline set.'], impact: ['Cannot calculate drift until baseline exists.'], nextSteps: ['Set baseline now.'] };
  }
  const summary = [];
  if (baseline.repoUrl !== current.repoUrl) summary.push(`Repo changed: ${baseline.repoUrl || '-'} -> ${current.repoUrl || '-'}`);
  if (baseline.branch !== current.branch) summary.push(`Branch changed: ${baseline.branch || '-'} -> ${current.branch || '-'}`);
  if (baseline.workingDir !== current.workingDir) summary.push(`Working dir changed: ${baseline.workingDir || '-'} -> ${current.workingDir || '-'}`);

  const cron = diffArrays(baseline.cron, current.cron);
  if (cron.added.length || cron.removed.length) {
    if (cron.added.length) summary.push(`Cron added: ${cron.added.join(', ')}`);
    if (cron.removed.length) summary.push(`Cron removed: ${cron.removed.join(', ')}`);
  }

  const env = diffArrays(baseline.envKeys, current.envKeys);
  if (env.added.length) summary.push(`Env keys added: ${env.added.join(', ')}`);
  if (env.removed.length) summary.push(`Env keys removed: ${env.removed.join(', ')}`);

  if (baseline.deploy.healthPath !== current.deploy.healthPath) summary.push('Health check path changed.');
  if (baseline.deploy.startCommand !== current.deploy.startCommand) summary.push('Start command changed.');

  const impact = [];
  if (summary.some((line) => line.includes('Branch changed') || line.includes('Repo changed'))) impact.push('Deploy source changed; verify build and runtime behavior.');
  if (summary.some((line) => line.includes('Env keys'))) impact.push('Environment mismatch may break startup or auth flows.');
  if (summary.some((line) => line.includes('Health'))) impact.push('Health probes may fail and trigger false outage alerts.');

  const nextSteps = ['Review changes with project owner.', 'If expected, set a new baseline.', 'If unexpected, revert risky config first.'];
  return { changed: summary.length > 0, summary, impact, nextSteps };
}

module.exports = { buildProjectSnapshot, calculateDrift };
