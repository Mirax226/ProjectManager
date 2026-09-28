const SAFE_ACTIONS = Object.freeze(['details', 'health', 'timeline', 'acknowledge', 'codex_task', 'excel_healthcheck', 'excel_snapshot', 'excel_backup', 'excel_reconciliation']);
const TYPED_OPERATION_TYPES = Object.freeze({ excel_healthcheck: 'EXCEL_HEALTHCHECK', excel_snapshot: 'EXCEL_SNAPSHOT', excel_backup: 'EXCEL_BACKUP', excel_reconciliation: 'EXCEL_RECONCILIATION' });

function truncate(value, limit) {
  const text = String(value == null ? '' : value);
  return text.length <= limit ? text : `${text.slice(0, limit - 1)}…`;
}

function sanitizeMessage(value) {
  return truncate(String(value == null ? '' : value)
    .replace(/((?:token|secret|password|authorization|api[-_]?key|cookie|dsn|private[-_]?key)\s*[:=]\s*)[^\s,;]+/gi, '$1[REDACTED]')
    .replace(/(postgres(?:ql)?:\/\/)[^\s@]+:[^\s@]+@/gi, '$1[REDACTED]@'), 500);
}

function formatTime(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? truncate(value || '-', 40) : date.toISOString();
}

function normalizeIncident(row = {}) {
  const meta = row.meta_json && typeof row.meta_json === 'object' ? row.meta_json : {};
  return {
    id: String(row.id || row.fingerprint || ''),
    project: String(row.projectId || row.project_id || meta.projectId || meta.project_id || 'global'),
    environment: String(row.environment || meta.environment || meta.env || '-'),
    severity: String(row.level || row.severity || 'info').toUpperCase(),
    category: String(row.category || 'GENERAL').toUpperCase(),
    component: String(meta.component || meta.service || meta.source || row.source || '-'),
    firstSeen: formatTime(row.first_seen_at || row.created_at || row.ts),
    lastSeen: formatTime(row.last_seen_at || row.created_at || row.ts),
    occurrences: Math.max(1, Number(row.occurrence_count) || 1),
    message: sanitizeMessage(row.message_short || row.message || row.message_full || ''),
    status: String(row.status || 'open').toLowerCase(),
  };
}

function buildIncidentCenterModel(rows = [], status = 'open') {
  const incidents = rows.map(normalizeIncident);
  const lines = [`🚨 Incident Center — ${String(status).toUpperCase()}`];
  if (!incidents.length) lines.push('', 'No incidents in this state.');
  incidents.forEach((incident, index) => {
    lines.push('', `${index + 1}. [${incident.severity}] ${incident.category}`, `Project: ${incident.project} · Env: ${incident.environment}`, `Component: ${incident.component}`, `First: ${incident.firstSeen}`, `Last: ${incident.lastSeen}`, `Occurrences: ${incident.occurrences}`, `Message: ${incident.message || '-'}`);
  });
  return { text: lines.join('\n'), incidents };
}

function buildIncidentDetailModel(row) {
  const incident = normalizeIncident(row);
  return {
    text: [
      `🚨 Incident Details — ${incident.category}`,
      `Project: ${incident.project}`,
      `Environment: ${incident.environment}`,
      `Severity: ${incident.severity}`,
      `Component: ${incident.component}`,
      `First seen: ${incident.firstSeen}`,
      `Last seen: ${incident.lastSeen}`,
      `Occurrences: ${incident.occurrences}`,
      `Status: ${incident.status}`,
      `Message: ${incident.message || '-'}`,
    ].join('\n'),
    incident,
  };
}

function buildRunnerDashboardModel(runners = [], jobs = []) {
  const lines = ['🖥 Runner Dashboard'];
  if (!runners.length) lines.push('', 'No runner heartbeat data available.');
  runners.forEach((runner) => {
    const recent = jobs.filter((job) => job.projectId === runner.projectId).slice(0, 3);
    lines.push('', `Runner: ${truncate(runner.runnerId || '-', 80)}`, `Status: ${String(runner.status || 'UNKNOWN').toUpperCase()}`, `Last heartbeat: ${formatTime(runner.lastSeenAt)}`, `Project: ${truncate(runner.projectId || '-', 80)}`, `Recent jobs: ${recent.length ? recent.map((job) => `${job.type || '-'}:${job.status || '-'}`).join(', ') : '-'}`);
  });
  return { text: lines.join('\n') };
}

function buildProjectManagerAdminModel(input = {}) {
  const lines = ['⚙️ ProjectManager Admin'];
  const config = input.config || {};
  const archive = config.admin?.archive || input.archive || {};
  lines.push('', `Archive destination: ${truncate(archive.label || 'Not configured', 100)}`, `Archive channel: ${archive.channelId ? '[configured]' : 'Not configured'}`);
  const projects = Array.isArray(config.projects) ? config.projects : [];
  lines.push('', 'Projects:');
  projects.slice(0, 20).forEach((project) => lines.push(`• ${truncate(project.projectId, 80)} — ${project.enabled === false ? 'DISABLED' : 'ENABLED'} · ${project.environment || '-'}`));
  const assets = Array.isArray(config.excelAssets) ? config.excelAssets : [];
  const profile = Array.isArray(config.desktopProfiles) ? config.desktopProfiles[0] : null;
  lines.push('', `Desktop Profile: ${truncate(profile?.id || 'Not configured', 80)} · ${profile?.runnerType || 'Unknown runner'} · ${profile?.excelVersion || 'Unknown Excel'}`);
  lines.push('', `Excel assets: ${assets.length}`);
  assets.slice(0, 10).forEach((asset) => lines.push(`• ${truncate(asset.assetId || asset.filename, 80)} · ${asset.project || '-'} · ${asset.status || 'UNKNOWN'}`));
  const providers = Array.isArray(config.providers) ? config.providers : [];
  lines.push(`Providers: ${providers.length}`);
  providers.slice(0, 10).forEach((provider) => lines.push(`• ${truncate(provider.providerId || provider.name, 80)} · ${provider.health || 'UNKNOWN'} · ${provider.enabled === false ? 'DISABLED' : 'ENABLED'}`));
  lines.push(`Recent archives: ${Array.isArray(input.archives) ? input.archives.length : 0}`, `Backup status: ${truncate(input.backupStatus || 'UNKNOWN', 80)}`);
  lines.push(`Excel health: ${truncate(input.excelHealth || 'UNKNOWN', 80)}`, `Reconciliation: ${truncate(input.reconciliationStatus || 'UNKNOWN', 80)}`, `Recent jobs: ${Array.isArray(input.jobs) ? input.jobs.length : 0}`);
  const daily = input.dailySystemHealth || input.dailySystem || {};
  if (daily && typeof daily === 'object') lines.push(`DailySystem health: ${sanitizeMessage(truncate(daily.status || daily.health || 'UNKNOWN', 80))}`, `DailySystem correlation: ${truncate(daily.correlationId || '-', 120)}`);
  const excel = input.excelAssetsByName || input.excelDesktop || {};
  if (excel && typeof excel === 'object') {
    for (const name of ['Mirax', 'Gozareshkar']) {
      const item = excel[name] || excel[name.toLowerCase()] || {};
      if (Object.keys(item).length) lines.push(`${name} health: ${sanitizeMessage(truncate(item.status || item.health || 'UNKNOWN', 80))}`);
    }
  }
  const reconciliation = input.latestReconciliation || {};
  if (reconciliation && typeof reconciliation === 'object' && Object.keys(reconciliation).length) lines.push(`Latest reconciliation: ${sanitizeMessage(truncate(`${reconciliation.status || 'UNKNOWN'} · mismatches=${Number(reconciliation.mismatchCount) || 0} · errors=${Number(reconciliation.errorCount) || 0}`, 160))}`);
  const latestJob = input.latestExcelJob || {};
  if (latestJob && typeof latestJob === 'object' && Object.keys(latestJob).length) lines.push(`Latest Excel job: ${sanitizeMessage(truncate(`${latestJob.type || 'UNKNOWN'} · ${latestJob.status || 'UNKNOWN'}`, 160))}`);
  if (input.runnerStatus || input.runner) lines.push(`Runner status: ${sanitizeMessage(truncate(input.runnerStatus || input.runner?.status || 'UNKNOWN', 80))}`);
  return { text: lines.join('\n') };
}

function buildTypedOperationalJob(action, projectId, assetId, operationKey) {
  const normalized = String(action || '').toLowerCase(); const type = TYPED_OPERATION_TYPES[normalized];
  if (!type || !/^[a-z0-9][a-z0-9._-]{0,79}$/i.test(String(projectId || ''))) return { ok: false, error: 'unsupported operational action' };
  if (!assetId || !/^[a-z0-9][a-z0-9._-]{0,159}$/i.test(String(assetId))) return { ok: false, error: 'assetId is required' };
  if (['excel_snapshot', 'excel_backup'].includes(normalized) && !operationKey) return { ok: false, error: 'operationKey is required' };
  return { ok: true, job: { type, projectId: String(projectId).toLowerCase(), idempotencyKey: operationKey || null, payload: { assetId: String(assetId), mode: normalized === 'excel_reconciliation' ? 'read_only' : undefined } } };
}

function isSafeOpsAction(role, action) {
  return ['owner', 'admin'].includes(String(role || '').toLowerCase()) && SAFE_ACTIONS.includes(String(action || '').toLowerCase());
}

module.exports = {
  SAFE_ACTIONS,
  sanitizeMessage,
  normalizeIncident,
  buildIncidentCenterModel,
  buildIncidentDetailModel,
  buildRunnerDashboardModel,
  buildProjectManagerAdminModel,
  buildTypedOperationalJob,
  isSafeOpsAction,
};
