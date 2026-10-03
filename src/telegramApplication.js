// Portable application boundary: no Node bot bootstrap or local execution imports.
const { buildProjectManagerAdminModel, buildRunnerDashboardModel, buildIncidentCenterModel, buildTypedOperationalJob } = require('./telegramOpsCenter');
const { readiness } = require('./controlPlane/zjReadiness');

function adminIds(env, store) {
  const bootstrap = String(env.TELEGRAM_ADMIN_USER_IDS || '').split(',').map((id) => id.trim()).filter(Boolean);
  return new Set([...bootstrap, ...(store.operationalConfig?.admin?.adminUserIds || [])].map(String));
}

const projectLabel = (project) => `${project?.id === 'zj' ? '🎓' : project?.id === 'daily-system' ? '📘' : '📁'} ${project?.displayName || project?.name || project?.id}`;
const backToProjects = { text: '⬅️ Back', callback_data: 'projects' };
const homeButton = { text: '🏠 Home', callback_data: 'start' };

async function dispatchTelegramUpdate(update, { store, env }) {
  const callback = update.callback_query;
  const message = callback?.message || update.message;
  const sender = callback?.from || message?.from;
  if (!adminIds(env, store).has(String(sender?.id)) || message?.chat?.type !== 'private' || String(message.chat.id) !== String(sender.id)) return { authorized: false };
  const input = String(callback?.data || message?.text || '').trim();
  const [command, projectId, assetId] = input.replace(/^\//, '').split(/\s+/);
  let text;
  let keyboard;
  if (['start', 'admin'].includes(command)) {
    text = buildProjectManagerAdminModel({ config: store.operationalConfig, jobs: await store.listJobs(), archives: [...store.archives.values()] }).text;
    keyboard = { inline_keyboard: [[{ text: '📊 Status', callback_data: 'status' }, { text: '🖥️ Runners', callback_data: 'runners' }], [{ text: '🚨 Incidents', callback_data: 'incidents' }, { text: '📋 Jobs', callback_data: 'jobs' }], [{ text: '📁 Projects', callback_data: 'projects' }]] };
  } else if (command === 'status') {
    const state = await store.status();
    text = `📊 ProjectManager · Cloudflare/D1\n📁 Projects: ${state.projects.length}\n🖥️ Runners: ${state.runners.length}\n🚨 Incidents: ${state.openAlerts.length}`;
  } else if (command === 'runners') {
    text = buildRunnerDashboardModel((await store.status()).runners, await store.listJobs()).text;
  } else if (command === 'incidents') {
    text = buildIncidentCenterModel((await store.status()).openAlerts).text;
  } else if (command === 'jobs') {
    text = '📋 Jobs\n' + ((await store.listJobs()).slice(0, 10).map((job) => `${job.id} · ${job.projectId} · ${job.type} · ${job.status}`).join('\n') || 'No jobs.');
  } else if (command === 'job' && projectId) {
    const job = await store.getJob(projectId);
    text = job ? `${job.id} · ${job.projectId} · ${job.type} · ${job.status}${job.result ? `\n${JSON.stringify(job.result).slice(0, 2500)}` : ''}` : 'Job not found.';
  } else if (command === 'projects') {
    const projects = await store.listProjects();
    text = `📁 Projects\n${projects.map((project) => `${projectLabel(project)}: ${project.enabled === false ? 'DISABLED' : 'ENABLED'}`).join('\n')}`;
    keyboard = { inline_keyboard: [...projects.filter((project) => project.enabled !== false).map((project) => [{ text: projectLabel(project), callback_data: `project ${project.id}` }]), [homeButton]] };
  } else if (command === 'project' && projectId) {
    const project = await store.getProject(projectId);
    text = project ? `${projectLabel(project)}\nEnvironment: ${project.environment}\nCapabilities: ${project.capabilities.join(', ')}` : 'Unknown project.';
    if (project?.id === 'zj') keyboard = { inline_keyboard: [[{ text: '📊 Status', callback_data: 'zj_status' }, { text: '🗂 Repository', callback_data: 'zj_repo' }], [{ text: '🧾 Release Evidence', callback_data: 'zj_release' }, { text: '✅ Release Readiness', callback_data: 'zj_readiness' }], [{ text: '🧪 Local Validation', callback_data: 'zj_validate' }, { text: '🩺 Staging Health', callback_data: 'zj_staging' }], [{ text: '🕘 Last Jobs', callback_data: 'zj_jobs' }], [backToProjects, homeButton]] };
    if (project?.id === 'daily-system') keyboard = { inline_keyboard: [[{ text: '📊 Status', callback_data: 'project_status daily-system' }, { text: '🩺 Health', callback_data: 'health daily-system' }], [{ text: '🧾 Jobs', callback_data: 'jobs' }], [backToProjects, homeButton]] };
  } else if (command === 'zj_status') {
    const jobs = await store.listJobs('zj'); const summary = readiness(jobs);
    const repo = jobs.find((job) => job.type === 'ZJ_REPO_STATUS')?.result;
    text = `🎓 ZJ\n🗂 Repository: ${repo?.state || 'UNKNOWN'}\n🌿 Branch: ${repo?.branch || 'UNKNOWN'}\n🔖 Commit: ${repo?.head?.slice(0, 8) || 'UNKNOWN'}\n🧪 Validation: ${summary.gates.LOCAL_TESTS}\n🚦 Release readiness: ${summary.status}\n🩺 Staging: ${summary.gates.STAGING_HEALTH}\n🕘 Last check: ${summary.timestamp || 'UNKNOWN'}`;
    keyboard = { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'zj_status' }], [{ text: '⬅️ Back', callback_data: 'project zj' }, homeButton]] };
  } else if (command === 'zj_jobs') {
    text = '🕘 ZJ Last Jobs\n' + ((await store.listJobs('zj')).slice(0, 10).map((job) => `${job.id} · ${job.type} · ${job.status}`).join('\n') || 'No jobs.');
    keyboard = { inline_keyboard: [[{ text: '🔄 Refresh', callback_data: 'zj_jobs' }], [{ text: '⬅️ Back', callback_data: 'project zj' }, homeButton]] };
  } else if (['zj_repo', 'zj_release', 'zj_validate', 'zj_staging', 'zj_readiness'].includes(command)) {
    const type = { zj_repo: 'ZJ_REPO_STATUS', zj_release: 'ZJ_RELEASE_EVIDENCE', zj_validate: 'ZJ_LOCAL_VALIDATION', zj_staging: 'ZJ_STAGING_HEALTHCHECK', zj_readiness: 'ZJ_RELEASE_READINESS' }[command];
    const created = await store.createJob({ projectId: 'zj', type, payload: {}, idempotencyKey: `telegram:${update.update_id}` }, 'telegram-admin');
    text = created.ok ? `⚙️ ${created.job.status === 'SUCCEEDED' ? 'Ready' : 'Queued'} ${type}\nJob: ${created.job.id}\nUse /job ${created.job.id} for the result.` : 'ZJ action is unavailable.';
    keyboard = { inline_keyboard: [[{ text: '⬅️ Back', callback_data: 'project zj' }, homeButton]] };
  } else if (['health', 'project_status', 'excel_healthcheck'].includes(command)) {
    const key = `telegram:${update.update_id}`;
    const inputJob = command === 'excel_healthcheck' ? buildTypedOperationalJob(command, projectId, assetId, key) : { ok: true, job: { projectId, type: command === 'health' ? 'HEALTHCHECK' : 'PROJECT_STATUS', payload: {}, idempotencyKey: key } };
    const created = inputJob.ok ? await store.createJob(inputJob.job, 'telegram-admin') : inputJob;
    text = created.ok ? `⚙️ Queued ${created.job.type}\nJob: ${created.job.id}\nUse /job ${created.job.id} for the Runner result.` : 'Invalid project or diagnostic action.';
    if (projectId === 'daily-system') keyboard = { inline_keyboard: [[{ text: '⬅️ Back', callback_data: 'project daily-system' }, homeButton]] };
  } else {
    text = '🏠 /start · ⚙️ /admin\n📊 /status · 📁 /projects · /project ID\n🖥️ /runners · 🚨 /incidents · 📋 /jobs · /job ID\n❤️ /health PROJECT · /project_status PROJECT\n📗 /excel_healthcheck PROJECT ASSET\nZJ: /zj_status · /zj_repo · /zj_release · /zj_validate · /zj_staging · /zj_readiness · /zj_jobs\nOther legacy actions are disabled.';
  }
  return { authorized: true, chatId: message.chat.id, text: text.slice(0, 4000), replyMarkup: keyboard, callbackId: callback?.id };
}

module.exports = { dispatchTelegramUpdate, adminIds };
