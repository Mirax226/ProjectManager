// Portable application boundary: no Node bot bootstrap or local execution imports.
const { buildProjectManagerAdminModel, buildRunnerDashboardModel, buildIncidentCenterModel, buildTypedOperationalJob } = require('./telegramOpsCenter');

function adminIds(env, store) {
  const bootstrap = String(env.TELEGRAM_ADMIN_USER_IDS || '').split(',').map((id) => id.trim()).filter(Boolean);
  return new Set([...bootstrap, ...(store.operationalConfig?.admin?.adminUserIds || [])].map(String));
}

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
    text += '\n\n🏠 Start: /start\n❤️ Health: /health PROJECT\n📁 Project Status: /project_status PROJECT\n📗 Excel Health: /excel_healthcheck PROJECT ASSET';
    keyboard = { inline_keyboard: [[{ text: '📊 Status', callback_data: 'status' }, { text: '🖥️ Runners', callback_data: 'runners' }], [{ text: '🚨 Incidents', callback_data: 'incidents' }, { text: '📋 Jobs', callback_data: 'jobs' }]] };
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
    text = job ? `${job.id} · ${job.projectId} · ${job.type} · ${job.status}` : 'Job not found.';
  } else if (['health', 'project_status', 'excel_healthcheck'].includes(command)) {
    const key = `telegram:${update.update_id}`;
    const inputJob = command === 'excel_healthcheck' ? buildTypedOperationalJob(command, projectId, assetId, key) : { ok: true, job: { projectId, type: command === 'health' ? 'HEALTHCHECK' : 'PROJECT_STATUS', payload: {}, idempotencyKey: key } };
    const created = inputJob.ok ? await store.createJob(inputJob.job, 'telegram-admin') : inputJob;
    text = created.ok ? `Queued ${created.job.type}\nJob: ${created.job.id}\nUse /job ${created.job.id} for the Runner result.` : 'Invalid project or diagnostic action.';
  } else {
    text = '🏠 /start · ⚙️ /admin\n📊 /status · 🖥️ /runners\n🚨 /incidents · 📋 /jobs · /job ID\n❤️ /health PROJECT\n📁 /project_status PROJECT\n📗 /excel_healthcheck PROJECT ASSET\nOther legacy actions are disabled.';
  }
  return { authorized: true, chatId: message.chat.id, text: text.slice(0, 4000), replyMarkup: keyboard, callbackId: callback?.id };
}

module.exports = { dispatchTelegramUpdate, adminIds };
