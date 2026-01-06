require('dotenv').config();

const http = require('http');
const https = require('https');
const { Bot, InlineKeyboard, Keyboard } = require('grammy');
const { Pool } = require('pg');

const { loadProjects, saveProjects, findProjectById } = require('./projectsStore');
const { setUserState, getUserState, clearUserState } = require('./state');
const {
  prepareRepository,
  createWorkingBranch,
  applyPatchToRepo,
  configureGitIdentity,
  commitAndPush,
  DEFAULT_BASE_BRANCH,
  fetchDryRun,
  getRepoInfo,
  getGithubToken,
  getDefaultWorkingDir,
  makePatchBranchName,
  slugifyProjectId,
} = require('./gitUtils');
const { createPullRequest, measureGithubLatency } = require('./githubUtils');
const {
  loadGlobalSettings,
  saveGlobalSettings,
  loadCronSettings,
} = require('./settingsStore');
const {
  loadSupabaseConnections,
  saveSupabaseConnections,
  findSupabaseConnection,
} = require('./supabaseConnectionsStore');
const { runCommandInProject } = require('./shellUtils');
const { LOG_LEVELS, normalizeLogLevel } = require('./logLevels');
const { configureSelfLogger, forwardSelfLog } = require('./logger');
const {
  CRON_API_KEY,
  listJobs,
  getJobDetails,
  createJob,
  updateJob,
  deleteJob,
} = require('./src/cronClient');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID;

if (!BOT_TOKEN) {
  throw new Error('BOT_TOKEN is required');
}

if (!ADMIN_TELEGRAM_ID) {
  throw new Error('ADMIN_TELEGRAM_ID is required');
}

const bot = new Bot(BOT_TOKEN);
const supabasePools = new Map();
let botStarted = false;
let botRetryTimeout = null;
const userState = new Map();
let configStatusPool = null;
const patchSessions = new Map();

configureSelfLogger({
  bot,
  adminId: ADMIN_TELEGRAM_ID,
  loadSettings: loadGlobalSettings,
});

const runtimeStatus = {
  configDbOk: false,
  configDbError: null,
};

function normalizeLogLevels(levels) {
  if (!Array.isArray(levels)) return [];
  return levels.map((level) => normalizeLogLevel(level)).filter(Boolean);
}

function getEffectiveProjectLogForwarding(project) {
  const forwarding = project?.logForwarding || {};
  const levels = normalizeLogLevels(forwarding.levels);
  return {
    enabled: forwarding.enabled === true,
    levels: levels.length ? levels : ['error'],
    targetChatId: forwarding.targetChatId,
  };
}

function getEffectiveSelfLogForwarding(settings) {
  const forwarding = settings?.selfLogForwarding || {};
  const levels = normalizeLogLevels(forwarding.levels);
  return {
    enabled: forwarding.enabled === true,
    levels: levels.length ? levels : ['error'],
    targetChatId: forwarding.targetChatId || ADMIN_TELEGRAM_ID,
  };
}

function getCronNotificationLevels(project) {
  if (!project || !Array.isArray(project.cronNotificationsLevels)) {
    return ['info', 'warning', 'error'];
  }
  const normalized = project.cronNotificationsLevels
    .map((level) => String(level).toLowerCase())
    .filter((level) => ['info', 'warning', 'error'].includes(level));
  return normalized.length ? normalized : ['info', 'warning', 'error'];
}

async function getEffectiveCronSettings() {
  const settings = await loadCronSettings();
  return settings || { enabled: true, defaultTimezone: 'UTC' };
}

async function renderOrEdit(ctx, text, extra) {
  if (ctx.callbackQuery) {
    try {
      return await ctx.editMessageText(text, extra);
    } catch (err) {
      console.error('[UI] editMessageText failed, fallback to reply', err);
      return ctx.reply(text, extra);
    }
  }
  return ctx.reply(text, extra);
}

function resetUserState(ctx) {
  if (!ctx?.from?.id) return;
  userState.delete(ctx.from.id);
  clearUserState(ctx.from.id);
}

function getPatchSession(userId) {
  return patchSessions.get(userId);
}

function startPatchSession(userId, projectId) {
  patchSessions.set(userId, { projectId, buffer: '' });
}

function clearPatchSession(userId) {
  patchSessions.delete(userId);
}

function appendPatchChunk(session, chunk) {
  const text = chunk || '';
  if (!text) return 0;
  if (session.buffer && !session.buffer.endsWith('\n')) {
    session.buffer += '\n';
  }
  session.buffer += text;
  if (!text.endsWith('\n')) {
    session.buffer += '\n';
  }
  return text.length;
}

async function renderMainMenu(ctx) {
  await renderOrEdit(ctx, 'Main menu:', { reply_markup: mainKeyboard });
}

function buildCancelKeyboard() {
  return new InlineKeyboard().text('❌ Cancel', 'cancel_input');
}

function buildBackKeyboard(callbackData, label = '⬅️ Back') {
  return new InlineKeyboard().text(label, callbackData);
}

function buildPatchSessionKeyboard() {
  return new InlineKeyboard()
    .text('✅ Patch completed', 'patch:finish')
    .row()
    .text('❌ Cancel', 'patch:cancel');
}

function getMessageTargetFromCtx(ctx) {
  const message = ctx.callbackQuery?.message;
  if (!message) return null;
  return { chatId: message.chat.id, messageId: message.message_id };
}

async function checkConfigDbStatus() {
  const dsn = process.env.PATH_APPLIER_CONFIG_DSN;
  if (!dsn) {
    return { ok: false, message: 'not configured' };
  }
  try {
    if (!configStatusPool) {
      configStatusPool = new Pool({
        connectionString: dsn,
        max: 1,
        idleTimeoutMillis: 30_000,
      });
    }
    await configStatusPool.query('SELECT 1');
    return { ok: true, message: 'OK' };
  } catch (error) {
    return { ok: false, message: error.message || 'error' };
  }
}

async function getCronStatusLine(cronSettings) {
  if (!cronSettings?.enabled) {
    return 'Cron: disabled (settings).';
  }
  if (!CRON_API_KEY) {
    return 'Cron: disabled (no API key).';
  }
  try {
    await listJobs();
    return 'Cron: ✅ API OK.';
  } catch (error) {
    return `Cron: ⚠️ API error: ${truncateText(error.message, 80)}`;
  }
}

async function testConfigDbConnection() {
  const status = await checkConfigDbStatus();
  if (status.ok) {
    runtimeStatus.configDbOk = true;
    runtimeStatus.configDbError = null;
    console.log('Config DB: OK');
    return;
  }
  runtimeStatus.configDbOk = false;
  runtimeStatus.configDbError = status.message || 'see logs';
  if (status.message === 'not configured') {
    console.warn('Config DB: PATH_APPLIER_CONFIG_DSN not set.');
    return;
  }
  console.error('Config DB connection failed', status.message);
  await forwardSelfLog('error', 'Config DB connection failed', {
    context: { error: status.message },
  });
}

const mainKeyboard = new Keyboard()
  .text('📁 Projects')
  .row()
  .text('🏭 Data Center')
  .row()
  .text('⚙️ Settings')
  .resized();

bot.use(async (ctx, next) => {
  if (!ctx.from) return;
  if (String(ctx.from.id) !== String(ADMIN_TELEGRAM_ID)) {
    if (ctx.updateType === 'message' || ctx.updateType === 'callback_query') {
      await ctx.reply('Unauthorized');
    }
    return;
  }
  return next();
});

bot.on('message:text', async (ctx, next) => {
  const session = getPatchSession(ctx.from.id);
  if (!session) {
    return next();
  }
  const chunkLength = appendPatchChunk(session, ctx.message.text);
  await ctx.reply(
    `Patch chunk received (${chunkLength} chars).\nSend more, or press ‘✅ Patch completed’.`,
    { reply_markup: buildPatchSessionKeyboard() },
  );
});

bot.on('message:document', async (ctx, next) => {
  const session = getPatchSession(ctx.from.id);
  if (!session) {
    return next();
  }
  const doc = ctx.message.document;
  const fileName = doc?.file_name || '';
  if (!fileName.endsWith('.patch') && !fileName.endsWith('.diff')) {
    await ctx.reply('Unsupported file type; only .patch/.diff are accepted in patch mode.');
    return;
  }
  const fileContents = await downloadTelegramFile(ctx, doc.file_id);
  const chunkLength = appendPatchChunk(session, fileContents);
  await ctx.reply(
    `Patch file received (${chunkLength} chars).\nPress ‘✅ Patch completed’ when ready.`,
    { reply_markup: buildPatchSessionKeyboard() },
  );
});

bot.on('message:text', async (ctx, next) => {
  const state = userState.get(ctx.from.id);
  if (!state || state.mode !== 'create-project') {
    return next();
  }
  await handleProjectWizardInput(ctx, state);
});

bot.on('message', async (ctx, next) => {
  const state = getUserState(ctx.from?.id);
  if (state) {
    await handleStatefulMessage(ctx, state);
    return;
  }
  return next();
});

bot.command('start', async (ctx) => {
  resetUserState(ctx);
  clearPatchSession(ctx.from.id);
  await renderMainMenu(ctx);
});

bot.hears('📁 Projects', async (ctx) => {
  resetUserState(ctx);
  await renderProjectsList(ctx);
});

bot.hears('🏭 Data Center', async (ctx) => {
  resetUserState(ctx);
  await renderDataCenterMenu(ctx);
});

bot.hears('⚙️ Settings', async (ctx) => {
  resetUserState(ctx);
  await renderGlobalSettings(ctx);
});

bot.callbackQuery('cancel_input', async (ctx) => {
  const state = getUserState(ctx.from.id);
  const wizardState = userState.get(ctx.from.id);
  let backTarget = 'main:back';

  if (state?.backCallback) {
    backTarget = state.backCallback;
  } else if (state?.projectId) {
    backTarget = `proj:open:${state.projectId}`;
  } else if (state?.type === 'supabase_add') {
    backTarget = 'supabase:back';
  } else if (state?.type === 'supabase_console' && state.connectionId) {
    backTarget = `supabase:conn:${state.connectionId}`;
  } else if (state?.type === 'global_change_base') {
    backTarget = 'gsettings:menu';
  } else if (wizardState?.mode === 'create-project') {
    backTarget = wizardState.backCallback || 'proj:list';
  }

  resetUserState(ctx);
  clearPatchSession(ctx.from.id);
  await ctx.answerCallbackQuery();
  try {
    await ctx.editMessageText('Operation cancelled.', {
      reply_markup: buildBackKeyboard(backTarget),
    });
  } catch (error) {
    // Ignore edit failures (old message, etc.)
  }
});

bot.callbackQuery('patch:cancel', async (ctx) => {
  clearPatchSession(ctx.from.id);
  await ctx.answerCallbackQuery();
  try {
    await ctx.editMessageText('Patch input cancelled.');
  } catch (error) {
    // Ignore edit failures
  }
  await renderMainMenu(ctx);
});

bot.callbackQuery('patch:finish', async (ctx) => {
  const session = getPatchSession(ctx.from.id);
  if (!session || !session.buffer.trim()) {
    await ctx.answerCallbackQuery({ text: 'No patch text received yet.', show_alert: true });
    return;
  }
  clearPatchSession(ctx.from.id);
  await ctx.answerCallbackQuery();
  await handlePatchApplication(ctx, session.projectId, session.buffer);
});

bot.on('callback_query:data', async (ctx) => {
  const data = ctx.callbackQuery.data;
  if (data.startsWith('main:')) {
    await handleMainCallback(ctx, data);
    return;
  }
  if (data.startsWith('proj:')) {
    await handleProjectCallback(ctx, data);
    return;
  }
  if (data.startsWith('projlog:')) {
    await handleProjectLogCallback(ctx, data);
    return;
  }
  if (data.startsWith('projwiz:')) {
    await handleProjectWizardCallback(ctx, data);
    return;
  }
  if (data.startsWith('gsettings:')) {
    await handleGlobalSettingsCallback(ctx, data);
    return;
  }
  if (data.startsWith('supabase:')) {
    await handleSupabaseCallback(ctx, data);
    return;
  }
  if (data.startsWith('cron:')) {
    await handleCronCallback(ctx, data);
    return;
  }
  if (data.startsWith('projcron:')) {
    await handleProjectCronCallback(ctx, data);
  }
});

async function handleStatefulMessage(ctx, state) {
  switch (state.type) {
    case 'rename_project':
      await handleRenameProjectStep(ctx, state);
      break;
    case 'change_base_branch':
      await handleChangeBaseBranchStep(ctx, state);
      break;
    case 'edit_repo':
      await handleEditRepoStep(ctx, state);
      break;
    case 'edit_working_dir':
      await handleEditWorkingDirStep(ctx, state);
      break;
    case 'edit_github_token':
      await handleEditGithubTokenStep(ctx, state);
      break;
    case 'edit_command_input':
      await handleEditCommandInput(ctx, state);
      break;
    case 'edit_render_url':
      await handleEditRenderUrl(ctx, state);
      break;
    case 'edit_supabase':
      await handleEditSupabase(ctx, state);
      break;
    case 'global_change_base':
      await handleGlobalBaseChange(ctx, state);
      break;
    case 'supabase_console':
      await handleSupabaseConsoleMessage(ctx, state);
      break;
    case 'supabase_add':
      await handleSupabaseAddMessage(ctx, state);
      break;
    case 'cron_create_url':
    case 'cron_create_schedule':
    case 'cron_create_name':
      await handleCronCreateMessage(ctx, state);
      break;
    case 'cron_edit_schedule':
      await handleCronEditScheduleMessage(ctx, state);
      break;
    case 'cron_edit_url':
      await handleCronEditUrlMessage(ctx, state);
      break;
    case 'cron_edit_name':
      await handleCronEditNameMessage(ctx, state);
      break;
    case 'cron_edit_timezone':
      await handleCronEditTimezoneMessage(ctx, state);
      break;
    case 'projcron_keepalive_schedule':
    case 'projcron_keepalive_recreate':
    case 'projcron_deploy_schedule':
    case 'projcron_deploy_recreate':
      await handleProjectCronScheduleMessage(ctx, state);
      break;
    default:
      clearUserState(ctx.from.id);
      break;
  }
}

function buildProjectsKeyboard(projects, globalSettings) {
  const defaultId = globalSettings?.defaultProjectId;
  const rows = projects.map((project) => {
    const label = `${project.id === defaultId ? '⭐ ' : ''}${project.name || project.id}`;
    return [
      {
        text: label,
        callback_data: `proj:open:${project.id}`,
      },
    ];
  });

  rows.push([{ text: '➕ Add project', callback_data: 'proj:add' }]);
  rows.push([{ text: '⬅️ Back', callback_data: 'main:back' }]);
  return { inline_keyboard: rows };
}

async function renderProjectsList(ctx) {
  const projects = await loadProjects();
  const globalSettings = await loadGlobalSettings();
  if (!projects.length) {
    await renderOrEdit(ctx, 'No projects configured yet.', {
      reply_markup: buildProjectsKeyboard([], globalSettings),
    });
    return;
  }

  await renderOrEdit(ctx, 'Select a project:', {
    reply_markup: buildProjectsKeyboard(projects, globalSettings),
  });
}

async function handleMainCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, action] = data.split(':');
  if (action === 'back') {
    await renderMainMenu(ctx);
  }
}

async function handleProjectCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, action, projectId, extra] = data.split(':');

  if (action === 'add') {
    await startProjectWizard(ctx);
    return;
  }

  if (action === 'list') {
    await renderProjectsList(ctx);
    return;
  }

  const projects = await loadProjects();
  const project = projectId ? findProjectById(projects, projectId) : null;
  if (!project && !['confirm_delete', 'cancel_delete'].includes(action)) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  switch (action) {
    case 'open':
      await renderProjectSettings(ctx, projectId);
      break;
    case 'project_menu':
      await renderProjectMenu(ctx, projectId);
      break;
    case 'server_menu':
      await renderServerMenu(ctx, projectId);
      break;
    case 'apply_patch':
      startPatchSession(ctx.from.id, projectId);
      await ctx.reply(
        'Send the git patch as text (you can use multiple messages)\n' +
          'or attach a .patch / .diff file.\n' +
          'When you are done, press ‘✅ Patch completed’.\n' +
          'Or press ‘❌ Cancel’.',
        { reply_markup: buildPatchSessionKeyboard() },
      );
      break;
    case 'delete':
      await renderDeleteConfirmation(ctx, projectId);
      break;
    case 'confirm_delete':
      await deleteProject(ctx, projectId);
      break;
    case 'cancel_delete':
      await renderOrEdit(ctx, 'Deletion cancelled.');
      await renderProjectsList(ctx);
      break;
    case 'rename':
      setUserState(ctx.from.id, {
        type: 'rename_project',
        step: 1,
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply('Send the new project name.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'edit_repo':
      setUserState(ctx.from.id, {
        type: 'edit_repo',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply(
        'Send new GitHub repo as owner/repo (for example: Mirax226/daily-system-bot-v2).\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    case 'edit_workdir':
      setUserState(ctx.from.id, {
        type: 'edit_working_dir',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply(
        'Send new working directory path. Or send "-" to reset to default based on repo.\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    case 'edit_github_token':
      setUserState(ctx.from.id, {
        type: 'edit_github_token',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply(
        'Send the env key that contains the GitHub token (for example: GITHUB_TOKEN_DS). Or send "-" to use the default GITHUB_TOKEN.\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    case 'change_base':
      setUserState(ctx.from.id, {
        type: 'change_base_branch',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply('Send the new base branch.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'commands':
      await renderCommandsScreen(ctx, projectId);
      break;
    case 'cmd_edit':
      setUserState(ctx.from.id, {
        type: 'edit_command_input',
        projectId,
        field: extra,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply(`Send new value for ${extra}.\n(Or press Cancel)`, {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'cmd_clearall':
      await clearProjectCommands(projectId);
      await renderProjectSettings(ctx, projectId);
      break;
    case 'diagnostics':
      await runProjectDiagnostics(ctx, projectId);
      break;
    case 'render_menu':
      await renderRenderUrlsScreen(ctx, projectId);
      break;
    case 'render_ping':
      await pingRenderService(ctx, projectId);
      break;
    case 'render_keepalive_url':
      await showKeepAliveUrl(ctx, projectId);
      break;
    case 'render_deploy':
      await triggerRenderDeploy(ctx, projectId);
      break;
    case 'render_edit':
      setUserState(ctx.from.id, {
        type: 'edit_render_url',
        projectId,
        field: extra,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply(`Send new value for ${extra}.\n(Or press Cancel)`, {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'render_clear':
      await updateProjectField(projectId, extra, undefined);
      await renderProjectSettings(ctx, projectId);
      break;
    case 'supabase':
      await renderSupabaseScreen(ctx, projectId);
      break;
    case 'supabase_edit':
      setUserState(ctx.from.id, {
        type: 'edit_supabase',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply('Send new supabaseConnectionId.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'supabase_clear':
      await updateProjectField(projectId, 'supabaseConnectionId', undefined);
      await renderProjectSettings(ctx, projectId);
      break;
    case 'set_default':
      await setDefaultProject(projectId);
      await renderProjectSettings(ctx, projectId);
      break;
    case 'back':
      await renderProjectsList(ctx);
      break;
    default:
      break;
  }
}

async function handleProjectLogCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const parts = data.split(':');
  const action = parts[1];
  const level = action === 'level' ? parts[2] : null;
  const projectId = action === 'level' ? parts[3] : parts[2];

  if (!projectId) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  if (action === 'back') {
    await renderProjectSettings(ctx, projectId);
    return;
  }

  const projects = await loadProjects();
  const idx = projects.findIndex((project) => project.id === projectId);
  if (idx === -1) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  const project = projects[idx];
  const current = getEffectiveProjectLogForwarding(project);
  const updated = {
    ...project,
    logForwarding: {
      enabled: current.enabled,
      levels: [...current.levels],
      targetChatId: current.targetChatId,
    },
  };

  if (action === 'menu') {
    await renderProjectLogAlerts(ctx, projectId);
    return;
  }

  if (action === 'toggle') {
    const nextEnabled = !current.enabled;
    updated.logForwarding.enabled = nextEnabled;
    if (nextEnabled && !updated.logForwarding.levels.length) {
      updated.logForwarding.levels = ['error'];
    }
  }

  if (action === 'level') {
    const normalized = normalizeLogLevel(level);
    if (normalized) {
      const levels = new Set(updated.logForwarding.levels);
      if (levels.has(normalized)) {
        levels.delete(normalized);
      } else {
        levels.add(normalized);
      }
      updated.logForwarding.levels = Array.from(levels).filter((entry) =>
        LOG_LEVELS.includes(entry),
      );
      if (current.enabled && updated.logForwarding.levels.length === 0) {
        updated.logForwarding.levels = ['error'];
      }
    }
  }

  projects[idx] = updated;
  await saveProjects(projects);
  await renderProjectLogAlerts(ctx, projectId);
}

async function handleGlobalSettingsCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const parts = data.split(':');
  const action = parts[1];
  switch (action) {
    case 'bot_log_alerts':
      await renderSelfLogAlerts(ctx);
      break;
    case 'bot_log_toggle': {
      const settings = await loadGlobalSettings();
      const current = getEffectiveSelfLogForwarding(settings);
      const updated = {
        ...settings,
        selfLogForwarding: {
          enabled: !current.enabled,
          levels: [...current.levels],
          targetChatId: settings?.selfLogForwarding?.targetChatId,
        },
      };
      if (updated.selfLogForwarding.enabled && !updated.selfLogForwarding.levels.length) {
        updated.selfLogForwarding.levels = ['error'];
      }
      await saveGlobalSettings(updated);
      await renderSelfLogAlerts(ctx);
      break;
    }
    case 'bot_log_level': {
      const level = normalizeLogLevel(parts[2]);
      if (!level) {
        await renderSelfLogAlerts(ctx);
        break;
      }
      const settings = await loadGlobalSettings();
      const current = getEffectiveSelfLogForwarding(settings);
      const levels = new Set(current.levels);
      if (levels.has(level)) {
        levels.delete(level);
      } else {
        levels.add(level);
      }
      const updatedLevels = Array.from(levels).filter((entry) => LOG_LEVELS.includes(entry));
      const updated = {
        ...settings,
        selfLogForwarding: {
          enabled: current.enabled,
          levels: updatedLevels,
          targetChatId: settings?.selfLogForwarding?.targetChatId,
        },
      };
      if (current.enabled && updated.selfLogForwarding.levels.length === 0) {
        updated.selfLogForwarding.levels = ['error'];
      }
      await saveGlobalSettings(updated);
      await renderSelfLogAlerts(ctx);
      break;
    }
    case 'change_default_base':
      setUserState(ctx.from.id, { type: 'global_change_base' });
      await ctx.reply('Send new default base branch.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'ping_test':
      await runPingTest(ctx);
      break;
    case 'clear_default_base':
      await clearDefaultBaseBranch();
      await renderOrEdit(ctx, 'Default base branch cleared (using environment default).');
      await renderGlobalSettings(ctx);
      break;
    case 'clear_default_project':
      await clearDefaultProject();
      await renderOrEdit(ctx, 'Default project cleared.');
      await renderGlobalSettings(ctx);
      break;
    case 'menu':
      await renderGlobalSettings(ctx);
      break;
    case 'back':
      await renderMainMenu(ctx);
      break;
    default:
      break;
  }
}

async function handleSupabaseCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, action, connectionId] = data.split(':');
  switch (action) {
    case 'add':
      resetUserState(ctx);
      setUserState(ctx.from.id, {
        type: 'supabase_add',
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await ctx.reply('Send the connection as: id, name, envKey\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'back':
      resetUserState(ctx);
      await renderDataCenterMenu(ctx);
      break;
    case 'connections':
      resetUserState(ctx);
      await renderSupabaseConnectionsMenu(ctx);
      break;
    case 'conn':
      resetUserState(ctx);
      setUserState(ctx.from.id, { type: 'supabase_console', connectionId, mode: null });
      await renderSupabaseConnectionMenu(ctx, connectionId);
      break;
    case 'tables':
      resetUserState(ctx);
      setUserState(ctx.from.id, { type: 'supabase_console', connectionId, mode: null });
      await listSupabaseTables(ctx, connectionId);
      break;
    case 'sql':
      resetUserState(ctx);
      setUserState(ctx.from.id, { type: 'supabase_console', connectionId, mode: 'awaiting-sql' });
      await promptSupabaseSql(ctx, connectionId);
      break;
    default:
      break;
  }
}

async function handleCronCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, action, jobId] = data.split(':');

  switch (action) {
    case 'menu':
      await renderCronMenu(ctx);
      break;
    case 'list':
      await renderCronJobList(ctx);
      break;
    case 'create':
      if (!CRON_API_KEY) {
        await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_KEY missing).', {
          reply_markup: buildBackKeyboard('main:back'),
        });
        return;
      }
      {
        const cronSettings = await getEffectiveCronSettings();
        if (!cronSettings.enabled) {
          await renderOrEdit(ctx, 'Cron integration is disabled in settings.', {
            reply_markup: buildBackKeyboard('main:back'),
          });
          return;
        }
      }
      setUserState(ctx.from.id, { type: 'cron_create_url', backCallback: 'cron:menu' });
      await ctx.reply('Send target URL (e.g. keep-alive or deploy hook).', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'job':
      await renderCronJobDetails(ctx, jobId, { backCallback: 'cron:list' });
      break;
    case 'edit':
      await renderCronJobEditMenu(ctx, jobId);
      break;
    case 'toggle': {
      try {
        const job = await fetchCronJob(jobId);
        await updateJob(jobId, { enabled: !job?.enabled });
        await renderCronJobDetails(ctx, jobId, { backCallback: 'cron:list' });
      } catch (error) {
        await renderOrEdit(ctx, `Failed to toggle cron job: ${error.message}`, {
          reply_markup: buildBackKeyboard('cron:menu'),
        });
      }
      break;
    }
    case 'edit_name':
      await promptCronNameInput(ctx, jobId, 'cron:job:' + jobId);
      break;
    case 'change_schedule':
      await promptCronScheduleInput(ctx, jobId, 'cron:job:' + jobId);
      break;
    case 'change_url':
      await promptCronUrlInput(ctx, jobId, 'cron:job:' + jobId);
      break;
    case 'edit_timezone':
      await promptCronTimezoneInput(ctx, jobId, 'cron:job:' + jobId);
      break;
    case 'delete': {
      const inline = new InlineKeyboard()
        .text('✅ Yes, delete', `cron:delete_confirm:${jobId}`)
        .text('⬅️ No', `cron:job:${jobId}`);
      await renderOrEdit(ctx, `Delete cron job #${jobId}?`, { reply_markup: inline });
      break;
    }
    case 'delete_confirm':
      try {
        await deleteJob(jobId);
        await renderOrEdit(ctx, 'Cron job deleted.', {
          reply_markup: buildBackKeyboard('cron:menu'),
        });
      } catch (error) {
        await renderOrEdit(ctx, `Failed to delete cron job: ${error.message}`, {
          reply_markup: buildBackKeyboard('cron:menu'),
        });
      }
      break;
    default:
      break;
  }
}

async function handleProjectCronCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const parts = data.split(':');
  const action = parts[1];
  const projectId = parts[2];
  const extra = parts[3];

  if (!projectId) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  switch (action) {
    case 'menu':
      await renderProjectCronBindings(ctx, projectId);
      break;
    case 'keepalive':
      await handleProjectCronJobAction(ctx, projectId, 'keepalive');
      break;
    case 'deploy':
      await handleProjectCronJobAction(ctx, projectId, 'deploy');
      break;
    case 'keepalive_view':
      await openProjectCronJob(ctx, projectId, 'keepalive');
      break;
    case 'deploy_view':
      await openProjectCronJob(ctx, projectId, 'deploy');
      break;
    case 'keepalive_recreate':
      await promptProjectCronSchedule(ctx, projectId, 'keepalive', true);
      break;
    case 'deploy_recreate':
      await promptProjectCronSchedule(ctx, projectId, 'deploy', true);
      break;
    case 'keepalive_unlink':
      await renderProjectCronUnlinkConfirm(ctx, projectId, 'keepalive');
      break;
    case 'deploy_unlink':
      await renderProjectCronUnlinkConfirm(ctx, projectId, 'deploy');
      break;
    case 'unlink_confirm':
      await unlinkProjectCronJob(ctx, projectId, extra);
      break;
    case 'alerts_toggle':
      await toggleProjectCronAlerts(ctx, projectId);
      break;
    case 'alerts_levels':
      await renderProjectCronAlertLevels(ctx, projectId);
      break;
    case 'alerts_level':
      await toggleProjectCronAlertLevel(ctx, projectId, extra);
      break;
    default:
      break;
  }
}

async function renderSupabaseConnectionsMenu(ctx) {
  const connections = await loadSupabaseConnections();
  if (!connections.length) {
    await renderOrEdit(ctx, 'No Supabase connections configured.', {
      reply_markup: new InlineKeyboard().text('⬅️ Back', 'supabase:back'),
    });
    return;
  }
  const inline = new InlineKeyboard();
  connections.forEach((connection) => {
    inline.text(`🗄️ ${connection.name}`, `supabase:conn:${connection.id}`).row();
  });
  inline.text('⬅️ Back', 'supabase:back');
  await renderOrEdit(ctx, 'Select a Supabase connection:', { reply_markup: inline });
}

async function renderSupabaseConnectionMenu(ctx, connectionId) {
  const connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    await ctx.reply('Supabase connection not found.');
    return;
  }
  const inline = new InlineKeyboard()
    .text('📋 List tables', `supabase:tables:${connectionId}`)
    .row()
    .text('📝 Run SQL', `supabase:sql:${connectionId}`)
    .row()
    .text('⬅️ Back', 'supabase:connections');

  await renderOrEdit(ctx, `Supabase: ${connection.name}`, { reply_markup: inline });
}

function parseScheduleInput(input) {
  const raw = input?.trim();
  if (!raw) {
    throw new Error('Schedule is required.');
  }
  const normalized = raw.toLowerCase();
  const match = normalized.match(/^every\s+(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours)$/);
  if (match) {
    const amount = Number(match[1]);
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new Error('Invalid interval.');
    }
    const unit = match[2].startsWith('h') ? 'hour' : 'minute';
    if (unit === 'minute') {
      return {
        cron: `*/${amount} * * * *`,
        label: `Every ${amount} minute${amount === 1 ? '' : 's'}`,
      };
    }
    return {
      cron: `0 */${amount} * * *`,
      label: `Every ${amount} hour${amount === 1 ? '' : 's'}`,
    };
  }
  return { cron: raw, label: raw };
}

function getCronJobId(job) {
  return job?.jobId ?? job?.id ?? job?.job_id ?? null;
}

function getCronJobTitle(job) {
  return job?.title ?? job?.name ?? job?.jobTitle ?? job?.jobName ?? '';
}

function getCronJobDisplayName(job) {
  const title = getCronJobTitle(job);
  return title ? title : '(unnamed)';
}

function getCronJobUrl(job) {
  return job?.url || job?.request?.url || job?.httpTargetUrl || '';
}

function unwrapCronJobPayload(payload) {
  if (!payload) return null;
  if (payload.job) {
    return unwrapCronJobPayload(payload.job);
  }
  return payload;
}

function normalizeCronField(value) {
  if (value == null) return null;
  if (value === -1 || value === '-1') return -1;
  if (Array.isArray(value)) {
    const parsed = value.map((item) => Number(item)).filter(Number.isFinite);
    if (parsed.length === 1 && parsed[0] === -1) return -1;
    return parsed.length ? parsed : null;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    if (trimmed === '-1') return -1;
    const parsed = trimmed
      .split(',')
      .map((item) => Number(item.trim()))
      .filter(Number.isFinite);
    return parsed.length ? parsed : null;
  }
  if (typeof value === 'number') return [value];
  return null;
}

function normalizeCronJob(payload) {
  const job = unwrapCronJobPayload(payload);
  if (!job) return null;
  const schedule = job?.schedule || {};
  const minutes = normalizeCronField(schedule.minutes);
  const hours = normalizeCronField(schedule.hours);
  const expression = schedule.cron || schedule.expression || null;
  return {
    id: getCronJobId(job),
    name: getCronJobTitle(job),
    enabled: job?.enabled !== false,
    url: getCronJobUrl(job),
    schedule,
    timezone: schedule.timezone || null,
    minutes,
    hours,
    expression,
    raw: job,
  };
}

function formatCronField(value) {
  if (value === -1 || value == null) return 'any';
  if (Array.isArray(value) && value.length) return value.join(',');
  return String(value);
}

function getStepValue(values, range) {
  if (!Array.isArray(values) || values.length < 2) return null;
  const sorted = [...new Set(values)].sort((a, b) => a - b);
  if (sorted[0] !== 0) return null;
  const step = sorted[1] - sorted[0];
  if (!Number.isFinite(step) || step <= 0) return null;
  for (let i = 0; i < sorted.length; i += 1) {
    if (sorted[i] !== i * step) return null;
  }
  if (sorted[sorted.length - 1] >= range) return null;
  if (Math.ceil(range / step) !== sorted.length) return null;
  return step;
}

function formatTimePart(value) {
  return String(value).padStart(2, '0');
}

function describeCronSchedule(job, options = {}) {
  if (!job) return '-';
  const { includeAllHours = false } = options;
  const minutes = job.minutes;
  const hours = job.hours;
  const expression = job.expression;

  if (minutes == null && hours == null) {
    return expression || '-';
  }

  if (minutes === -1 && hours === -1) {
    return 'every minute';
  }

  const minuteStep = getStepValue(minutes, 60);
  const hourList = Array.isArray(hours) ? [...new Set(hours)].sort((a, b) => a - b) : null;

  if (minuteStep && hours === -1) {
    if (minuteStep === 1) return 'every minute';
    return includeAllHours ? `every ${minuteStep} minutes (all hours)` : `every ${minuteStep} minutes`;
  }

  if (minuteStep && hourList && hourList.length === 1) {
    return `every ${minuteStep} minutes at ${formatTimePart(hourList[0])}:00`;
  }

  if (Array.isArray(minutes) && minutes.length === 1 && hourList && hourList.length === 1) {
    return `daily at ${formatTimePart(hourList[0])}:${formatTimePart(minutes[0])}`;
  }

  if (Array.isArray(minutes) && minutes.length === 1 && hours === -1) {
    return `hourly at :${formatTimePart(minutes[0])}`;
  }

  if (expression) {
    return expression;
  }

  return `Minutes: ${formatCronField(minutes)}; Hours: ${formatCronField(hours)}`;
}

async function fetchCronJobs() {
  const { jobs, someFailed } = await listJobs();
  const normalized = jobs.map(normalizeCronJob).filter((job) => job && job.id != null);
  return { jobs: normalized, someFailed };
}

async function fetchCronJob(jobId) {
  const job = await getJobDetails(jobId);
  return normalizeCronJob(job);
}

function buildCronJobUpdatePayload(job, overrides = {}) {
  if (!job) {
    throw new Error('Cron job not found.');
  }
  const schedule = overrides.schedule ?? job.schedule ?? (job.expression ? { cron: job.expression } : null);
  if (!schedule) {
    throw new Error('Cron job schedule missing.');
  }
  return buildCronJobPayload({
    name: overrides.name ?? job.name ?? '',
    url: overrides.url ?? job.url ?? '',
    schedule,
    timezone: overrides.timezone ?? job.timezone ?? job.schedule?.timezone,
    enabled: overrides.enabled ?? job.enabled,
  });
}

function buildCronJobPayload({ name, url, schedule, timezone, enabled }) {
  let schedulePayload = null;
  if (schedule && typeof schedule === 'object') {
    schedulePayload = { ...schedule };
    if (!schedulePayload.timezone) {
      schedulePayload.timezone = timezone || 'UTC';
    }
  } else {
    schedulePayload = {
      timezone: timezone || 'UTC',
      cron: schedule,
    };
  }
  return {
    title: name,
    url,
    enabled: enabled !== false,
    schedule: schedulePayload,
  };
}

function formatScheduleValue(value, everyLabel) {
  const normalized = normalizeCronField(value);
  if (normalized == null) return '-';
  if (normalized === -1) return everyLabel;
  if (Array.isArray(normalized)) return normalized.join(',');
  return String(normalized);
}

function buildCronJobButtonLabel(job) {
  const maxLength = 32;
  const title = getCronJobDisplayName(job);
  let label = title === '(unnamed)' ? `⏰ Job ${job.id}` : `⏰ ${title}`;
  if (label.length > maxLength) {
    const suffix = ` (${job.id})`;
    const available = maxLength - suffix.length;
    if (available > 4) {
      label = `${label.slice(0, available - 1)}…${suffix}`;
    } else {
      label = `${label.slice(0, maxLength - 1)}…`;
    }
  }
  return label;
}

async function renderCronMenu(ctx) {
  const cronSettings = await getEffectiveCronSettings();
  if (!cronSettings.enabled) {
    await renderOrEdit(ctx, 'Cron integration is disabled in settings.', {
      reply_markup: buildBackKeyboard('main:back'),
    });
    return;
  }
  if (!CRON_API_KEY) {
    await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_KEY missing).', {
      reply_markup: buildBackKeyboard('main:back'),
    });
    return;
  }
  let jobs = [];
  try {
    const response = await fetchCronJobs();
    jobs = response.jobs;
  } catch (error) {
    await renderOrEdit(ctx, `Failed to list cron jobs: ${error.message}`, {
      reply_markup: buildBackKeyboard('main:back'),
    });
    return;
  }

  const lines = ['⏰ Cron jobs', '', `Total jobs: ${jobs.length}`];
  const inline = new InlineKeyboard()
    .text('📋 List jobs', 'cron:list')
    .row()
    .text('➕ Create job', 'cron:create')
    .row()
    .text('⬅️ Back', 'main:back');

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderCronJobList(ctx) {
  if (!CRON_API_KEY) {
    await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_KEY missing).', {
      reply_markup: buildBackKeyboard('main:back'),
    });
    return;
  }
  let jobs;
  let someFailed = false;
  try {
    const response = await fetchCronJobs();
    jobs = response.jobs;
    someFailed = response.someFailed;
  } catch (error) {
    await renderOrEdit(ctx, `Failed to list cron jobs: ${error.message}`, {
      reply_markup: buildBackKeyboard('cron:menu'),
    });
    return;
  }
  const lines = ['Cron jobs:'];
  jobs.forEach((job) => {
    const status = job.enabled ? 'Enabled' : 'Disabled';
    const name = getCronJobDisplayName(job);
    lines.push(`- ${job.id} — "${name}" — ${status}`);
  });
  if (!jobs.length) {
    lines.push('No cron jobs found.');
  }
  if (someFailed) {
    lines.push('', '⚠️ Some jobs failed to load from cron-job.org.');
  }

  const inline = new InlineKeyboard();
  jobs.forEach((job) => {
    const label = buildCronJobButtonLabel(job);
    inline.text(label, `cron:job:${job.id}`).row();
  });
  inline.text('⬅️ Back', 'main:back');

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderCronJobDetails(ctx, jobId, options = {}) {
  let job;
  try {
    job = await fetchCronJob(jobId);
  } catch (error) {
    await renderOrEdit(ctx, `Failed to load cron job: ${error.message}`, {
      reply_markup: buildBackKeyboard(options.backCallback || 'cron:menu'),
    });
    return;
  }
  if (!job) {
    await renderOrEdit(ctx, 'Cron job not found.', {
      reply_markup: buildBackKeyboard(options.backCallback || 'cron:menu'),
    });
    return;
  }

  const schedule = describeCronSchedule(job, { includeAllHours: true });
  const timezone = job?.schedule?.timezone || job?.timezone || '-';
  const url = job?.url || '-';
  const scheduleDetails = [
    `- Timezone: ${timezone}`,
    `- Minutes: ${formatScheduleValue(job?.schedule?.minutes, 'every minute')}`,
    `- Hours: ${formatScheduleValue(job?.schedule?.hours, 'every hour')}`,
    `- Days of month: ${formatScheduleValue(job?.schedule?.mdays, 'every day')}`,
    `- Months: ${formatScheduleValue(job?.schedule?.months, 'every month')}`,
    `- Weekdays: ${formatScheduleValue(job?.schedule?.wdays, 'every day')}`,
  ];
  const lines = [
    `Cron job #${jobId}:`,
    `Title: ${getCronJobDisplayName(job)}`,
    `Enabled: ${job?.enabled ? 'Yes' : 'No'}`,
    `URL: ${url}`,
    'Schedule:',
    ...scheduleDetails,
    `Schedule summary: ${schedule}`,
  ];

  const toggleLabel = job?.enabled ? '⏸️ Disable' : '✅ Enable';
  const inline = new InlineKeyboard()
    .text('✏️ Edit name', `cron:edit_name:${jobId}`)
    .row()
    .text('🔗 Edit URL', `cron:change_url:${jobId}`)
    .row()
    .text(toggleLabel, `cron:toggle:${jobId}`)
    .row()
    .text('⏰ Edit schedule', `cron:change_schedule:${jobId}`)
    .row()
    .text('🌍 Edit timezone', `cron:edit_timezone:${jobId}`)
    .row()
    .text('🗑 Delete', `cron:delete:${jobId}`)
    .row()
    .text('⬅️ Back', options.backCallback || 'cron:list');

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderCronJobEditMenu(ctx, jobId) {
  let job;
  try {
    job = await fetchCronJob(jobId);
  } catch (error) {
    await renderOrEdit(ctx, `Failed to load cron job: ${error.message}`, {
      reply_markup: buildBackKeyboard('cron:menu'),
    });
    return;
  }

  const lines = [
    `Edit Cron job #${jobId}:`,
    `- Enabled: ${job?.enabled ? '✅' : '❌'}`,
    `- Schedule: ${describeCronSchedule(job, { includeAllHours: true })}`,
    `- URL: ${job?.url || '-'}`,
    `- Timezone: ${job?.timezone || '-'}`,
    `- Name: ${getCronJobDisplayName(job)}`,
  ];

  const inline = new InlineKeyboard()
    .text('🔁 Toggle enabled', `cron:toggle:${jobId}`)
    .row()
    .text('⏱ Change schedule', `cron:change_schedule:${jobId}`)
    .row()
    .text('✏️ Change name', `cron:edit_name:${jobId}`)
    .row()
    .text('🔗 Change URL', `cron:change_url:${jobId}`)
    .row()
    .text('🌍 Change timezone', `cron:edit_timezone:${jobId}`)
    .row()
    .text('⬅️ Back', `cron:job:${jobId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function promptCronScheduleInput(ctx, jobId, backCallback) {
  setUserState(ctx.from.id, {
    type: 'cron_edit_schedule',
    jobId,
    backCallback,
  });
  await ctx.reply(
    "Send new schedule (cron string or 'every 10m', 'every 1h'). Or press Cancel.",
    { reply_markup: buildCancelKeyboard() },
  );
}

async function promptCronUrlInput(ctx, jobId, backCallback) {
  setUserState(ctx.from.id, {
    type: 'cron_edit_url',
    jobId,
    backCallback,
  });
  await ctx.reply('Send new URL for this job. Or press Cancel.', {
    reply_markup: buildCancelKeyboard(),
  });
}

async function promptCronNameInput(ctx, jobId, backCallback) {
  setUserState(ctx.from.id, {
    type: 'cron_edit_name',
    jobId,
    backCallback,
  });
  await ctx.reply('Send new name for this job (or type "clear" to remove).', {
    reply_markup: buildCancelKeyboard(),
  });
}

async function promptCronTimezoneInput(ctx, jobId, backCallback) {
  setUserState(ctx.from.id, {
    type: 'cron_edit_timezone',
    jobId,
    backCallback,
  });
  await ctx.reply('Send timezone (e.g. Europe/Berlin). Or press Cancel.', {
    reply_markup: buildCancelKeyboard(),
  });
}

async function handleSupabaseAddMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send: id, name, envKey');
    return;
  }

  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.', {
      reply_markup: buildBackKeyboard('supabase:back'),
    });
    return;
  }

  const parts = text.split(',').map((part) => part.trim());
  if (parts.length !== 3 || parts.some((part) => !part)) {
    await ctx.reply(
      'Invalid format.\n\nFormat:\n  id, name, envKey\n\nYou can also type `cancel` to exit.',
    );
    return;
  }

  const [id, name, envKey] = parts;
  const connections = await loadSupabaseConnections();
  if (connections.find((connection) => connection.id === id)) {
    await ctx.reply('A Supabase connection with this ID already exists.');
    return;
  }

  const next = [...connections, { id, name, envKey }];
  await saveSupabaseConnections(next);
  resetUserState(ctx);
  await ctx.reply('Supabase connection saved.');
  await renderDataCenterMenuForMessage(state.messageContext);
  if (!state.messageContext) {
    await renderDataCenterMenu(ctx);
  }
}

async function handleCronCreateMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a value or press Cancel.');
    return;
  }

  if (state.type === 'cron_create_url') {
    setUserState(ctx.from.id, {
      type: 'cron_create_schedule',
      url: text,
      backCallback: state.backCallback,
    });
    await ctx.reply(
      "Send schedule (cron string or 'every 10m', 'every 1h'). Or press Cancel.",
      { reply_markup: buildCancelKeyboard() },
    );
    return;
  }

  if (state.type === 'cron_create_schedule') {
    let schedule;
    try {
      schedule = parseScheduleInput(text);
    } catch (error) {
      await ctx.reply(`Invalid schedule: ${error.message}`);
      return;
    }
    setUserState(ctx.from.id, {
      type: 'cron_create_name',
      url: state.url,
      schedule,
      backCallback: state.backCallback,
    });
    await ctx.reply('Send job name (or type "skip" for default).', {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }

  if (state.type === 'cron_create_name') {
    const name = text.toLowerCase() === 'skip' ? null : text;
    const cronSettings = await getEffectiveCronSettings();
    try {
      const payload = buildCronJobPayload({
        name: name || `path-applier:custom:${Date.now()}`,
        url: state.url,
        schedule: state.schedule.cron,
        timezone: cronSettings.defaultTimezone,
        enabled: true,
      });
      const created = await createJob(payload);
      clearUserState(ctx.from.id);
      await ctx.reply(`Cron job created. ID: ${created.id}`);
      await renderCronMenu(ctx);
    } catch (error) {
      await ctx.reply(`Failed to create cron job: ${error.message}`);
    }
  }
}

async function handleCronEditScheduleMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a schedule or press Cancel.');
    return;
  }
  let schedule;
  try {
    schedule = parseScheduleInput(text);
  } catch (error) {
    await ctx.reply(`Invalid schedule: ${error.message}`);
    return;
  }
  try {
    const job = await fetchCronJob(state.jobId);
    const payload = buildCronJobUpdatePayload(job, { schedule: schedule.cron });
    await updateJob(state.jobId, payload);
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    await ctx.reply(`Failed to update cron job: ${error.message}`);
  }
}

async function handleCronEditUrlMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a URL or press Cancel.');
    return;
  }
  try {
    const job = await fetchCronJob(state.jobId);
    const payload = buildCronJobUpdatePayload(job, { url: text });
    await updateJob(state.jobId, payload);
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    await ctx.reply(`Failed to update cron job: ${error.message}`);
  }
}

async function handleCronEditNameMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a name or press Cancel.');
    return;
  }
  const name = text.toLowerCase() === 'clear' ? '' : text;
  try {
    const job = await fetchCronJob(state.jobId);
    const payload = buildCronJobUpdatePayload(job, { name });
    await updateJob(state.jobId, payload);
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    await ctx.reply(`Failed to update cron job: ${error.message}`);
  }
}

async function handleCronEditTimezoneMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a timezone or press Cancel.');
    return;
  }
  try {
    const job = await fetchCronJob(state.jobId);
    const payload = buildCronJobUpdatePayload(job, { timezone: text });
    await updateJob(state.jobId, payload);
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    await ctx.reply(`Failed to update cron job: ${error.message}`);
  }
}

async function handleProjectCronScheduleMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a schedule or press Cancel.');
    return;
  }
  let schedule;
  try {
    schedule = parseScheduleInput(text);
  } catch (error) {
    await ctx.reply(`Invalid schedule: ${error.message}`);
    return;
  }

  const projects = await loadProjects();
  const project = findProjectById(projects, state.projectId);
  if (!project) {
    clearUserState(ctx.from.id);
    await ctx.reply('Project not found.');
    return;
  }

  const cronSettings = await getEffectiveCronSettings();
  const isKeepAlive = state.type.includes('keepalive');
  const jobName = isKeepAlive
    ? `path-applier:${project.id}:keep-alive`
    : `path-applier:${project.id}:deploy`;

  let targetUrl = '';
  if (isKeepAlive) {
    if (!project.renderServiceUrl) {
      clearUserState(ctx.from.id);
      await ctx.reply('renderServiceUrl is not configured for this project.', {
        reply_markup: buildBackKeyboard(`projcron:menu:${project.id}`),
      });
      return;
    }
    const baseUrl = getPublicBaseUrl();
    targetUrl = `${baseUrl.replace(/\/+$/, '')}/keep-alive/${project.id}`;
  } else {
    if (!project.renderDeployHookUrl) {
      clearUserState(ctx.from.id);
      await ctx.reply('renderDeployHookUrl is not configured for this project.', {
        reply_markup: buildBackKeyboard(`projcron:menu:${project.id}`),
      });
      return;
    }
    targetUrl = project.renderDeployHookUrl;
  }

  const payload = buildCronJobPayload({
    name: jobName,
    url: targetUrl,
    schedule: schedule.cron,
    timezone: cronSettings.defaultTimezone,
    enabled: true,
  });

  try {
    if (state.type.endsWith('recreate')) {
      const oldJobId = isKeepAlive ? project.cronKeepAliveJobId : project.cronDeployHookJobId;
      if (oldJobId) {
        try {
          await deleteJob(oldJobId);
        } catch (error) {
          console.error('[cron] Failed to delete existing job', error);
        }
      }
    }
    const created = await createJob(payload);
    if (isKeepAlive) {
      project.cronKeepAliveJobId = created.id;
    } else {
      project.cronDeployHookJobId = created.id;
    }
    await saveProjects(projects);
    clearUserState(ctx.from.id);
    await ctx.reply(`Cron job created. ID: ${created.id}`);
    await renderProjectCronBindings(ctx, project.id);
  } catch (error) {
    await ctx.reply(`Failed to create cron job: ${error.message}`);
  }
}

async function handleSupabaseConsoleMessage(ctx, state) {
  if (state.mode !== 'awaiting-sql') {
    return;
  }
  const sql = ctx.message.text?.trim();
  if (!sql) {
    await ctx.reply('Please send the SQL query as text.');
    return;
  }
  if (sql.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.', {
      reply_markup: buildBackKeyboard(`supabase:conn:${state.connectionId}`),
    });
    return;
  }
  await runSupabaseSql(ctx, state.connectionId, sql);
  setUserState(ctx.from.id, { type: 'supabase_console', connectionId: state.connectionId, mode: null });
  await renderSupabaseConnectionMenu(ctx, state.connectionId);
}

async function promptSupabaseSql(ctx, connectionId) {
  const connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    await ctx.reply('Supabase connection not found.');
    return;
  }
  await ctx.reply(`Send the SQL query to execute on ${connection.name}.\n(Or press Cancel)`, {
    reply_markup: buildCancelKeyboard(),
  });
}

async function listSupabaseTables(ctx, connectionId) {
  const query = `
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY schemaname, tablename;
  `;
  try {
    const { rows } = await runSupabaseQuery(connectionId, query);
    const lines = rows.map((row) => `- ${row.schemaname}.${row.tablename}`);
    const output = truncateMessage(`Supabase tables:\n${lines.join('\n')}`, 3500);
    await ctx.reply(output);
  } catch (error) {
    await ctx.reply(`SQL error: ${error.message}`);
  }
}

async function runSupabaseSql(ctx, connectionId, sql) {
  try {
    const result = await runSupabaseQuery(connectionId, sql);
    if (result.rows && result.rows.length) {
      const lines = result.rows.slice(0, 50).map((row) => formatSqlRow(row));
      const output = truncateMessage(lines.join('\n'), 3500);
      await ctx.reply(output);
      return;
    }
    if (result.command === 'SELECT') {
      await ctx.reply('No rows returned.');
      return;
    }
    await ctx.reply(`Query executed. ${result.rowCount || 0} rows affected.`);
  } catch (error) {
    await ctx.reply(`SQL error: ${error.message}`);
  }
}

async function runSupabaseQuery(connectionId, sql) {
  let connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    const connections = await loadSupabaseConnections();
    connection = connections.find((item) => item.id === connectionId);
  }
  if (!connection) {
    throw new Error('Supabase connection not found.');
  }
  const dsn = process.env[connection.envKey];
  if (!dsn) {
    throw new Error(`Supabase DSN not configured for ${connection.name}.`);
  }
  let pool = supabasePools.get(connectionId);
  if (!pool) {
    pool = new Pool({ connectionString: dsn });
    supabasePools.set(connectionId, pool);
  }
  return pool.query(sql);
}

function formatSqlRow(row) {
  return Object.entries(row)
    .map(([key, value]) => `${key}: ${value === null ? 'null' : String(value)}`)
    .join(' | ');
}

function truncateMessage(text, limit) {
  if (text.length <= limit) return text;
  return `${text.slice(0, limit)}\n... (truncated)`;
}

function getWizardSteps() {
  return [
    'name',
    'id',
    'repoSlug',
    'workingDirConfirm',
    'workingDirCustom',
    'githubTokenEnvKey',
    'startCommand',
    'testCommand',
    'diagnosticCommand',
    'renderServiceUrl',
    'renderDeployHookUrl',
  ];
}

function getNextWizardStep(current) {
  const steps = getWizardSteps();
  const idx = steps.indexOf(current);
  return steps[idx + 1] || null;
}

function isWizardStepSkippable(step) {
  return [
    'name',
    'id',
    'startCommand',
    'testCommand',
    'diagnosticCommand',
    'renderServiceUrl',
    'renderDeployHookUrl',
  ].includes(step);
}

function getWizardKeyboard(step) {
  const inline = new InlineKeyboard();
  if (isWizardStepSkippable(step)) {
    inline.text('⏭ Skip', 'projwiz:skip').row();
  }
  inline.text('❌ Cancel', 'cancel_input');
  return inline;
}

function getWorkingDirChoiceKeyboard() {
  return new InlineKeyboard()
    .text('✅ Keep default', 'projwiz:keep_workdir')
    .text('✏️ Change working dir', 'projwiz:change_workdir')
    .row()
    .text('❌ Cancel', 'cancel_input');
}

function parseRepoSlug(value) {
  if (!value) return undefined;
  const trimmed = value.trim();
  const parts = trimmed.split('/');
  if (parts.length !== 2) return undefined;
  const [owner, repo] = parts;
  if (!owner || !repo) return undefined;
  return `${owner}/${repo}`;
}

async function startProjectWizard(ctx) {
  userState.set(ctx.from.id, {
    mode: 'create-project',
    step: 'name',
    draft: {},
    backCallback: 'proj:list',
  });

  await promptNextProjectField(ctx, userState.get(ctx.from.id));
}

async function handleProjectWizardCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, action] = data.split(':');
  const state = userState.get(ctx.from.id);
  if (!state || state.mode !== 'create-project') {
    return;
  }

  if (action === 'skip') {
    if (!isWizardStepSkippable(state.step)) {
      await ctx.reply('This step is required. Please enter a value.');
      return;
    }
    state.step = getNextWizardStep(state.step);
    await promptNextProjectField(ctx, state);
    return;
  }

  if (action === 'keep_workdir') {
    state.step = 'githubTokenEnvKey';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (action === 'change_workdir') {
    state.step = 'workingDirCustom';
    await promptNextProjectField(ctx, state);
  }
}

async function handleProjectWizardInput(ctx, state) {
  const text = ctx.message.text.trim();
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.', {
      reply_markup: buildBackKeyboard('proj:list'),
    });
    return;
  }
  const value = text;
  if (!value) {
    await ctx.reply('Please send text or press Skip.');
    return;
  }

  if (state.step === 'name') {
    state.draft.name = value;
    state.step = 'id';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'id') {
    state.draft.id = value;
    state.step = 'repoSlug';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'repoSlug') {
    const repoSlug = parseRepoSlug(value);
    if (!repoSlug) {
      await ctx.reply('Please send a valid repo in the format owner/repo.');
      return;
    }
    state.draft.repoSlug = repoSlug;
    state.draft.repoUrl = `https://github.com/${repoSlug}`;
    const defaultWorkingDir = getDefaultWorkingDir(repoSlug);
    if (defaultWorkingDir) {
      state.draft.workingDir = defaultWorkingDir;
      state.draft.isWorkingDirCustom = false;
    }
    state.step = 'workingDirConfirm';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'workingDirConfirm') {
    await ctx.reply('Please use the buttons below to choose a working directory option.');
    return;
  }

  if (state.step === 'workingDirCustom') {
    if (!value) {
      await ctx.reply('Please send a valid working directory path.');
      return;
    }
    state.draft.workingDir = value;
    state.draft.isWorkingDirCustom = true;
    state.step = 'githubTokenEnvKey';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'githubTokenEnvKey') {
    if (value === '-') {
      state.draft.githubTokenEnvKey = undefined;
    } else {
      state.draft.githubTokenEnvKey = value;
    }
    state.step = 'startCommand';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'startCommand') {
    state.draft.startCommand = value;
    state.step = 'testCommand';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'testCommand') {
    state.draft.testCommand = value;
    state.step = 'diagnosticCommand';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'diagnosticCommand') {
    state.draft.diagnosticCommand = value;
    state.step = 'renderServiceUrl';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'renderServiceUrl') {
    state.draft.renderServiceUrl = value;
    state.step = 'renderDeployHookUrl';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'renderDeployHookUrl') {
    state.draft.renderDeployHookUrl = value;
    state.step = null;
    await promptNextProjectField(ctx, state);
  }
}

async function promptNextProjectField(ctx, state) {
  if (!state.step) {
    await finalizeProjectWizard(ctx, state);
    return;
  }

  const prompts = {
    name: '🆕 New project\n\nSend project *name* or press Skip.\n(Or press Cancel)',
    id: 'Send project *ID* (unique short handle) or press Skip.\n(Or press Cancel)',
    repoSlug:
      'Send GitHub repo as `owner/repo` (for example: Mirax226/daily-system-bot-v2).\n(Or press Cancel)',
    workingDirConfirm: null,
    workingDirCustom: 'Send working directory path.\n(Or press Cancel)',
    githubTokenEnvKey:
      'GitHub token env key:\nDefault: GITHUB_TOKEN\nSend a custom env key or type `-` to use the default.',
    startCommand: 'Send *startCommand* (or Skip).\n(Or press Cancel)',
    testCommand: 'Send *testCommand* (or Skip).\n(Or press Cancel)',
    diagnosticCommand: 'Send *diagnosticCommand* (or Skip).\n(Or press Cancel)',
    renderServiceUrl: 'Send Render service URL (or Skip).\n(Or press Cancel)',
    renderDeployHookUrl: 'Send Render deploy hook URL (or Skip).\n(Or press Cancel)',
  };

  if (state.step === 'workingDirConfirm') {
    await renderOrEdit(
      ctx,
      `Default working directory:\n${state.draft.workingDir || '-'}\nDo you want to change it?`,
      { reply_markup: getWorkingDirChoiceKeyboard() },
    );
    return;
  }

  await renderOrEdit(ctx, prompts[state.step], {
    parse_mode: 'Markdown',
    reply_markup: getWizardKeyboard(state.step),
  });
}

async function finalizeProjectWizard(ctx, state) {
  const draft = state.draft || {};
  const baseId = draft.id || slugifyProjectId(draft.name || 'project');
  const fallbackId = baseId || `project-${Date.now()}`;

  const projects = await loadProjects();
  let finalId = fallbackId;
  if (projects.find((project) => project.id === finalId)) {
    finalId = `${finalId}-${Date.now()}`;
  }

  const repoSlug = draft.repoSlug;
  const owner = repoSlug ? repoSlug.split('/')[0] : undefined;
  const repo = repoSlug ? repoSlug.split('/')[1] : undefined;

  const project = {
    id: finalId,
    name: draft.name || finalId,
    repoSlug,
    repoUrl: draft.repoUrl || (repoSlug ? `https://github.com/${repoSlug}` : undefined),
    workingDir: draft.workingDir || (repoSlug ? getDefaultWorkingDir(repoSlug) : undefined),
    isWorkingDirCustom: draft.isWorkingDirCustom || false,
    githubTokenEnvKey: draft.githubTokenEnvKey,
    owner,
    repo,
    startCommand: draft.startCommand,
    testCommand: draft.testCommand,
    diagnosticCommand: draft.diagnosticCommand,
    renderServiceUrl: draft.renderServiceUrl,
    renderDeployHookUrl: draft.renderDeployHookUrl,
  };

  projects.push(project);
  try {
    await saveProjects(projects);
  } catch (error) {
    console.error('[configStore] Failed to save projects', error);
    await ctx.reply('Failed to save project settings (DB error). Changes may not persist.');
  }
  userState.delete(ctx.from.id);

  await renderOrEdit(
    ctx,
    `✅ Project created.\nName: ${project.name}\nID: ${project.id}`,
  );
  await renderProjectsList(ctx);
}

async function handlePatchApplication(ctx, projectId, patchText) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const startTime = Date.now();
  const globalSettings = await loadGlobalSettings();

  try {
    const effectiveBaseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
    let repoInfo;
    try {
      repoInfo = getRepoInfo(project);
    } catch (error) {
      if (error.message === 'Project is missing repoSlug') {
        await ctx.reply(
          'This project is not fully configured: repoSlug is missing. Use "📝 Edit repo" to set it.',
        );
        return;
      }
      throw error;
    }

    await ctx.reply('Updating repository…');
    const { git, repoDir } = await prepareRepository(project, effectiveBaseBranch);
    const branchName = makePatchBranchName(project.id);

    await createWorkingBranch(git, effectiveBaseBranch, branchName);

    await ctx.reply('Applying patch…');
    await applyPatchToRepo(git, repoDir, patchText);

    await ctx.reply('Committing and pushing…');
    const identityResult = await configureGitIdentity(git);
    if (!identityResult.ok) {
      const stderr = identityResult.error?.stderr || identityResult.error?.message || 'Unknown error';
      console.error(`[gitIdentity] Failed to set ${identityResult.step}: ${stderr}`);
      await ctx.reply('Failed to configure git author identity for this project. Please check logs.');
      throw new Error('Failed to configure git author identity for this project.');
    }
    const hasChanges = await commitAndPush(git, branchName);
    if (!hasChanges) {
      await ctx.reply('Patch applied but no changes detected.');
      return;
    }

    await ctx.reply('Creating Pull Request…');
    const prBody = buildPrBody(patchText);
    const [owner, repo] = repoInfo.repoSlug.split('/');
    const githubToken = getGithubToken(project);
    const pr = await createPullRequest({
      owner,
      repo,
      baseBranch: effectiveBaseBranch || DEFAULT_BASE_BRANCH,
      headBranch: branchName,
      title: `Automated patch: ${project.id}`,
      body: prBody,
      token: githubToken,
    });

    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const inline = new InlineKeyboard().url('View PR', pr.html_url);
    await ctx.reply(`Patch applied successfully.\nElapsed: ~${elapsed}s`, { reply_markup: inline });
  } catch (error) {
    console.error('Failed to apply patch', error);
    await ctx.reply(`Failed to apply patch: ${error.message}`);
  } finally {
    clearUserState(ctx.from.id);
  }
}

async function saveProjectsWithFeedback(ctx, projects) {
  try {
    await saveProjects(projects);
    return true;
  } catch (error) {
    console.error('[configStore] Failed to save project settings', error);
    await ctx.reply('Failed to save project settings (DB error). Changes may not persist.');
    return false;
  }
}

async function handleRenameProjectStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  if (state.step === 1) {
    setUserState(ctx.from.id, {
      type: 'rename_project',
      step: 2,
      projectId: state.projectId,
      data: { newName: text },
    });
    await ctx.reply('Send new ID (or leave empty to keep current).\n(Or press Cancel)', {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }

  if (state.step === 2) {
    const newId = text || state.projectId;
    const projects = await loadProjects();
    const idx = projects.findIndex((p) => p.id === state.projectId);
    if (idx === -1) {
      await ctx.reply('Project not found.');
      clearUserState(ctx.from.id);
      return;
    }

    if (newId !== state.projectId && projects.find((p) => p.id === newId)) {
      await ctx.reply('A project with this ID already exists.');
      return;
    }

    const updatedProject = { ...projects[idx], name: state.data.newName, id: newId };
    projects[idx] = updatedProject;
    await saveProjects(projects);

    const settings = await loadGlobalSettings();
    if (settings.defaultProjectId === state.projectId) {
      settings.defaultProjectId = newId;
      await saveGlobalSettings(settings);
    }

    clearUserState(ctx.from.id);
    await renderProjectSettingsForMessage(state.messageContext, newId);
    if (!state.messageContext) {
      await renderProjectSettings(ctx, newId);
    }
  }
}

async function handleChangeBaseBranchStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const updated = await updateProjectField(state.projectId, 'baseBranch', text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectSettingsForMessage(state.messageContext, state.projectId);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId);
  }
}

async function handleEditRepoStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const repoSlug = parseRepoSlug(text);
  if (!repoSlug) {
    await ctx.reply('Please send a valid repo in the format owner/repo.');
    return;
  }

  const projects = await loadProjects();
  const idx = projects.findIndex((project) => project.id === state.projectId);
  if (idx === -1) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const [owner, repo] = repoSlug.split('/');
  const repoUrl = `https://github.com/${repoSlug}`;
  const defaultWorkingDir = getDefaultWorkingDir(repoSlug);
  const updatedProject = {
    ...projects[idx],
    repoSlug,
    repoUrl,
    owner,
    repo,
  };

  if (!updatedProject.isWorkingDirCustom) {
    updatedProject.workingDir = defaultWorkingDir;
    updatedProject.isWorkingDirCustom = false;
  }

  projects[idx] = updatedProject;
  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) {
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectSettingsForMessage(state.messageContext, state.projectId);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId);
  }
}

async function handleEditWorkingDirStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const projects = await loadProjects();
  const idx = projects.findIndex((project) => project.id === state.projectId);
  if (idx === -1) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const project = projects[idx];
  const trimmed = text.trim();
  let nextWorkingDir = trimmed;
  let isWorkingDirCustom = true;

  if (trimmed === '-') {
    if (!project.repoSlug) {
      await ctx.reply('Cannot auto-set workingDir without repoSlug.');
      return;
    }
    const defaultDir = getDefaultWorkingDir(project.repoSlug);
    if (!defaultDir) {
      await ctx.reply('Cannot derive workingDir from repoSlug.');
      return;
    }
    nextWorkingDir = defaultDir;
    isWorkingDirCustom = false;
  }

  projects[idx] = {
    ...project,
    workingDir: nextWorkingDir,
    isWorkingDirCustom,
  };

  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) {
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectSettingsForMessage(state.messageContext, state.projectId);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId);
  }
}

async function handleEditGithubTokenStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const projects = await loadProjects();
  const idx = projects.findIndex((project) => project.id === state.projectId);
  if (idx === -1) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const tokenKey = text.trim() === '-' ? undefined : text.trim();
  projects[idx] = {
    ...projects[idx],
    githubTokenEnvKey: tokenKey,
  };

  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) {
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectSettingsForMessage(state.messageContext, state.projectId);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId);
  }
}

async function handleEditCommandInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const updated = await updateProjectField(state.projectId, state.field, text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectSettingsForMessage(state.messageContext, state.projectId);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId);
  }
}

async function handleEditRenderUrl(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const updated = await updateProjectField(state.projectId, state.field, text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectSettingsForMessage(state.messageContext, state.projectId);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId);
  }
}

async function handleEditSupabase(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const updated = await updateProjectField(state.projectId, 'supabaseConnectionId', text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectSettingsForMessage(state.messageContext, state.projectId);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId);
  }
}

async function handleGlobalBaseChange(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await ctx.reply('Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const settings = await loadGlobalSettings();
  settings.defaultBaseBranch = text;
  await saveGlobalSettings(settings);
  clearUserState(ctx.from.id);
  await ctx.reply('Default base branch updated.');
  await renderGlobalSettings(ctx);
}

async function runProjectDiagnostics(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const command = project.diagnosticCommand || project.testCommand;
  if (!command) {
    await ctx.reply('No diagnostic/test command configured for this project.');
    return;
  }

  const globalSettings = await loadGlobalSettings();
  const effectiveBaseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;

  let repoInfo;
  try {
    repoInfo = getRepoInfo(project);
    await prepareRepository(project, effectiveBaseBranch);
  } catch (error) {
    if (error.message === 'Project is missing repoSlug') {
      await ctx.reply(
        'This project is not fully configured: repoSlug is missing. Use "📝 Edit repo" to set it.',
      );
      return;
    }
    console.error('Failed to prepare repository for diagnostics', error);
  }

  const workingDir = project.workingDir || repoInfo?.workingDir;
  if (!workingDir) {
    await ctx.reply('No working directory configured for this project.');
    return;
  }

  if (!project.workingDir) {
    await updateProjectField(projectId, 'workingDir', workingDir);
  }

  const result = await runCommandInProject({ ...project, workingDir }, command);
  if (result.exitCode === 0) {
    await ctx.reply(
      `🧪 Diagnostics finished successfully.\nProject: ${project.name || project.id}\nDuration: ${result.durationMs} ms\n\nLast output:\n${result.stdout || '(no output)'}`,
    );
    return;
  }

  const errorExcerpt = result.stderr || result.stdout || '(no output)';
  await ctx.reply(
    `🧪 Diagnostics FAILED (exit code ${result.exitCode}).\nProject: ${project.name || project.id}\nDuration: ${result.durationMs} ms\n\nError excerpt:\n${errorExcerpt}`,
  );
}

async function pingRenderService(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  if (!project.renderServiceUrl) {
    await ctx.reply('No render service URL configured for this project.');
    return;
  }
  const start = Date.now();
  try {
    const response = await requestUrl('GET', project.renderServiceUrl);
    const durationMs = Date.now() - start;
    await ctx.reply(`Ping Render: HTTP ${response.status} in ~${durationMs} ms`);
  } catch (error) {
    await ctx.reply(`Ping Render failed: ${error.message}`);
  }
}

async function showKeepAliveUrl(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const baseUrl = getPublicBaseUrl();
  const url = `${baseUrl.replace(/\/$/, '')}/keep-alive/${project.id}`;
  await ctx.reply(`Keep-alive URL:\n${url}`);
}

async function triggerRenderDeploy(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  if (!project.renderDeployHookUrl) {
    await ctx.reply('No deploy hook URL configured for this project.');
    return;
  }
  const start = Date.now();
  try {
    const response = await requestUrl('POST', project.renderDeployHookUrl);
    const durationMs = Date.now() - start;
    const bodySnippet = response.body ? response.body.slice(0, 200) : '';
    const details = bodySnippet ? `\nBody: ${bodySnippet}` : '';
    await ctx.reply(`Render deploy hook: HTTP ${response.status} in ~${durationMs} ms${details}`);
  } catch (error) {
    await ctx.reply(`Render deploy hook failed: ${error.message}`);
  }
}

async function getProjectById(projectId, ctx) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project && ctx) {
    await renderOrEdit(ctx, 'Project not found.');
  }
  return project;
}

async function renderProjectSettings(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const globalSettings = await loadGlobalSettings();
  const view = buildProjectSettingsView(project, globalSettings);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function buildProjectSettingsView(project, globalSettings) {
  const effectiveBase = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  const isDefault = globalSettings.defaultProjectId === project.id;
  const name = project.name || project.id;
  const tokenKey = project.githubTokenEnvKey || 'GITHUB_TOKEN';
  const tokenLabel = tokenKey === 'GITHUB_TOKEN' ? 'GITHUB_TOKEN (default)' : tokenKey;

  const lines = [
    `Project: ${isDefault ? '⭐ ' : ''}${name} (id: ${project.id})`,
    '',
    'Repo:',
    `- slug: ${project.repoSlug || 'not set'}`,
    `- url: ${project.repoUrl || 'not set'}`,
    `Working dir: ${project.workingDir || '-'}`,
    `GitHub token env: ${tokenLabel}`,
    `Base branch: ${effectiveBase}`,
    '',
    'Commands:',
    `- start: ${project.startCommand || '-'}`,
    `- test: ${project.testCommand || '-'}`,
    `- diag: ${project.diagnosticCommand || '-'}`,
    '',
    'Render:',
    `- service: ${project.renderServiceUrl || '-'}`,
    `- deploy hook: ${project.renderDeployHookUrl || '-'}`,
    '',
    'Supabase:',
    `- connectionId: ${project.supabaseConnectionId || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Edit project', `proj:project_menu:${project.id}`)
    .text('🌱 Change base branch', `proj:change_base:${project.id}`)
    .row()
    .text('📝 Edit repo', `proj:edit_repo:${project.id}`)
    .text('📁 Edit working dir', `proj:edit_workdir:${project.id}`)
    .row()
    .text('🔑 Edit GitHub token', `proj:edit_github_token:${project.id}`)
    .row()
    .text('🧰 Edit commands', `proj:commands:${project.id}`)
    .row()
    .text('📡 Server', `proj:server_menu:${project.id}`)
    .text('🗄 Supabase binding', `proj:supabase:${project.id}`)
    .row()
    .text('📣 Log alerts', `projlog:menu:${project.id}`)
    .row();

  if (!isDefault) {
    inline.text('⭐ Set as default project', `proj:set_default:${project.id}`).row();
  }

  inline.text('🗑 Delete project', `proj:delete:${project.id}`).text('⬅️ Back', 'proj:list');

  return { text: lines.join('\n'), keyboard: inline };
}

function buildProjectLogAlertsView(project) {
  const forwarding = getEffectiveProjectLogForwarding(project);
  const levelsLabel = forwarding.levels.length ? forwarding.levels.join(' / ') : 'error';
  const selected = new Set(forwarding.levels);
  const lines = [
    `📣 Log alerts — ${project.name || project.id}`,
    '',
    `Status: ${forwarding.enabled ? 'Enabled' : 'Disabled'}`,
    `Levels: ${levelsLabel}`,
  ];

  const inline = new InlineKeyboard()
    .text(forwarding.enabled ? '✅ Enabled' : '🚫 Disabled', `projlog:toggle:${project.id}`)
    .row()
    .text(`❗ Errors: ${selected.has('error') ? 'ON' : 'OFF'}`, `projlog:level:error:${project.id}`)
    .text(`⚠️ Warnings: ${selected.has('warn') ? 'ON' : 'OFF'}`, `projlog:level:warn:${project.id}`)
    .row()
    .text(`ℹ️ Info: ${selected.has('info') ? 'ON' : 'OFF'}`, `projlog:level:info:${project.id}`)
    .row()
    .text('⬅️ Back', `projlog:back:${project.id}`);

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectLogAlerts(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const view = buildProjectLogAlertsView(project);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function renderProjectSettingsForMessage(messageContext, projectId) {
  if (!messageContext) {
    return;
  }
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    return;
  }
  const globalSettings = await loadGlobalSettings();
  const view = buildProjectSettingsView(project, globalSettings);
  try {
    await bot.api.editMessageText(
      messageContext.chatId,
      messageContext.messageId,
      view.text,
      { reply_markup: view.keyboard },
    );
  } catch (error) {
    console.error('[UI] Failed to update project card message', error);
  }
}

async function renderProjectMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;

  const inline = new InlineKeyboard()
    .text('🧩 Apply patch', `proj:apply_patch:${projectId}`)
    .row()
    .text('✏️ Edit project', `proj:rename:${projectId}`)
    .row()
    .text('🌿 Change base branch', `proj:change_base:${projectId}`)
    .row()
    .text('🧰 Edit commands', `proj:commands:${projectId}`)
    .row()
    .text('📡 Edit Render URLs', `proj:render_menu:${projectId}`)
    .row()
    .text('🧪 Run diagnostics', `proj:diagnostics:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, `📂 Project menu: ${project.name || project.id}`, {
    reply_markup: inline,
  });
}

async function renderServerMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;

  const lines = [`📡 Server actions: ${project.name || project.id}`];

  const inline = new InlineKeyboard()
    .text('📡 Ping Render now', `proj:render_ping:${projectId}`)
    .row()
    .text('🔗 Show keep-alive URL', `proj:render_keepalive_url:${projectId}`)
    .row()
    .text('🚀 Deploy (Render)', `proj:render_deploy:${projectId}`)
    .row()
    .text('⏱ Cron bindings', `projcron:menu:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderProjectCronBindings(ctx, projectId) {
  const cronSettings = await getEffectiveCronSettings();
  if (!cronSettings.enabled) {
    await renderOrEdit(ctx, 'Cron integration is disabled in settings.', {
      reply_markup: buildBackKeyboard(`proj:server_menu:${projectId}`),
    });
    return;
  }
  if (!CRON_API_KEY) {
    await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_KEY missing).', {
      reply_markup: buildBackKeyboard(`proj:server_menu:${projectId}`),
    });
    return;
  }

  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  const levels = getCronNotificationLevels(project).join(' / ');
  const lines = [
    `Cron for project ${project.name || project.id}:`,
    `- Keep-alive job: ${project.cronKeepAliveJobId || 'none'}`,
    `- Deploy hook job: ${project.cronDeployHookJobId || 'none'}`,
    `- Notifications: ${project.cronNotificationsEnabled ? 'on' : 'off'}`,
    `- Levels: ${levels}`,
  ];

  const inline = new InlineKeyboard()
    .text(
      project.cronKeepAliveJobId ? '✏️ Keep-alive job' : '➕ Keep-alive job',
      `projcron:keepalive:${projectId}`,
    )
    .row()
    .text(
      project.cronDeployHookJobId ? '✏️ Deploy job' : '➕ Deploy job',
      `projcron:deploy:${projectId}`,
    )
    .row()
    .text('🔔 Alerts on/off', `projcron:alerts_toggle:${projectId}`)
    .row()
    .text('⚙️ Alert levels', `projcron:alerts_levels:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:server_menu:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function handleProjectCronJobAction(ctx, projectId, type) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  const jobId =
    type === 'keepalive' ? project.cronKeepAliveJobId : project.cronDeployHookJobId;

  if (jobId) {
    const inline = new InlineKeyboard()
      .text('🔍 View cron job', `projcron:${type}_view:${projectId}`)
      .row()
      .text('♻️ Recreate job', `projcron:${type}_recreate:${projectId}`)
      .row()
      .text('🗑 Unlink job', `projcron:${type}_unlink:${projectId}`)
      .row()
      .text('⬅️ Back', `projcron:menu:${projectId}`);
    await renderOrEdit(
      ctx,
      `${type === 'keepalive' ? 'Keep-alive' : 'Deploy'} cron job: ${jobId}`,
      { reply_markup: inline },
    );
    return;
  }

  await promptProjectCronSchedule(ctx, projectId, type, false);
}

async function promptProjectCronSchedule(ctx, projectId, type, recreate) {
  setUserState(ctx.from.id, {
    type: recreate
      ? type === 'keepalive'
        ? 'projcron_keepalive_recreate'
        : 'projcron_deploy_recreate'
      : type === 'keepalive'
        ? 'projcron_keepalive_schedule'
        : 'projcron_deploy_schedule',
    projectId,
    backCallback: `projcron:menu:${projectId}`,
  });
  await ctx.reply(
    'Send schedule (cron string or "every 10m", "every 1h"). Or press Cancel.',
    { reply_markup: buildCancelKeyboard() },
  );
}

async function openProjectCronJob(ctx, projectId, type) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const jobId =
    type === 'keepalive' ? project.cronKeepAliveJobId : project.cronDeployHookJobId;
  if (!jobId) {
    await renderOrEdit(ctx, 'Cron job not linked.', {
      reply_markup: buildBackKeyboard(`projcron:menu:${projectId}`),
    });
    return;
  }
  await renderCronJobDetails(ctx, jobId, { backCallback: `projcron:menu:${projectId}` });
}

async function renderProjectCronUnlinkConfirm(ctx, projectId, type) {
  const label = type === 'keepalive' ? 'keep-alive' : 'deploy';
  const inline = new InlineKeyboard()
    .text('✅ Yes, unlink', `projcron:unlink_confirm:${projectId}:${type}`)
    .text('⬅️ No', `projcron:menu:${projectId}`);
  await renderOrEdit(ctx, `Unlink ${label} cron job?`, { reply_markup: inline });
}

async function unlinkProjectCronJob(ctx, projectId, type) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const jobId =
    type === 'keepalive' ? project.cronKeepAliveJobId : project.cronDeployHookJobId;
  if (jobId) {
    try {
      await deleteJob(jobId);
    } catch (error) {
      console.error('[cron] Failed to delete cron job during unlink', error);
    }
  }
  if (type === 'keepalive') {
    project.cronKeepAliveJobId = null;
  } else {
    project.cronDeployHookJobId = null;
  }
  await saveProjects(projects);
  await renderProjectCronBindings(ctx, projectId);
}

async function toggleProjectCronAlerts(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  project.cronNotificationsEnabled = !project.cronNotificationsEnabled;
  await saveProjects(projects);
  await renderProjectCronBindings(ctx, projectId);
}

async function renderProjectCronAlertLevels(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const levels = new Set(getCronNotificationLevels(project));
  const lines = [
    `Alert levels for ${project.name || project.id}:`,
    `${levels.has('info') ? '[x]' : '[ ]'} info`,
    `${levels.has('warning') ? '[x]' : '[ ]'} warning`,
    `${levels.has('error') ? '[x]' : '[ ]'} error`,
  ];

  const inline = new InlineKeyboard()
    .text(`${levels.has('info') ? '✅' : '➖'} info`, `projcron:alerts_level:${projectId}:info`)
    .text(
      `${levels.has('warning') ? '✅' : '➖'} warning`,
      `projcron:alerts_level:${projectId}:warning`,
    )
    .row()
    .text(`${levels.has('error') ? '✅' : '➖'} error`, `projcron:alerts_level:${projectId}:error`)
    .row()
    .text('⬅️ Back', `projcron:menu:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function toggleProjectCronAlertLevel(ctx, projectId, level) {
  if (!['info', 'warning', 'error'].includes(level)) {
    await renderProjectCronAlertLevels(ctx, projectId);
    return;
  }
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const levels = new Set(getCronNotificationLevels(project));
  if (levels.has(level)) {
    levels.delete(level);
  } else {
    levels.add(level);
  }
  if (levels.size === 0) {
    await ctx.answerCallbackQuery({
      text: 'You must keep at least one level enabled.',
      show_alert: true,
    });
    return;
  }
  project.cronNotificationsLevels = Array.from(levels);
  await saveProjects(projects);
  await renderProjectCronAlertLevels(ctx, projectId);
}

async function renderDataCenterMenu(ctx) {
  const view = await buildDataCenterView();
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function buildDataCenterView() {
  const connections = await loadSupabaseConnections();
  const cronSettings = await getEffectiveCronSettings();
  const cronStatus = await getCronStatusLine(cronSettings);
  const lines = [
    '🏭 Data Center',
    `Config DB: ${
      runtimeStatus.configDbOk
        ? '✅ OK'
        : `❌ ERROR – ${runtimeStatus.configDbError || 'see logs'}`
    }`,
    cronStatus,
  ];

  if (!connections.length) {
    lines.push('Supabase connections: none configured.');
  } else {
    lines.push('Supabase connections:');
    connections.forEach((connection) => {
      lines.push(`• ${connection.id} – env: ${connection.envKey}`);
    });
  }

  const inline = new InlineKeyboard()
    .text('➕ Add Supabase connection', 'supabase:add')
    .row()
    .text('🧾 List connections', 'supabase:connections')
    .row();

  if (cronSettings.enabled) {
    inline.text('⏰ Cron jobs', 'cron:menu').row();
  }

  inline.text('⬅️ Back', 'main:back');

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderDataCenterMenuForMessage(messageContext) {
  if (!messageContext) {
    return;
  }
  const view = await buildDataCenterView();
  try {
    await bot.api.editMessageText(
      messageContext.chatId,
      messageContext.messageId,
      view.text,
      { reply_markup: view.keyboard },
    );
  } catch (error) {
    console.error('[UI] Failed to update data center message', error);
  }
}

async function renderDeleteConfirmation(ctx, projectId) {
  const inline = new InlineKeyboard()
    .text('🗑️ Yes, delete', `proj:confirm_delete:${projectId}`)
    .text('⬅️ Cancel', `proj:cancel_delete:${projectId}`);
  await renderOrEdit(ctx, `Are you sure you want to delete project ${projectId}?`, {
    reply_markup: inline,
  });
}

async function deleteProject(ctx, projectId) {
  const projects = await loadProjects();
  const filtered = projects.filter((p) => p.id !== projectId);
  if (filtered.length === projects.length) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  await saveProjects(filtered);
  const settings = await loadGlobalSettings();
  if (settings.defaultProjectId === projectId) {
    settings.defaultProjectId = undefined;
    await saveGlobalSettings(settings);
  }
  await renderOrEdit(ctx, `Project ${projectId} deleted.`);
  await renderProjectsList(ctx);
}

async function renderCommandsScreen(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const lines = [
    `startCommand: ${project.startCommand || '-'}`,
    `testCommand: ${project.testCommand || '-'}`,
    `diagnosticCommand: ${project.diagnosticCommand || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Edit startCommand', `proj:cmd_edit:${project.id}:startCommand`)
    .row()
    .text('✏️ Edit testCommand', `proj:cmd_edit:${project.id}:testCommand`)
    .row()
    .text('✏️ Edit diagnosticCommand', `proj:cmd_edit:${project.id}:diagnosticCommand`);

  if (project.startCommand || project.testCommand || project.diagnosticCommand) {
    inline.row().text('🧹 Clear all commands', `proj:cmd_clearall:${project.id}`);
  }

  inline.row().text('⬅️ Back', `proj:project_menu:${project.id}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderRenderUrlsScreen(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const lines = [
    `renderServiceUrl: ${project.renderServiceUrl || '-'}`,
    `renderDeployHookUrl: ${project.renderDeployHookUrl || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Edit service URL', `proj:render_edit:${project.id}:renderServiceUrl`)
    .row()
    .text('✏️ Edit deploy hook URL', `proj:render_edit:${project.id}:renderDeployHookUrl`);

  if (project.renderServiceUrl) {
    inline.row().text('🧹 Clear service URL', `proj:render_clear:${project.id}:renderServiceUrl`);
  }
  if (project.renderDeployHookUrl) {
    inline
      .row()
      .text('🧹 Clear deploy hook URL', `proj:render_clear:${project.id}:renderDeployHookUrl`);
  }

  inline.row().text('⬅️ Back', `proj:project_menu:${project.id}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderSupabaseScreen(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const lines = [`supabaseConnectionId: ${project.supabaseConnectionId || '-'}`];
  const inline = new InlineKeyboard().text('✏️ Edit', `proj:supabase_edit:${project.id}`);
  if (project.supabaseConnectionId) {
    inline.text('🧹 Clear', `proj:supabase_clear:${project.id}`);
  }
  inline.row().text('⬅️ Back', `proj:open:${project.id}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderGlobalSettings(ctx) {
  const settings = await loadGlobalSettings();
  const projects = await loadProjects();
  const defaultProject = settings.defaultProjectId
    ? findProjectById(projects, settings.defaultProjectId)
    : undefined;
  const selfLogForwarding = getEffectiveSelfLogForwarding(settings);
  const lines = [
    `defaultBaseBranch: ${settings.defaultBaseBranch || DEFAULT_BASE_BRANCH}`,
    `defaultProjectId: ${settings.defaultProjectId || '-'}` +
      (defaultProject ? ` (${defaultProject.name || defaultProject.id})` : ''),
    `selfLogForwarding: ${selfLogForwarding.enabled ? 'enabled' : 'disabled'} (${selfLogForwarding.levels.join('/')})`,
  ];

  const inline = buildSettingsKeyboard();

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

function buildSettingsKeyboard() {
  return new InlineKeyboard()
    .text('📣 Bot log alerts', 'gsettings:bot_log_alerts')
    .row()
    .text('📶 Ping test', 'gsettings:ping_test')
    .row()
    .text('✏️ Change default base branch', 'gsettings:change_default_base')
    .row()
    .text('🧹 Clear default base branch', 'gsettings:clear_default_base')
    .row()
    .text('🧹 Clear default project', 'gsettings:clear_default_project')
    .row()
    .text('⬅️ Back', 'gsettings:back');
}

function buildSelfLogAlertsView(settings) {
  const forwarding = getEffectiveSelfLogForwarding(settings);
  const levelsLabel = forwarding.levels.length ? forwarding.levels.join(' / ') : 'error';
  const selected = new Set(forwarding.levels);
  const lines = [
    '📣 Bot log alerts',
    '',
    `Status: ${forwarding.enabled ? 'Enabled' : 'Disabled'}`,
    `Levels: ${levelsLabel}`,
  ];

  const inline = new InlineKeyboard()
    .text(forwarding.enabled ? '✅ Enabled' : '🚫 Disabled', 'gsettings:bot_log_toggle')
    .row()
    .text(`❗ Errors: ${selected.has('error') ? 'ON' : 'OFF'}`, 'gsettings:bot_log_level:error')
    .text(`⚠️ Warnings: ${selected.has('warn') ? 'ON' : 'OFF'}`, 'gsettings:bot_log_level:warn')
    .row()
    .text(`ℹ️ Info: ${selected.has('info') ? 'ON' : 'OFF'}`, 'gsettings:bot_log_level:info')
    .row()
    .text('⬅️ Back', 'gsettings:menu');

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderSelfLogAlerts(ctx) {
  const settings = await loadGlobalSettings();
  const view = buildSelfLogAlertsView(settings);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function runPingTest(ctx) {
  const parts = [];
  try {
    const gh = await measureGithubLatency();
    parts.push(`GitHub API: ~${gh} ms`);
  } catch (error) {
    console.error('GitHub ping failed', error);
    parts.push('GitHub API: failed');
  }

  try {
    const projects = await loadProjects();
    const globalSettings = await loadGlobalSettings();
    if (projects.length) {
      const effectiveBaseBranch = projects[0].baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
      const start = Date.now();
      await fetchDryRun(projects[0], effectiveBaseBranch);
      parts.push(`git fetch (first project): ~${Date.now() - start} ms`);
    } else {
      parts.push('git fetch: no projects yet');
    }
  } catch (error) {
    console.error('Git fetch ping failed', error);
    parts.push('git fetch: failed');
  }

  await renderOrEdit(ctx, parts.join('\n'), { reply_markup: buildSettingsKeyboard() });
}

async function setDefaultProject(projectId) {
  const settings = await loadGlobalSettings();
  settings.defaultProjectId = projectId;
  await saveGlobalSettings(settings);
}

async function clearDefaultProject() {
  const settings = await loadGlobalSettings();
  settings.defaultProjectId = undefined;
  await saveGlobalSettings(settings);
}

async function clearDefaultBaseBranch() {
  const settings = await loadGlobalSettings();
  delete settings.defaultBaseBranch;
  await saveGlobalSettings(settings);
}

async function updateProjectField(projectId, field, value) {
  const projects = await loadProjects();
  const idx = projects.findIndex((p) => p.id === projectId);
  if (idx === -1) {
    return false;
  }
  projects[idx] = { ...projects[idx], [field]: value };
  await saveProjects(projects);
  return true;
}

async function clearProjectCommands(projectId) {
  const projects = await loadProjects();
  const idx = projects.findIndex((p) => p.id === projectId);
  if (idx === -1) {
    return false;
  }
  projects[idx] = {
    ...projects[idx],
    startCommand: undefined,
    testCommand: undefined,
    diagnosticCommand: undefined,
  };
  await saveProjects(projects);
  return true;
}

function requestUrl(method, targetUrl, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const isHttps = url.protocol === 'https:';
    const lib = isHttps ? https : http;
    const options = {
      method,
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: `${url.pathname}${url.search}`,
      headers: body
        ? {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
          }
        : undefined,
    };

    const start = Date.now();
    const req = lib.request(options, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve({ status: res.statusCode, body: data, durationMs: Date.now() - start });
      });
    });

    req.on('error', reject);
    req.setTimeout(15000, () => {
      req.destroy(new Error('Request timed out'));
    });

    if (body) {
      req.write(body);
    }
    req.end();
  });
}

function getPublicBaseUrl() {
  return (
    process.env.PUBLIC_BASE_URL ||
    process.env.RENDER_EXTERNAL_URL ||
    `http://localhost:${port}`
  );
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > 5 * 1024 * 1024) {
        req.destroy();
      }
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function truncateText(value, limit) {
  if (!value) return '';
  const text = String(value);
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 1))}…`;
}

function parseProjectLogPayload(rawBody) {
  const now = new Date().toISOString();
  if (!rawBody) {
    return { level: 'error', message: '(no message)', timestamp: now };
  }
  try {
    const parsed = JSON.parse(rawBody);
    if (parsed && typeof parsed === 'object') {
      const level = normalizeLogLevel(parsed.level) || 'error';
      const message = parsed.message != null ? String(parsed.message) : '(no message)';
      return {
        level,
        message,
        stack: parsed.stack ? String(parsed.stack) : '',
        context: parsed.context,
        timestamp: parsed.timestamp || now,
        source: parsed.source,
      };
    }
    return { level: 'error', message: truncateText(parsed, 1000), timestamp: now };
  } catch (error) {
    return {
      level: 'error',
      message: truncateText(rawBody, 1000),
      timestamp: now,
    };
  }
}

function parseCronAlertPayload(rawBody) {
  if (!rawBody) {
    return { level: 'error', message: '(no message)' };
  }
  try {
    const parsed = JSON.parse(rawBody);
    if (parsed && typeof parsed === 'object') {
      const rawLevel = String(parsed.level || parsed.severity || 'error').toLowerCase();
      const level = ['info', 'warning', 'error'].includes(rawLevel) ? rawLevel : 'error';
      return {
        level,
        message: parsed.message != null ? String(parsed.message) : '(no message)',
        jobId: parsed.jobId ? String(parsed.jobId) : null,
        time: parsed.time != null ? String(parsed.time) : null,
      };
    }
  } catch (error) {
    console.error('[cron-alert] Failed to parse JSON body', error);
  }
  return { level: 'error', message: String(rawBody) };
}

function formatProjectLogMessage(project, event) {
  const projectLabel = project.name || project.id;
  const lines = [
    `⚠️ [${event.level.toUpperCase()}] ${projectLabel}`,
    `Time: ${event.timestamp || new Date().toISOString()}`,
    `Source: ${event.source || 'external'}`,
    `Message: ${truncateText(event.message, 1200) || '(no message)'}`,
  ];

  if (event.stack) {
    lines.push(`Stack: ${truncateText(event.stack, 800)}`);
  }

  if (event.context && typeof event.context === 'object') {
    const contextSnippet = truncateText(JSON.stringify(event.context), 600);
    if (contextSnippet) {
      lines.push(`Context: ${contextSnippet}`);
    }
  }

  return lines.join('\n');
}

function parseRenderErrorPayload(body) {
  if (!body) {
    return { message: '', level: '' };
  }
  try {
    const parsed = JSON.parse(body);
    return {
      message: parsed.message || parsed.error || JSON.stringify(parsed).slice(0, 1000),
      level: parsed.level || parsed.severity || '',
    };
  } catch (error) {
    return {
      message: body.slice(0, 1000),
      level: '',
    };
  }
}

function downloadTelegramFile(ctx, fileId) {
  return ctx.api.getFile(fileId).then((file) => {
    const fileUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${file.file_path}`;
    return new Promise((resolve, reject) => {
      https
        .get(fileUrl, (res) => {
          if (res.statusCode !== 200) {
            reject(new Error(`Failed to download file: ${res.statusCode}`));
            return;
          }
          let data = '';
          res.setEncoding('utf8');
          res.on('data', (chunk) => {
            data += chunk;
          });
          res.on('end', () => resolve(data));
        })
        .on('error', reject);
    });
  });
}

function buildPrBody(patchText) {
  const preview = patchText.split('\n').slice(0, 20).join('\n');
  return `Automated patch at ${new Date().toISOString()}\n\nPreview:\n\n${preview}`;
}

bot.catch(async (err) => {
  console.error('Bot error:', err);
  await forwardSelfLog('error', 'Bot error encountered', {
    stack: err?.stack,
    context: { error: err?.message },
  });
});

const port = process.env.PORT || 3000;

async function initializeConfig() {
  try {
    await loadProjects();
    await loadGlobalSettings();
    await loadCronSettings();
  } catch (error) {
    console.error('Failed to load initial configuration', error);
  }
}

function startHttpServer() {
  return new Promise((resolve) => {
    http
      .createServer(async (req, res) => {
        const url = new URL(req.url, `http://${req.headers.host}`);
        if (req.method === 'GET' && url.pathname.startsWith('/keep-alive/')) {
          const projectId = decodeURIComponent(url.pathname.split('/')[2] || '');
          const project = projectId ? await getProjectById(projectId) : null;
          if (!project) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, error: 'Project not found.' }));
            return;
          }
          if (!project.renderServiceUrl) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, error: 'renderServiceUrl not configured.' }));
            return;
          }
          const start = Date.now();
          try {
            const response = await requestUrl('GET', project.renderServiceUrl);
            const durationMs = Date.now() - start;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true, status: response.status, durationMs }));
          } catch (error) {
            const durationMs = Date.now() - start;
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, status: null, durationMs, error: error.message }));
            await bot.api.sendMessage(
              ADMIN_TELEGRAM_ID,
              `Render keep-alive FAILED for project ${project.name || project.id}.`,
            );
          }
          return;
        }

        if (req.method === 'POST' && url.pathname.startsWith('/render-error-hook/')) {
          const projectId = decodeURIComponent(url.pathname.split('/')[2] || '');
          const body = await readRequestBody(req);
          const { message, level } = parseRenderErrorPayload(body);
          const timestamp = new Date().toISOString();
          const text = `⚠️ Render error for project ${projectId} at ${timestamp}.\nLevel: ${
            level || '-'
          }\nMessage: ${message || '-'}`;
          await bot.api.sendMessage(ADMIN_TELEGRAM_ID, text);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true }));
          return;
        }

        if (req.method === 'POST' && url.pathname.startsWith('/project-log/')) {
          try {
            const projectId = decodeURIComponent(url.pathname.split('/')[2] || '');
            const projects = await loadProjects();
            const project = findProjectById(projects, projectId);
            if (!project) {
              res.writeHead(404, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: false, error: 'Unknown projectId' }));
              return;
            }

            const rawBody = await readRequestBody(req);
            const event = parseProjectLogPayload(rawBody);
            const forwarding = getEffectiveProjectLogForwarding(project);

            if (forwarding.enabled !== true) {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: true, forwarded: false, reason: 'disabled' }));
              return;
            }

            let allowedLevels = normalizeLogLevels(forwarding.levels).filter((level) =>
              LOG_LEVELS.includes(level),
            );
            if (!allowedLevels.length) {
              allowedLevels = ['error'];
            }

            if (!allowedLevels.includes(event.level)) {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: true, forwarded: false, reason: 'level filtered' }));
              return;
            }

            const targetChatId = forwarding.targetChatId || ADMIN_TELEGRAM_ID;
            const message = formatProjectLogMessage(project, event);
            await bot.api.sendMessage(targetChatId, message, {
              disable_web_page_preview: true,
            });
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true, forwarded: true }));
          } catch (error) {
            console.error('[project-log] Failed to process log event', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, error: 'internal error' }));
          }
          return;
        }

        if (req.method === 'POST' && url.pathname.startsWith('/cron-alert/')) {
          try {
            const projectId = decodeURIComponent(url.pathname.split('/')[2] || '');
            const projects = await loadProjects();
            const project = findProjectById(projects, projectId);
            if (!project) {
              res.writeHead(404, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: false, error: 'Unknown project' }));
              return;
            }

            const rawBody = await readRequestBody(req);
            const event = parseCronAlertPayload(rawBody);

            if (project.cronNotificationsEnabled !== true) {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: true, skipped: true }));
              return;
            }

            if (
              Array.isArray(project.cronNotificationsLevels) &&
              !project.cronNotificationsLevels
                .map((level) => String(level).toLowerCase())
                .includes(event.level)
            ) {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: true, skipped: true }));
              return;
            }

            const messageLines = [
              `⏱ Cron alert for project ${project.name || project.id} (id: ${project.id})`,
              `Level: ${event.level}`,
            ];
            if (event.jobId) {
              messageLines.push(`Job: ${event.jobId}`);
            }
            if (event.time) {
              messageLines.push(`Time: ${event.time}`);
            }
            messageLines.push('', truncateText(event.message, 1000));

            await bot.api.sendMessage(ADMIN_TELEGRAM_ID, messageLines.join('\n'), {
              disable_web_page_preview: true,
            });

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
          } catch (error) {
            console.error('[cron-alert] Failed to process alert', error);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true, error: 'processing failed' }));
          }
          return;
        }

        // Example:
        // POST https://path-applier.onrender.com/project-error/daily-system
        // Body (JSON):
        // { "level": "error", "message": "Failed to apply XP change", "stack": "Error: ...", "meta": { "userId": 123 } }
        if (req.method === 'POST' && url.pathname.startsWith('/project-error/')) {
          const projectId = decodeURIComponent(url.pathname.split('/')[2] || '');
          const rawBody = await readRequestBody(req);
          let body = null;
          if (rawBody) {
            try {
              body = JSON.parse(rawBody);
            } catch (error) {
              body = rawBody;
            }
          }

          const now = new Date().toISOString();
          let summary = `⚠️ Project error\nProject: ${projectId}\nTime: ${now}\n`;

          if (body && typeof body === 'object') {
            const level = body.level || body.severity || 'error';
            const message = body.message || body.error || '(no message)';
            const stack = body.stack || body.trace || '';

            summary += `Level: ${level}\nMessage: ${message}\n`;

            if (stack) {
              const lines = String(stack).split('\n').slice(0, 10).join('\n');
              summary += `Stack (first lines):\n${lines}\n`;
            }

            if (body.meta) {
              const metaStr = JSON.stringify(body.meta).slice(0, 500);
              summary += `Meta: ${metaStr}\n`;
            }
          } else if (typeof body === 'string') {
            summary += `Body: ${body.slice(0, 1000)}\n`;
          } else {
            summary += 'Body: (no JSON / text body)\n';
          }

          try {
            await bot.api.sendMessage(ADMIN_TELEGRAM_ID, summary);
          } catch (error) {
            console.error('[project-error] Failed to send Telegram notification', error);
          }

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true }));
          return;
        }

        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Path Applier is running.\n');
      })
      .listen(port, () => {
        console.log(`HTTP health server listening on port ${port}`);
        resolve();
      });
  });
}

async function startBotPolling() {
  if (botStarted) {
    console.log('[Path Applier] bot.start() already called, skipping.');
    return;
  }
  botStarted = true;
  if (botRetryTimeout) {
    clearTimeout(botRetryTimeout);
    botRetryTimeout = null;
  }

  try {
    await bot.start();
    console.log('[Path Applier] Bot polling started.');
  } catch (error) {
    botStarted = false;
    if (
      error?.error_code === 409 &&
      typeof error.description === 'string' &&
      error.description.includes('terminated by other getUpdates request')
    ) {
      console.error(
        '[Path Applier] Telegram returned 409 (another getUpdates in progress). Will retry in 15 seconds.',
      );
      if (!botRetryTimeout) {
        botRetryTimeout = setTimeout(() => {
          botRetryTimeout = null;
          startBotPolling().catch((retryError) => {
            console.error(
              '[Path Applier] Retry failed:',
              retryError?.stack || retryError,
            );
          });
        }, 15000);
      }
      return;
    }
    console.error('[Path Applier] Failed to start bot polling:', error?.stack || error);
    throw error;
  }
}

async function startBot() {
  await startHttpServer();
  await testConfigDbConnection();
  await initializeConfig();
  try {
    await bot.api.deleteWebhook({ drop_pending_updates: false });
    console.log('[Path Applier] Webhook deleted (if any). Using long polling.');
  } catch (error) {
    console.error('[Path Applier] Failed to delete webhook:', error?.stack || error);
  }
  await startBotPolling();
}

startBot().catch((error) => {
  console.error('Failed to start bot', error?.stack || error);
});
