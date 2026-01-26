require('dotenv').config();

const http = require('http');
const https = require('https');
const fs = require('fs/promises');
const path = require('path');
const { Bot, InlineKeyboard, Keyboard } = require('grammy');
const { Pool } = require('pg');

const { loadProjects, saveProjects, findProjectById } = require('./projectsStore');
const { setUserState, getUserState, clearUserState } = require('./state');
const {
  ensureDefaultEnvVarSet,
  listEnvVarKeys,
  listEnvVars,
  getEnvVarRecord,
  getEnvVarValue,
  upsertEnvVar,
  deleteEnvVar,
} = require('./envVaultStore');
const {
  getProjectTelegramBot,
  getTelegramBotToken,
  upsertTelegramBotToken,
  clearTelegramBotToken,
  updateTelegramWebhook,
  updateTelegramTestStatus,
} = require('./telegramBotStore');
const { getMasterKeyStatus, MASTER_KEY_ERROR_MESSAGE } = require('./envVaultCrypto');
const {
  setWebhook,
  getWebhookInfo,
  sendMessage,
  sendSafeMessage,
} = require('./telegramApi');
const { listCronJobLinks, getCronJobLink, upsertCronJobLink } = require('./cronJobLinksStore');
const { QUICK_KEYS, getProjectTypeTemplate, getProjectTypeOptions } = require('./envVaultTemplates');
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
const { createPullRequest } = require('./githubUtils');
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
const { listSelfLogs, getSelfLogById } = require('./loggerStore');
const {
  DEFAULT_SETTINGS: DEFAULT_LOG_ALERT_SETTINGS,
  getProjectLogSettings,
  upsertProjectLogSettings,
  addRecentLog,
  listRecentLogs,
  getRecentLogById,
} = require('./logIngestStore');
const { createLogIngestService, formatContext } = require('./logIngestService');
const {
  CRON_API_TOKEN,
  listJobs,
  getJob,
  createJob,
  updateJob,
  toggleJob,
  deleteJob,
} = require('./src/cronClient');
const { buildCb, resolveCallbackData, sanitizeReplyMarkup } = require('./callbackData');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID;
const PORT = Number(process.env.PORT || 3000);
const SUPABASE_ENV_VAULT_PROJECT_ID = 'supabase_connections';
const SUPABASE_MESSAGE_LIMIT = 3500;
const SUPABASE_ROWS_PAGE_SIZE = 20;
const SUPABASE_CELL_TRUNCATE_LIMIT = 120;
const SUPABASE_QUERY_TIMEOUT_MS = 5000;

if (!BOT_TOKEN) {
  throw new Error('BOT_TOKEN is required');
}

if (!ADMIN_TELEGRAM_ID) {
  throw new Error('ADMIN_TELEGRAM_ID is required');
}

const bot = new Bot(BOT_TOKEN);
const supabasePools = new Map();
const envVaultPools = new Map();
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

const logIngestService = createLogIngestService({
  getProjectById: async (projectId) => {
    const projects = await loadProjects();
    return findProjectById(projects, projectId);
  },
  resolveProjectLogSettings: async (project) => getProjectLogSettingsWithDefaults(project.id),
  addRecentLog,
  sendTelegramMessage: async (chatId, text) =>
    bot.api.sendMessage(chatId, text, { disable_web_page_preview: true }),
  logger: console,
});

const runtimeStatus = {
  configDbOk: false,
  configDbError: null,
};
const CRON_RATE_LIMIT_MESSAGE = 'Cron API rate limit reached. Please wait a bit and try again.';
const CRON_JOBS_CACHE_TTL_MS = 30_000;
let lastCronJobsCache = null;
let lastCronJobsFetchedAt = 0;
const TELEGRAM_WEBHOOK_PATH_PREFIX = '/webhook';

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

function normalizeProjectLogSettings(settings) {
  const payload = settings || {};
  const levels = normalizeLogLevels(payload.levels);
  return {
    enabled: typeof payload.enabled === 'boolean' ? payload.enabled : DEFAULT_LOG_ALERT_SETTINGS.enabled,
    levels: levels.length ? levels : [...DEFAULT_LOG_ALERT_SETTINGS.levels],
    destinationChatId: payload.destinationChatId ? String(payload.destinationChatId) : null,
  };
}

async function getProjectLogSettingsWithDefaults(projectId) {
  const settings = await getProjectLogSettings(projectId);
  if (!settings) {
    return { ...DEFAULT_LOG_ALERT_SETTINGS };
  }
  return normalizeProjectLogSettings(settings);
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

function normalizeTelegramExtra(extra) {
  if (!extra) return extra;
  const payload = { ...extra };
  if (payload.reply_markup?.inline_keyboard) {
    sanitizeReplyMarkup(payload.reply_markup);
  }
  return payload;
}

function isButtonDataInvalidError(error) {
  const message = error?.description || error?.response?.description || error?.message || '';
  return message.includes('BUTTON_DATA_INVALID');
}

function isMessageNotModifiedError(error) {
  const message = error?.description || error?.response?.description || error?.message || '';
  return message.includes('message is not modified');
}

async function handleTelegramUiError(ctx, error, fallbackText) {
  if (!isButtonDataInvalidError(error)) {
    throw error;
  }
  console.error('[UI] BUTTON_DATA_INVALID', {
    message: error?.description || error?.message,
  });
  if (ctx?.reply) {
    await ctx.reply(
      fallbackText ||
        'UI render error: invalid button payload. Please /start and try again.',
    );
  }
}

async function ensureAnswerCallback(ctx, options) {
  if (!ctx?.callbackQuery || typeof ctx.answerCallbackQuery !== 'function') {
    return;
  }
  try {
    await ctx.answerCallbackQuery(options);
  } catch (error) {
    console.error('[UI] Failed to answer callback query', error);
  }
}

function normalizeTelegramExtraForRespond(extra) {
  if (!extra) return extra;
  const payload = normalizeTelegramExtra(extra);
  if (payload?.entities && payload?.parse_mode) {
    delete payload.parse_mode;
  }
  if (payload?.parse_mode == null) {
    delete payload.parse_mode;
  }
  return payload;
}

function getChatIdFromCtx(ctx) {
  return ctx?.chat?.id || ctx?.callbackQuery?.message?.chat?.id || null;
}

async function replySafely(ctx, text, extra) {
  if (ctx?.reply) {
    try {
      return await ctx.reply(text, extra);
    } catch (error) {
      if (isButtonDataInvalidError(error)) {
        await handleTelegramUiError(ctx, error);
        return;
      }
      throw error;
    }
  }
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) {
    console.error('[UI] Unable to reply: missing chat id.');
    return;
  }
  try {
    return await bot.api.sendMessage(chatId, text, extra);
  } catch (error) {
    if (isButtonDataInvalidError(error)) {
      await handleTelegramUiError({ reply: (...args) => bot.api.sendMessage(chatId, ...args) }, error);
      return;
    }
    throw error;
  }
}

async function respond(ctx, text, extra) {
  if (!ctx) {
    console.error('[UI] respond called without ctx');
    return;
  }
  const safeExtra = normalizeTelegramExtraForRespond(extra);
  if (ctx.callbackQuery && ctx.callbackQuery.message && ctx.editMessageText) {
    try {
      return await ctx.editMessageText(text, safeExtra);
    } catch (err) {
      if (isMessageNotModifiedError(err)) {
        await ensureAnswerCallback(ctx);
        return;
      }
      if (isButtonDataInvalidError(err)) {
        await handleTelegramUiError(ctx, err);
        return;
      }
      console.error('[UI] editMessageText failed, fallback to reply', err);
    }
  }
  return replySafely(ctx, text, safeExtra);
}

async function renderOrEdit(ctx, text, extra) {
  return respond(ctx, text, extra);
}

function buildCronCorrelationId() {
  return `CRON-${Math.random().toString(36).slice(2, 8).toUpperCase()}`;
}

function extractCronApiErrorReason(error) {
  if (!error?.body) return '';
  const raw = String(error.body).trim();
  if (!raw) return '';
  try {
    const parsed = JSON.parse(raw);
    if (parsed?.message) return String(parsed.message);
    if (parsed?.error) return String(parsed.error);
    if (Array.isArray(parsed?.errors) && parsed.errors.length) {
      const entry = parsed.errors[0];
      if (entry?.message) return String(entry.message);
      if (entry?.error) return String(entry.error);
      return String(entry);
    }
  } catch (parseError) {
    return truncateText(raw, 200);
  }
  return truncateText(raw, 200);
}

function logCronApiError({ operation, error, userId, projectId, correlationId }) {
  const responseBody = truncateText(error?.body ?? '', 500);
  console.error('[cron] API error', {
    correlationId,
    operation,
    method: error?.method,
    path: error?.path,
    status: error?.status,
    responseBody,
    userId,
    projectId,
  });
}

function formatCronApiErrorMessage({ error, hint, correlationId }) {
  const lines = ['❌ Cron API error'];
  if (error?.status) {
    lines.push(`Status: ${error.status}`);
  }
  const reason = extractCronApiErrorReason(error);
  if (reason) {
    lines.push(`Reason: ${reason}`);
  }
  if (hint) {
    lines.push(`Hint: ${hint}`);
  }
  if (correlationId) {
    lines.push(`Ref: ${correlationId}`);
  }
  return lines.join('\n');
}

function formatCronApiErrorNotice(prefix, error, correlationId) {
  const reason = extractCronApiErrorReason(error);
  const statusSuffix = error?.status ? ` (status ${error.status})` : '';
  const detail = reason || error?.message || 'request failed';
  return `${prefix}${statusSuffix}: ${detail}\nRef: ${correlationId}`;
}

function isCronUrlValidationError(error) {
  if (!error) return false;
  if (error.status === 422) return true;
  const reason = extractCronApiErrorReason(error).toLowerCase();
  return reason.includes('url') || reason.includes('uri') || reason.includes('http');
}

function validateCronUrlInput(value) {
  const raw = value?.trim();
  if (!raw) {
    return { valid: false, message: 'URL is required.' };
  }
  if (raw.length > 2048) {
    return { valid: false, message: 'URL is too long (max 2048 characters).' };
  }
  let parsed;
  try {
    parsed = new URL(raw);
  } catch (error) {
    return { valid: false, message: 'URL must be a valid http/https address.' };
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    return { valid: false, message: 'URL must start with http:// or https://.' };
  }
  if (!parsed.hostname) {
    return { valid: false, message: 'URL must include a hostname.' };
  }
  return { valid: true, url: raw };
}

function validateCronExpression(raw) {
  const parts = raw.trim().split(/\s+/);
  if (parts.length !== 5) {
    return { valid: false, message: 'Cron expression must have 5 fields.' };
  }
  const fieldPattern = /^[\d*/,\-?]+$/;
  if (!parts.every((part) => part && fieldPattern.test(part))) {
    return { valid: false, message: 'Cron expression has invalid characters.' };
  }
  return { valid: true };
}

function maskSecretValue(value) {
  if (!value) return '********';
  return '********';
}

function normalizeEnvKeyInput(value) {
  return String(value || '').trim().toUpperCase().replace(/\s+/g, '_');
}

function detectProjectType(project) {
  const tags = Array.isArray(project?.tags) ? project.tags.map((tag) => String(tag).toLowerCase()) : [];
  const haystack = [
    project?.repoSlug,
    project?.repoUrl,
    project?.name,
    project?.id,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  const matchTag = (value) => tags.includes(value);
  if (matchTag('node-bot') || matchTag('bot') || /bot/.test(haystack)) {
    return 'node-bot';
  }
  if (matchTag('node-api') || matchTag('api') || /api/.test(haystack)) {
    return 'node-api';
  }
  if (matchTag('python') || /python|py/.test(haystack)) {
    return 'python';
  }
  if (matchTag('node') || /node/.test(haystack)) {
    return 'node-api';
  }
  return 'generic';
}

function resolveProjectType(project) {
  const explicit = project?.projectType || project?.project_type;
  if (explicit && explicit !== 'other') {
    return explicit;
  }
  return detectProjectType(project);
}

function parseEnvVaultImportText(text) {
  const lines = text.split(/\r?\n/);
  const entries = [];
  const skipped = [];
  let index = 0;

  while (index < lines.length) {
    const rawLine = lines[index];
    const trimmed = rawLine.trim();
    index += 1;
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const normalizedLine = trimmed.replace(/^export\s+/, '');
    const eqIndex = normalizedLine.indexOf('=');
    if (eqIndex === -1) {
      skipped.push({ line: trimmed, reason: 'invalid_format' });
      continue;
    }
    const rawKey = normalizedLine.slice(0, eqIndex).trim();
    if (!rawKey) {
      skipped.push({ line: trimmed, reason: 'missing_key' });
      continue;
    }
    let value = normalizedLine.slice(eqIndex + 1);
    if (value == null) {
      skipped.push({ line: trimmed, reason: 'missing_value' });
      continue;
    }

    const trimmedValue = value.trimStart();
    const tripleQuote = trimmedValue.startsWith('"""')
      ? '"""'
      : trimmedValue.startsWith("'''")
        ? "'''"
        : null;

    if (tripleQuote) {
      let chunk = trimmedValue.slice(3);
      const collected = [];
      let closed = false;
      while (true) {
        const closeIndex = chunk.indexOf(tripleQuote);
        if (closeIndex !== -1) {
          collected.push(chunk.slice(0, closeIndex));
          closed = true;
          break;
        }
        collected.push(chunk);
        if (index >= lines.length) {
          break;
        }
        chunk = lines[index];
        index += 1;
      }
      if (!closed) {
        skipped.push({ line: trimmed, reason: 'unterminated_triple_quote' });
        continue;
      }
      value = collected.join('\n');
    } else {
      value = value.trim();
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
    }

    value = value.replace(/\\n/g, '\n');
    entries.push({ key: normalizeEnvKeyInput(rawKey), value });
  }

  return { entries, skipped };
}

function isSupabaseUrl(value) {
  return typeof value === 'string' && value.startsWith('https://') && value.includes('.supabase.co');
}

function validateEnvValue(key, value) {
  const trimmed = String(value || '').trim();
  if (!trimmed) {
    return { valid: false, message: 'Value cannot be empty.' };
  }
  if (key === 'SUPABASE_URL' && !isSupabaseUrl(trimmed)) {
    return { valid: false, message: 'SUPABASE_URL must start with https:// and contain .supabase.co' };
  }
  if (key === 'TZ') {
    try {
      new Intl.DateTimeFormat('en-US', { timeZone: trimmed });
    } catch (error) {
      return { valid: false, message: 'TZ must be a valid IANA timezone.' };
    }
  }
  return { valid: true };
}

function getProjectTypeLabel(project) {
  const type = resolveProjectType(project);
  const option = getProjectTypeOptions().find((entry) => entry.id === type);
  return option ? option.label : type;
}

async function renderCronWizardMessage(ctx, state, text, extra) {
  if (state && !state.messageContext && ctx.callbackQuery?.message) {
    state.messageContext = {
      chatId: ctx.callbackQuery.message.chat.id,
      messageId: ctx.callbackQuery.message.message_id,
    };
  }
  const messageContext = state?.messageContext;
  const safeExtra = normalizeTelegramExtra(extra);
  if (messageContext?.chatId && messageContext?.messageId) {
    try {
      await ctx.telegram.editMessageText(
        messageContext.chatId,
        messageContext.messageId,
        undefined,
        text,
        safeExtra,
      );
      return;
    } catch (error) {
      if (isButtonDataInvalidError(error)) {
        await handleTelegramUiError(ctx, error);
        return;
      }
      console.error('[UI] editMessageText failed, fallback to reply', error);
    }
  }
  const message = await renderOrEdit(ctx, text, safeExtra);
  if (message?.chat?.id && message?.message_id && state) {
    state.messageContext = { chatId: message.chat.id, messageId: message.message_id };
  }
}

async function renderStateMessage(ctx, state, text, extra) {
  const safeExtra = normalizeTelegramExtra(extra);
  const messageContext = state?.messageContext;
  if (messageContext?.chatId && messageContext?.messageId) {
    try {
      await ctx.telegram.editMessageText(
        messageContext.chatId,
        messageContext.messageId,
        undefined,
        text,
        safeExtra,
      );
      return;
    } catch (error) {
      if (isButtonDataInvalidError(error)) {
        await handleTelegramUiError(ctx, error);
        return;
      }
      console.error('[UI] editMessageText failed, fallback to reply', error);
    }
  }
  await renderOrEdit(ctx, text, safeExtra);
}

function setCronWizardStep(state, wizardStep, flowStep) {
  if (!state) return;
  state.step = wizardStep;
  if (state.temp && flowStep) {
    state.temp.flowStep = flowStep;
  }
}

function buildCronWizardErrorKeyboard() {
  return new InlineKeyboard()
    .text('🔁 Retry step', 'cronwiz:retry')
    .row()
    .text('✏️ Change URL', 'cronwiz:change:url')
    .text('🕒 Change schedule', 'cronwiz:change:schedule')
    .row()
    .text('✍️ Change name', 'cronwiz:change:name')
    .text('❌ Cancel', 'cronwiz:cancel');
}

function isCronRateLimitError(error) {
  if (!error) return false;
  if (error.status === 429) return true;
  return /rate limit/i.test(error.message || '');
}

async function renderCronRateLimitIfNeeded(ctx, error, extra, correlationId) {
  if (!isCronRateLimitError(error)) return false;
  const message = correlationId ? `${CRON_RATE_LIMIT_MESSAGE}\nRef: ${correlationId}` : CRON_RATE_LIMIT_MESSAGE;
  await renderOrEdit(ctx, message, extra);
  return true;
}

async function replyCronRateLimitIfNeeded(ctx, error, correlationId) {
  if (!isCronRateLimitError(error)) return false;
  const message = correlationId ? `${CRON_RATE_LIMIT_MESSAGE}\nRef: ${correlationId}` : CRON_RATE_LIMIT_MESSAGE;
  await ctx.reply(message);
  return true;
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

function wrapCallbackHandler(handler, label) {
  return async (ctx) => {
    try {
      await handler(ctx);
    } catch (error) {
      console.error(`[callback] ${label || 'handler'} failed`, error);
      await ensureAnswerCallback(ctx);
      try {
        await respond(ctx, '⚠️ Something went wrong. Please try again.');
      } catch (replyError) {
        console.error('[callback] Failed to notify user', replyError);
      }
    }
  };
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

async function getCronStatusLine(ctx, cronSettings) {
  if (!cronSettings?.enabled) {
    return 'Cron: disabled (settings).';
  }
  if (!CRON_API_TOKEN) {
    return 'Cron: disabled (no API token).';
  }
  try {
    await fetchCronJobs();
    return 'Cron: ✅ API OK.';
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'list',
      error,
      userId: ctx?.from?.id,
      projectId: null,
      correlationId,
    });
    return `Cron: ⚠️ API error: ${truncateText(error.message, 80)} (Ref: ${correlationId})`;
  }
}

async function testConfigDbConnection() {
  const status = await checkConfigDbStatus();
  if (status.ok) {
    runtimeStatus.configDbOk = true;
    runtimeStatus.configDbError = null;
    console.log('Config DB: OK');
    return status;
  }
  runtimeStatus.configDbOk = false;
  runtimeStatus.configDbError = status.message || 'see logs';
  if (status.message === 'not configured') {
    const errorMessage = 'Startup aborted: PATH_APPLIER_CONFIG_DSN not set.';
    console.error(errorMessage);
    await forwardSelfLog('error', 'Config DB missing PATH_APPLIER_CONFIG_DSN', {
      context: { error: status.message },
    });
    throw new Error(errorMessage);
  }
  console.error('Config DB connection failed', status.message);
  await forwardSelfLog('error', 'Config DB connection failed', {
    context: { error: status.message },
  });
  throw new Error(`Startup aborted: Config DB connection failed (${status.message})`);
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

bot.callbackQuery('cancel_input', wrapCallbackHandler(async (ctx) => {
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
  await ensureAnswerCallback(ctx);
  try {
    await ctx.editMessageText('Operation cancelled.', {
      reply_markup: buildBackKeyboard(backTarget),
    });
  } catch (error) {
    // Ignore edit failures (old message, etc.)
  }
}, 'cancel_input'));

bot.callbackQuery('patch:cancel', wrapCallbackHandler(async (ctx) => {
  clearPatchSession(ctx.from.id);
  await ensureAnswerCallback(ctx);
  try {
    await ctx.editMessageText('Patch input cancelled.');
  } catch (error) {
    // Ignore edit failures
  }
  await renderMainMenu(ctx);
}, 'patch_cancel'));

bot.callbackQuery('patch:finish', wrapCallbackHandler(async (ctx) => {
  const session = getPatchSession(ctx.from.id);
  if (!session || !session.buffer.trim()) {
    await ctx.answerCallbackQuery({ text: 'No patch text received yet.', show_alert: true });
    return;
  }
  clearPatchSession(ctx.from.id);
  await ensureAnswerCallback(ctx);
  await handlePatchApplication(ctx, session.projectId, session.buffer);
}, 'patch_finish'));

bot.on('callback_query:data', wrapCallbackHandler(async (ctx) => {
  const resolved = await resolveCallbackData(ctx.callbackQuery.data);
  if (resolved.expired || !resolved.data) {
    await ctx.answerCallbackQuery({ text: 'Expired, please reopen the menu.', show_alert: true });
    return;
  }
  const data = resolved.data;
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
  if (data.startsWith('cronwiz:')) {
    await handleCronWizardCallback(ctx, data);
    return;
  }
  if (data.startsWith('projcron:')) {
    await handleProjectCronCallback(ctx, data);
    return;
  }
  if (data.startsWith('envvault:')) {
    await handleEnvVaultCallback(ctx, data);
    return;
  }
  if (data.startsWith('cronlink:')) {
    await handleCronLinkCallback(ctx, data);
    return;
  }
  if (data.startsWith('tgbot:')) {
    await handleTelegramBotCallback(ctx, data);
    return;
  }
}, 'callback_query:data'));

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
    case 'cron_wizard':
      await handleCronWizardInput(ctx, state);
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
    case 'env_vault_custom_key':
      await handleEnvVaultCustomKeyInput(ctx, state);
      break;
    case 'env_vault_value':
      await handleEnvVaultValueInput(ctx, state);
      break;
    case 'env_vault_import':
      await handleEnvVaultImportInput(ctx, state);
      break;
    case 'cron_link_label':
      await handleCronLinkLabelInput(ctx, state);
      break;
    case 'telegram_token_input':
      await handleTelegramTokenInput(ctx, state);
      break;
    case 'project_sql_input':
      await handleProjectSqlInput(ctx, state);
      break;
    case 'proj_log_chat_input':
      await handleProjectLogChatInput(ctx, state);
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
  await ensureAnswerCallback(ctx);
  const [, action] = data.split(':');
  if (action === 'back') {
    await renderMainMenu(ctx);
  }
}

async function handleProjectCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
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
      await renderOrEdit(
        ctx,
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
      await renderOrEdit(ctx, 'Send the new project name.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'edit_repo':
      setUserState(ctx.from.id, {
        type: 'edit_repo',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(
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
      await renderOrEdit(
        'Send new working directory (absolute path). Or send "-" to reset to default based on repo.\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    case 'edit_github_token':
      setUserState(ctx.from.id, {
        type: 'edit_github_token',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(
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
      await renderOrEdit(ctx, 'Send the new base branch.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'commands':
      await renderCommandsScreen(ctx, projectId);
      break;
    case 'project_type':
      await renderProjectTypeMenu(ctx, projectId);
      break;
    case 'project_type_set':
      await updateProjectType(ctx, projectId, extra);
      break;
    case 'cmd_edit':
      setUserState(ctx.from.id, {
        type: 'edit_command_input',
        projectId,
        field: extra,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, `Send new value for ${extra}.\n(Or press Cancel)`, {
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
      await renderOrEdit(ctx, `Send new value for ${extra}.\n(Or press Cancel)`, {
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
      await renderOrEdit(ctx, 'Send new supabaseConnectionId.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'supabase_clear':
      await updateProjectField(projectId, 'supabaseConnectionId', undefined);
      await renderProjectSettings(ctx, projectId);
      break;
    case 'sql_menu':
      await renderProjectSqlMenu(ctx, projectId);
      break;
    case 'sql_supabase':
      await startProjectSqlInput(ctx, projectId, 'supabase');
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
  await ensureAnswerCallback(ctx);
  const parts = data.split(':');
  const action = parts[1];
  const level = action === 'level' ? parts[2] : null;
  const projectId = action === 'level' ? parts[3] : parts[2];
  const page = action === 'logs' ? Number(parts[3] || 0) : Number(parts[4] || 0);
  const logId = action === 'log' ? parts[3] : null;

  if (!projectId) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  if (action === 'back') {
    await renderProjectSettings(ctx, projectId);
    return;
  }

  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  const current = await getProjectLogSettingsWithDefaults(projectId);
  const updated = {
    ...current,
  };

  if (action === 'menu') {
    await renderProjectLogAlerts(ctx, projectId);
    return;
  }

  if (action === 'toggle') {
    const nextEnabled = !current.enabled;
    updated.enabled = nextEnabled;
    if (nextEnabled && !updated.levels.length) {
      updated.levels = ['error'];
    }
  }

  if (action === 'level') {
    const normalized = normalizeLogLevel(level);
    if (normalized) {
      const levels = new Set(updated.levels);
      if (levels.has(normalized)) {
        levels.delete(normalized);
      } else {
        levels.add(normalized);
      }
      updated.levels = Array.from(levels).filter((entry) => LOG_LEVELS.includes(entry));
      if (current.enabled && updated.levels.length === 0) {
        updated.levels = ['error'];
      }
    }
  }

  if (action === 'set_chat') {
    setUserState(ctx.from.id, {
      type: 'proj_log_chat_input',
      projectId,
      messageContext: getMessageTargetFromCtx(ctx),
    });
    await renderOrEdit(ctx, 'Send destination chat_id for log alerts.\n(Or press Cancel)', {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }

  if (action === 'use_chat') {
    const chatId = ctx.chat?.id;
    if (!chatId) {
      await ctx.reply('Unable to detect current chat id.');
      return;
    }
    updated.destinationChatId = String(chatId);
  }

  if (action === 'clear_chat') {
    updated.destinationChatId = null;
  }

  if (action === 'logs') {
    await renderProjectLogList(ctx, projectId, Number.isNaN(page) ? 0 : page);
    return;
  }

  if (action === 'log') {
    await renderProjectLogDetail(ctx, projectId, logId, Number.isNaN(page) ? 0 : page);
    return;
  }

  await upsertProjectLogSettings(projectId, updated);
  await renderProjectLogAlerts(ctx, projectId);
}

async function handleGlobalSettingsCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
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
    case 'bot_logs': {
      const page = Number(parts[2]) || 0;
      await renderSelfLogList(ctx, page);
      break;
    }
    case 'bot_log': {
      const logId = parts[2];
      const page = Number(parts[3]) || 0;
      await renderSelfLogDetail(ctx, logId, page);
      break;
    }
    case 'change_default_base':
      setUserState(ctx.from.id, {
        type: 'global_change_base',
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Send new default base branch.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'ping_test':
      await runPingTest(ctx);
      break;
    case 'clear_default_base':
      await clearDefaultBaseBranch();
      await renderGlobalSettings(ctx, '✅ Default base branch cleared (using environment default).');
      break;
    case 'clear_default_project':
      await clearDefaultProject();
      await renderGlobalSettings(ctx, '✅ Default project cleared.');
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
  await ensureAnswerCallback(ctx);
  const [, action, connectionId, tableToken, extra] = data.split(':');
  switch (action) {
    case 'add':
      resetUserState(ctx);
      setUserState(ctx.from.id, {
        type: 'supabase_add',
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Send the connection as: id, name, envKey\n(Or press Cancel)', {
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
    case 'table':
      resetUserState(ctx);
      await renderSupabaseTableDetails(ctx, connectionId, decodeSupabaseTableName(tableToken));
      break;
    case 'rows':
      resetUserState(ctx);
      await renderSupabaseTableRows(
        ctx,
        connectionId,
        decodeSupabaseTableName(tableToken),
        Number(extra) || 0,
      );
      break;
    case 'count':
      resetUserState(ctx);
      await renderSupabaseTableCount(ctx, connectionId, decodeSupabaseTableName(tableToken));
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
  await ensureAnswerCallback(ctx);
  const [, action, jobId] = data.split(':');

  switch (action) {
    case 'menu':
      await renderCronMenu(ctx);
      break;
    case 'list':
      await renderCronJobList(ctx);
      break;
    case 'create':
      if (!CRON_API_TOKEN) {
        await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_TOKEN missing).', {
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
      await startCronCreateWizard(ctx, null, 'cron:menu');
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
        await toggleJob(jobId, !job?.enabled);
        clearCronJobsCache();
        await renderCronJobDetails(ctx, jobId, { backCallback: 'cron:list' });
      } catch (error) {
        const correlationId = buildCronCorrelationId();
        logCronApiError({
          operation: 'toggle',
          error,
          userId: ctx.from?.id,
          projectId: null,
          correlationId,
        });
        if (
          await renderCronRateLimitIfNeeded(ctx, error, {
            reply_markup: buildBackKeyboard('cron:menu'),
          }, correlationId)
        ) {
          return;
        }
        await renderOrEdit(
          ctx,
          formatCronApiErrorNotice('Failed to toggle cron job', error, correlationId),
          {
            reply_markup: buildBackKeyboard('cron:menu'),
          },
        );
      }
      break;
    }
    case 'edit_name':
      await promptCronNameInput(ctx, jobId, 'cron:job:' + jobId);
      break;
    case 'change_schedule':
      await startCronEditScheduleWizard(ctx, jobId, 'cron:job:' + jobId);
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
        clearCronJobsCache();
        await renderOrEdit(ctx, 'Cron job deleted.', {
          reply_markup: buildBackKeyboard('cron:menu'),
        });
      } catch (error) {
        const correlationId = buildCronCorrelationId();
        logCronApiError({
          operation: 'delete',
          error,
          userId: ctx.from?.id,
          projectId: null,
          correlationId,
        });
        if (
          await renderCronRateLimitIfNeeded(ctx, error, {
            reply_markup: buildBackKeyboard('cron:menu'),
          }, correlationId)
        ) {
          return;
        }
        await renderOrEdit(
          ctx,
          formatCronApiErrorNotice('Failed to delete cron job', error, correlationId),
          { reply_markup: buildBackKeyboard('cron:menu') },
        );
      }
      break;
    default:
      break;
  }
}

async function handleCronWizardCancel(ctx, state) {
  clearUserState(ctx.from.id);
  try {
    await ctx.editMessageText('Canceled.');
  } catch (error) {
    await ctx.reply('Canceled.');
  }
  if (state.mode === 'edit' && state.temp?.jobId) {
    await renderCronJobDetails(ctx, state.temp.jobId, {
      backCallback: state.backCallback || 'cron:list',
    });
    return;
  }
  await renderCronMenu(ctx);
}

async function handleCronWizardCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const state = getUserState(ctx.from.id);
  if (!state) {
    return;
  }
  const parts = data.split(':');
  const action = parts[1];
  const subAction = parts[2];
  const extra = parts[3];

  if (action === 'cancel') {
    if (state.type === 'cron_wizard') {
      await handleCronWizardCancel(ctx, state);
      return;
    }
    clearUserState(ctx.from.id);
    try {
      await ctx.editMessageText('Canceled.');
    } catch (error) {
      await ctx.reply('Canceled.');
    }
    await renderCronMenu(ctx);
    return;
  }

  if (action === 'retry' && state.mode === 'create') {
    if (state.step !== 'confirm') {
      await renderCronWizardMessage(ctx, state, 'Nothing to retry yet.', {
        reply_markup: buildCronWizardErrorKeyboard(),
      });
      return;
    }
    if (state.lastError?.isUrlError) {
      setCronWizardStep(state, 'url', 'url');
      state.temp.inputType = 'url';
      state.temp.urlNextStep = 'confirm';
      await renderCronWizardMessage(ctx, state, 'Send target URL (e.g. keep-alive or deploy hook).', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    await attemptCronWizardCreate(ctx, state);
    return;
  }

  if (action === 'change' && state.mode === 'create') {
    if (subAction === 'url') {
      setCronWizardStep(state, 'url', 'url');
      state.temp.inputType = 'url';
      state.temp.urlNextStep = 'confirm';
      await renderCronWizardMessage(ctx, state, 'Send target URL (e.g. keep-alive or deploy hook).', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    if (subAction === 'schedule') {
      setCronWizardStep(state, 'schedule', 'choose-pattern');
      state.temp.previousStep = 'choose-pattern';
      await renderCronWizardPatternMenu(ctx, state);
      return;
    }
    if (subAction === 'name') {
      setCronWizardStep(state, 'name', 'name');
      state.temp.inputType = 'ask-name';
      await renderCronWizardMessage(
        ctx,
        state,
        "Send job name (or type 'skip' for default). Or press Cancel.",
        { reply_markup: buildCronWizardCancelKeyboard() },
      );
      return;
    }
  }

  if (state.type !== 'cron_wizard') {
    return;
  }

  if (action === 'entry') {
    if (subAction === 'pattern') {
      setCronWizardStep(state, 'schedule', 'choose-pattern');
      state.temp.previousStep = 'edit-entry';
      await renderCronWizardPatternMenu(ctx, state);
      return;
    }
    if (subAction === 'advanced') {
      setCronWizardStep(state, 'schedule', 'advanced-menu');
      state.temp.previousStep = 'edit-entry';
      await renderCronWizardAdvancedMenu(ctx, state);
    }
    return;
  }

  if (action === 'back') {
    if (subAction === 'pattern') {
      setCronWizardStep(state, 'schedule', 'choose-pattern');
      await renderCronWizardPatternMenu(ctx, state);
      return;
    }
    if (subAction === 'weekly_days') {
      setCronWizardStep(state, 'schedule', 'pattern-weekly-days');
      await renderCronWizardWeeklyDaysMenu(ctx, state);
    }
    return;
  }

  if (action === 'pattern') {
    if (subAction === 'minutes') {
      setCronWizardStep(state, 'schedule', 'pattern-minutes');
      await renderCronWizardMinutesMenu(ctx);
      return;
    }
    if (subAction === 'hours') {
      setCronWizardStep(state, 'schedule', 'pattern-hours');
      await renderCronWizardHoursMenu(ctx);
      return;
    }
    if (subAction === 'daily') {
      setCronWizardStep(state, 'schedule', 'pattern-daily');
      await renderCronWizardDailyMenu(ctx);
      return;
    }
    if (subAction === 'weekly') {
      setCronWizardStep(state, 'schedule', 'pattern-weekly-days');
      if (!Array.isArray(state.temp.schedule.wdays) || state.temp.schedule.wdays.includes(-1)) {
        state.temp.schedule.wdays = [];
      }
      await renderCronWizardWeeklyDaysMenu(ctx, state);
      return;
    }
    if (subAction === 'advanced') {
      setCronWizardStep(state, 'schedule', 'advanced-menu');
      state.temp.previousStep = 'choose-pattern';
      await renderCronWizardAdvancedMenu(ctx, state);
    }
    return;
  }

  if (action === 'minutes') {
    if (subAction === 'custom') {
      setCronWizardStep(state, 'schedule', 'input');
      state.temp.inputType = 'custom-minutes';
      await renderCronWizardMessage(ctx, state, 'Send interval in minutes (1-720).', {
        reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:back:pattern'),
      });
      return;
    }
    const interval = Number(subAction);
    if (!Number.isFinite(interval) || interval <= 0) {
      return;
    }
    const timezone = state.temp.schedule.timezone;
    if (interval > 60) {
      if (interval % 60 !== 0) {
        await renderCronWizardMessage(ctx, state, 'Interval must be 60 or a multiple of 60 minutes.', {
          reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:back:pattern'),
        });
        return;
      }
      state.temp.schedule = buildEveryNHoursSchedule(interval / 60, timezone);
    } else {
      state.temp.schedule = buildEveryNMinutesSchedule(interval, timezone);
    }
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (action === 'hours') {
    if (subAction === 'custom') {
      setCronWizardStep(state, 'schedule', 'input');
      state.temp.inputType = 'custom-hours';
      await renderCronWizardMessage(ctx, state, 'Send interval in hours (1-24).', {
        reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:back:pattern'),
      });
      return;
    }
    const interval = Number(subAction);
    if (!Number.isFinite(interval) || interval <= 0) {
      return;
    }
    const timezone = state.temp.schedule.timezone;
    state.temp.schedule = buildEveryNHoursSchedule(interval, timezone);
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (action === 'daily') {
    if (subAction === 'custom') {
      setCronWizardStep(state, 'schedule', 'input');
      state.temp.inputType = 'custom-daily-time';
      await renderCronWizardMessage(ctx, state, 'Send time as HH:MM (24h).', {
        reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:back:pattern'),
      });
      return;
    }
    const time = parseTimeInput(`${subAction}:${extra}`);
    if (!time) {
      return;
    }
    const timezone = state.temp.schedule.timezone;
    state.temp.schedule = buildDailySchedule(time, timezone);
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (action === 'weekly_day') {
    const day = Number(subAction);
    if (!Number.isFinite(day)) return;
    const current = Array.isArray(state.temp.schedule.wdays)
      ? state.temp.schedule.wdays.filter((value) => value !== -1)
      : [];
    const selected = new Set(current);
    if (selected.has(day)) {
      selected.delete(day);
    } else {
      selected.add(day);
    }
    state.temp.schedule.wdays = uniqueSorted(Array.from(selected));
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    await renderCronWizardWeeklyDaysMenu(ctx, state);
    return;
  }

  if (action === 'weekly_done') {
    const selected = Array.isArray(state.temp.schedule.wdays)
      ? state.temp.schedule.wdays.filter((value) => value !== -1)
      : [];
    if (!selected.length) {
      await renderCronWizardMessage(ctx, state, 'Select at least one weekday.', {
        reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:back:pattern'),
      });
      return;
    }
    setCronWizardStep(state, 'schedule', 'pattern-weekly-time');
    await renderCronWizardWeeklyTimeMenu(ctx);
    return;
  }

  if (action === 'weekly_time') {
    if (subAction === 'custom') {
      setCronWizardStep(state, 'schedule', 'input');
      state.temp.inputType = 'custom-weekly-time';
      await renderCronWizardMessage(ctx, state, 'Send time as HH:MM (24h).', {
        reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:back:weekly_days'),
      });
      return;
    }
    const time = parseTimeInput(`${subAction}:${extra}`);
    if (!time) {
      return;
    }
    const selected = Array.isArray(state.temp.schedule.wdays)
      ? state.temp.schedule.wdays.filter((value) => value !== -1)
      : [];
    const timezone = state.temp.schedule.timezone;
    state.temp.schedule = buildWeeklySchedule(selected, time, timezone);
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (action === 'advanced') {
    if (subAction === 'done') {
      setCronWizardStep(state, 'confirm', 'confirm');
      state.temp.previousStep = 'advanced-menu';
      await renderCronWizardConfirm(ctx, state);
      return;
    }
    if (subAction === 'back') {
      if (state.temp.previousStep === 'edit-entry' && state.mode === 'edit') {
        setCronWizardStep(state, 'schedule', 'edit-entry');
        await renderCronWizardEditEntry(ctx, state);
      } else {
        setCronWizardStep(state, 'schedule', 'choose-pattern');
        await renderCronWizardPatternMenu(ctx, state);
      }
      return;
    }
    if (subAction === 'timezone') {
      setCronWizardStep(state, 'schedule', 'input');
      state.temp.inputType = 'timezone';
      await renderCronWizardMessage(ctx, state, 'Send timezone (e.g. Asia/Tehran).', {
        reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:advanced:back'),
      });
      return;
    }
    if (['minutes', 'hours', 'mdays', 'months', 'wdays'].includes(subAction)) {
      setCronWizardStep(state, 'schedule', 'advanced-edit-field');
      state.temp.fieldBeingEdited = subAction;
      await renderCronWizardAdvancedFieldMenu(ctx, subAction);
    }
    return;
  }

  if (action === 'field') {
    const field = subAction;
    const fieldAction = extra;
    if (!field || !fieldAction) return;
    if (fieldAction === 'all') {
      state.temp.schedule[field] = [-1];
      if (state.draft) {
        state.draft.schedule = state.temp.schedule;
      }
      await renderCronWizardAdvancedMenu(ctx, state);
      return;
    }
    if (fieldAction === 'custom') {
      setCronWizardStep(state, 'schedule', 'input');
      state.temp.inputType = 'advanced-custom-list';
      state.temp.fieldBeingEdited = field;
      const fieldLabel = field === 'mdays' ? 'month days (1-31)' : field;
      await renderCronWizardMessage(ctx, state, `Send comma-separated list for ${fieldLabel}.`, {
        reply_markup: buildCronWizardBackCancelKeyboard('cronwiz:advanced:back'),
      });
      return;
    }
    if (fieldAction === 'preset') {
      const preset = parts[4];
      if (field === 'minutes') {
        if (preset === 'every5') {
          state.temp.schedule.minutes = buildEveryNMinutesSchedule(5).minutes;
        }
        if (preset === 'every10') {
          state.temp.schedule.minutes = buildEveryNMinutesSchedule(10).minutes;
        }
      }
      if (field === 'hours') {
        if (preset === 'every2') {
          state.temp.schedule.hours = buildEveryNHoursSchedule(2).hours;
        }
        if (preset === 'work') {
          state.temp.schedule.hours = uniqueSorted(
            Array.from({ length: 9 }, (_, idx) => idx + 9),
          );
        }
      }
      if (field === 'mdays') {
        if (preset === 'first') {
          state.temp.schedule.mdays = [1];
        }
        if (preset === 'mid') {
          state.temp.schedule.mdays = [15];
        }
      }
      if (field === 'months') {
        if (preset === 'quarterly') {
          state.temp.schedule.months = [1, 4, 7, 10];
        }
      }
      if (field === 'wdays') {
        if (preset === 'weekdays') {
          state.temp.schedule.wdays = [1, 2, 3, 4, 5];
        }
        if (preset === 'weekends') {
          state.temp.schedule.wdays = [0, 6];
        }
      }
      if (state.draft) {
        state.draft.schedule = state.temp.schedule;
      }
      await renderCronWizardAdvancedMenu(ctx, state);
    }
    return;
  }

  if (action === 'confirm') {
    if (subAction === 'use') {
      if (state.mode === 'edit') {
        try {
          await updateJob(state.temp.jobId, { schedule: state.temp.schedule });
          clearCronJobsCache();
          clearUserState(ctx.from.id);
          await ctx.reply(`Cron job #${state.temp.jobId} schedule updated.`);
          await renderCronJobDetails(ctx, state.temp.jobId, {
            backCallback: state.backCallback || 'cron:list',
          });
        } catch (error) {
          const correlationId = buildCronCorrelationId();
          logCronApiError({
            operation: 'update',
            error,
            userId: ctx.from?.id,
            projectId: state.projectId,
            correlationId,
          });
          const hint = 'Please re-enter the schedule.';
          await renderCronWizardMessage(
            ctx,
            state,
            formatCronApiErrorMessage({ error, hint, correlationId }),
            { reply_markup: buildCronWizardCancelKeyboard() },
          );
        }
        return;
      }
      setCronWizardStep(state, 'name', 'name');
      state.temp.inputType = 'ask-name';
      await renderCronWizardMessage(
        ctx,
        state,
        "Send job name (or type 'skip' for default). Or press Cancel.",
        { reply_markup: buildCronWizardCancelKeyboard() },
      );
      return;
    }
    if (subAction === 'adjust') {
      if (state.temp.previousStep === 'advanced-menu') {
        setCronWizardStep(state, 'schedule', 'advanced-menu');
        await renderCronWizardAdvancedMenu(ctx, state);
      } else if (state.temp.previousStep === 'edit-entry' && state.mode === 'edit') {
        setCronWizardStep(state, 'schedule', 'edit-entry');
        await renderCronWizardEditEntry(ctx, state);
      } else {
        setCronWizardStep(state, 'schedule', 'choose-pattern');
        await renderCronWizardPatternMenu(ctx, state);
      }
    }
  }
}

async function handleCronWizardInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await renderCronWizardMessage(ctx, state, 'Please send a value or press Cancel.', {
      reply_markup: buildCronWizardCancelKeyboard(),
    });
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    await handleCronWizardCancel(ctx, state);
    return;
  }

  const inputType = state.temp.inputType;
  const timezone = state.temp.schedule?.timezone;

  if (inputType === 'url') {
    const validation = validateCronUrlInput(text);
    if (!validation.valid) {
      state.lastError = { status: null, message: validation.message, isUrlError: true };
      await renderCronWizardMessage(ctx, state, `❌ Invalid URL\nReason: ${validation.message}\nHint: Please re-enter the URL.`, {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    state.temp.url = validation.url;
    state.draft.url = validation.url;
    state.lastError = null;
    if (state.temp.urlNextStep === 'confirm' && state.temp.schedule) {
      setCronWizardStep(state, 'confirm', 'confirm');
      state.temp.inputType = null;
      await renderCronWizardMessage(ctx, state, buildCronWizardDraftSummary(state), {
        reply_markup: buildCronWizardErrorKeyboard(),
      });
      return;
    }
    setCronWizardStep(state, 'schedule', 'choose-pattern');
    state.temp.inputType = null;
    await renderCronWizardPatternMenu(ctx, state);
    return;
  }

  if (inputType === 'custom-minutes') {
    const value = Number(text);
    if (!Number.isInteger(value) || value <= 0 || value > 720) {
      await renderCronWizardMessage(ctx, state, 'Please send a valid number of minutes (1-720).', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    if (value > 60 && value % 60 !== 0) {
      await renderCronWizardMessage(ctx, state, 'Minutes over 60 must be divisible by 60.', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    state.temp.schedule =
      value > 60
        ? buildEveryNHoursSchedule(value / 60, timezone)
        : buildEveryNMinutesSchedule(value, timezone);
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (inputType === 'custom-hours') {
    const value = Number(text);
    if (!Number.isInteger(value) || value <= 0 || value > 24) {
      await renderCronWizardMessage(ctx, state, 'Please send a valid number of hours (1-24).', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    state.temp.schedule = buildEveryNHoursSchedule(value, timezone);
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (inputType === 'custom-daily-time') {
    const time = parseTimeInput(text);
    if (!time) {
      await renderCronWizardMessage(ctx, state, 'Invalid time format. Use HH:MM.', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    state.temp.schedule = buildDailySchedule(time, timezone);
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (inputType === 'custom-weekly-time') {
    const time = parseTimeInput(text);
    if (!time) {
      await renderCronWizardMessage(ctx, state, 'Invalid time format. Use HH:MM.', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    const selected = Array.isArray(state.temp.schedule.wdays)
      ? state.temp.schedule.wdays.filter((value) => value !== -1)
      : [];
    if (!selected.length) {
      await renderCronWizardMessage(ctx, state, 'Select weekdays first.', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    state.temp.schedule = buildWeeklySchedule(selected, time, timezone);
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'confirm', 'confirm');
    state.temp.previousStep = 'choose-pattern';
    await renderCronWizardConfirm(ctx, state);
    return;
  }

  if (inputType === 'advanced-custom-list') {
    const field = state.temp.fieldBeingEdited;
    const ranges = {
      minutes: { min: 0, max: 59 },
      hours: { min: 0, max: 23 },
      mdays: { min: 1, max: 31 },
      months: { min: 1, max: 12 },
      wdays: { min: 0, max: 6 },
    };
    const range = ranges[field];
    if (!range) {
      await renderCronWizardMessage(ctx, state, 'Invalid field.', {
        reply_markup: buildCronWizardCancelKeyboard(),
      });
      return;
    }
    const values = parseNumberList(text, range);
    if (!values) {
      await renderCronWizardMessage(
        ctx,
        state,
        'Invalid list. Please send comma-separated numbers.',
        { reply_markup: buildCronWizardCancelKeyboard() },
      );
      return;
    }
    state.temp.schedule[field] = values;
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
    }
    setCronWizardStep(state, 'schedule', 'advanced-menu');
    await renderCronWizardAdvancedMenu(ctx, state);
    return;
  }

  if (inputType === 'timezone') {
    state.temp.schedule.timezone = text;
    if (state.draft) {
      state.draft.schedule = state.temp.schedule;
      state.draft.timezone = text;
    }
    setCronWizardStep(state, 'schedule', 'advanced-menu');
    await renderCronWizardAdvancedMenu(ctx, state);
    return;
  }

  if (inputType === 'ask-name') {
    const name = text.toLowerCase() === 'skip' ? null : text;
    state.draft.name = name || '';
    setCronWizardStep(state, 'confirm', 'confirm');
    await attemptCronWizardCreate(ctx, state);
  }
}

async function handleProjectCronCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
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

async function handleEnvVaultCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = data.split(':');
  const action = parts[1];
  const projectId = parts[2];
  const key = parts[3];
  const extra = parts[4];

  if (!projectId) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  const envStatus = getMasterKeyStatus();
  if (!envStatus.ok) {
    await renderOrEdit(ctx, buildEnvVaultUnavailableMessage(), {
      reply_markup: buildBackKeyboard(`proj:open:${projectId}`),
    });
    return;
  }

  switch (action) {
    case 'menu':
      await renderEnvVaultMenu(ctx, projectId);
      break;
    case 'list':
      await renderEnvVaultKeyList(ctx, projectId);
      break;
    case 'key':
      await renderEnvVaultKeyDetails(ctx, projectId, key);
      break;
    case 'add':
      await renderEnvVaultQuickKeyMenu(ctx, projectId);
      break;
    case 'set_key':
      await promptEnvVaultValue(ctx, projectId, key);
      break;
    case 'set_custom':
      setUserState(ctx.from.id, {
        type: 'env_vault_custom_key',
        projectId,
        backCallback: `envvault:menu:${projectId}`,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Send the ENV key name.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'delete_menu':
      await renderEnvVaultDeleteMenu(ctx, projectId);
      break;
    case 'delete':
      await handleEnvVaultDelete(ctx, projectId, key);
      break;
    case 'clear':
      await handleEnvVaultDelete(ctx, projectId, key);
      break;
    case 'reveal':
      await revealEnvVaultValue(ctx, projectId, key);
      break;
    case 'import':
      setUserState(ctx.from.id, {
        type: 'env_vault_import',
        projectId,
        backCallback: `envvault:menu:${projectId}`,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Send KEY=VALUE lines (multi-line). Values will be encrypted.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'export':
      await renderEnvVaultExport(ctx, projectId);
      break;
    case 'recommend':
      await renderEnvVaultProjectTypeMenu(ctx, projectId);
      break;
    case 'recommend_type':
      await renderEnvVaultRecommendedOptions(ctx, projectId, key);
      break;
    case 'recommend_required':
      await startEnvVaultRecommendedSequence(ctx, projectId, key, 'required');
      break;
    case 'recommend_optional':
      await startEnvVaultRecommendedSequence(ctx, projectId, key, 'optional');
      break;
    case 'recommend_pick':
      await startEnvVaultPickMenu(ctx, projectId, key);
      break;
    case 'recommend_toggle':
      await toggleEnvVaultPickKey(ctx, projectId, key, extra);
      break;
    case 'recommend_confirm':
      await confirmEnvVaultPickKeys(ctx, projectId);
      break;
    case 'skip':
      await skipEnvVaultKey(ctx, projectId);
      break;
    case 'add_missing':
      await startEnvVaultRecommendedSequence(ctx, projectId, null, 'required');
      break;
    case 'sql':
      await startProjectSqlInput(ctx, projectId, 'env_vault');
      break;
    default:
      break;
  }
}

async function handleCronLinkCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = data.split(':');
  const action = parts[1];
  const jobId = parts[2];
  const extra = parts[3];

  switch (action) {
    case 'menu':
      await renderCronLinkMenu(ctx);
      break;
    case 'select':
      await renderCronLinkProjectPicker(ctx, jobId, getMessageTargetFromCtx(ctx));
      break;
    case 'set':
      await updateCronJobLink(ctx, jobId, extra, getMessageTargetFromCtx(ctx));
      break;
    case 'set_other':
      await updateCronJobLink(ctx, jobId, null, getMessageTargetFromCtx(ctx));
      break;
    case 'label':
      setUserState(ctx.from.id, {
        type: 'cron_link_label',
        jobId,
        backCallback: `cronlink:select:${jobId}`,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Send a custom label for this cron job.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'other':
      await renderCronJobList(ctx, { otherOnly: true });
      break;
    case 'filter_menu':
      await renderCronProjectFilterMenu(ctx);
      break;
    case 'filter':
      await renderCronJobList(ctx, { projectId: jobId || null });
      break;
    default:
      break;
  }
}

async function handleTelegramBotCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = data.split(':');
  const action = parts[1];
  const projectId = parts[2];

  if (!projectId) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  const envStatus = getMasterKeyStatus();
  if (!envStatus.ok) {
    await renderOrEdit(ctx, buildEnvVaultUnavailableMessage('Telegram setup unavailable.'), {
      reply_markup: buildBackKeyboard(`proj:open:${projectId}`),
    });
    return;
  }

  switch (action) {
    case 'menu':
      await renderTelegramSetupMenu(ctx, projectId);
      break;
    case 'set_token':
      setUserState(ctx.from.id, {
        type: 'telegram_token_input',
        projectId,
        backCallback: `tgbot:menu:${projectId}`,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Send the Telegram bot token.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'clear_token':
      await clearTelegramBotToken(projectId);
      await renderTelegramSetupMenu(ctx, projectId);
      break;
    case 'set_webhook':
      await setProjectTelegramWebhook(ctx, projectId);
      break;
    case 'test':
      await runTelegramWebhookTest(ctx, projectId);
      break;
    case 'status':
      await renderTelegramStatus(ctx, projectId);
      break;
    default:
      break;
  }
}

async function renderEnvVaultMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const envSetId = await ensureProjectEnvSet(projectId);
  const keys = await listEnvVarKeys(projectId, envSetId);
  const warnings = [];
  if (!process.env.PATH_APPLIER_CONFIG_DSN) {
    warnings.push('⚠️ Config DB is not configured; Env Vault is in-memory only.');
  }
  const lines = [
    `🔐 Env Vault — ${project.name || project.id}`,
    `Keys stored: ${keys.length}`,
    '',
  ];
  if (warnings.length) {
    lines.push(...warnings, '');
  }
  lines.push('Choose an action:');

  const inline = new InlineKeyboard()
    .text('➕ Add/Update ENV var', `envvault:add:${projectId}`)
    .row()
    .text('🧾 List keys', `envvault:list:${projectId}`)
    .row()
    .text('🗑 Delete key', `envvault:delete_menu:${projectId}`)
    .row()
    .text('🧩 Recommended keys', `envvault:recommend:${projectId}`)
    .row()
    .text('🧩 Import from text', `envvault:import:${projectId}`)
    .row()
    .text('📤 Export keys', `envvault:export:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderEnvVaultKeyList(ctx, projectId) {
  const envSetId = await ensureProjectEnvSet(projectId);
  const keys = await listEnvVarKeys(projectId, envSetId);
  const lines = [`Keys for ${projectId}:`];
  if (!keys.length) {
    lines.push('No keys stored yet.');
  } else {
    keys.forEach((key) => lines.push(`- ${key}`));
  }

  const inline = new InlineKeyboard();
  keys.forEach((key) => {
    inline.text(key, `envvault:key:${projectId}:${key}`).row();
  });
  inline.text('⬅️ Back', `envvault:menu:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

function findEnvKeyInfo(project, key) {
  const template = getProjectTypeTemplate(resolveProjectType(project));
  const entries = [...(template?.required || []), ...(template?.optional || [])];
  return entries.find((entry) => entry.key === key) || null;
}

async function renderEnvVaultKeyDetails(ctx, projectId, key) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const envSetId = await ensureProjectEnvSet(projectId);
  const record = await getEnvVarRecord(projectId, key, envSetId);
  const info = findEnvKeyInfo(project, key);
  const lines = [
    `Key: ${key}`,
    `Value: ${record ? maskSecretValue(record.valueEnc) : '(not set)'}`,
  ];
  if (info?.notes) {
    lines.push(`Notes: ${info.notes}`);
  }

  const inline = new InlineKeyboard()
    .text('🔁 Update value', `envvault:set_key:${projectId}:${key}`)
    .row();

  if (record) {
    inline.text('👁 Reveal once', `envvault:reveal:${projectId}:${key}`).row();
    inline.text('🧹 Clear key', `envvault:clear:${projectId}:${key}`).row();
  }

  inline.text('⬅️ Back', `envvault:list:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderEnvVaultDeleteMenu(ctx, projectId) {
  const envSetId = await ensureProjectEnvSet(projectId);
  const keys = await listEnvVarKeys(projectId, envSetId);
  const lines = ['Select a key to delete:'];
  if (!keys.length) {
    lines.push('No keys stored.');
  }

  const inline = new InlineKeyboard();
  keys.forEach((key) => {
    inline.text(key, `envvault:delete:${projectId}:${key}`).row();
  });
  inline.text('⬅️ Back', `envvault:menu:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function handleEnvVaultDelete(ctx, projectId, key) {
  const envSetId = await ensureProjectEnvSet(projectId);
  const deleted = await deleteEnvVar(projectId, key, envSetId);
  const message = deleted ? `Key ${key} deleted.` : `Key ${key} not found.`;
  await renderOrEdit(ctx, message, {
    reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`),
  });
}

async function revealEnvVaultValue(ctx, projectId, key) {
  const envSetId = await ensureProjectEnvSet(projectId);
  try {
    const value = await getEnvVarValue(projectId, key, envSetId);
    if (!value) {
      await ctx.reply('Key not found.');
      return;
    }
    await ctx.reply(`🔐 ${key}:\n${value}`);
  } catch (error) {
    await ctx.reply(`Failed to decrypt key: ${error.message}`);
  }
}

async function renderEnvVaultExport(ctx, projectId) {
  const envSetId = await ensureProjectEnvSet(projectId);
  const keys = await listEnvVarKeys(projectId, envSetId);
  if (!keys.length) {
    await renderOrEdit(ctx, 'No keys stored.', {
      reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`),
    });
    return;
  }
  const output = keys.join('\n');
  await renderOrEdit(ctx, `Exported keys (no values):\n${output}`, {
    reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`),
  });
}

async function renderEnvVaultQuickKeyMenu(ctx, projectId) {
  const inline = new InlineKeyboard();
  QUICK_KEYS.forEach((key) => {
    inline.text(key, `envvault:set_key:${projectId}:${key}`).row();
  });
  inline.text('✍️ Custom key', `envvault:set_custom:${projectId}`).row();
  inline.text('⬅️ Back', `envvault:menu:${projectId}`);
  await renderOrEdit(ctx, 'Choose an ENV key to add/update:', { reply_markup: inline });
}

async function promptEnvVaultValue(ctx, projectId, key, options = {}) {
  const envSetId = await ensureProjectEnvSet(projectId);
  const messageContext = options.messageContext || getMessageTargetFromCtx(ctx);
  const state = {
    type: 'env_vault_value',
    projectId,
    envSetId,
    queue: Array.isArray(options.queue) ? [...options.queue] : [key],
    currentKey: key,
    allowSkip: options.allowSkip === true,
    skipExisting: options.skipExisting === true,
    requiredKeys: options.requiredKeys || [],
    added: [],
    skipped: [],
    existing: [],
    backCallback: `envvault:menu:${projectId}`,
    messageContext,
  };
  await promptNextEnvVaultKey(ctx, state);
}

async function promptNextEnvVaultKey(ctx, state) {
  while (state.queue.length) {
    const nextKey = state.queue[0];
    const existing = await getEnvVarRecord(state.projectId, nextKey, state.envSetId);
    if (existing && state.skipExisting) {
      state.existing.push(nextKey);
      state.queue.shift();
      continue;
    }
    state.currentKey = nextKey;
    setUserState(ctx.from.id, state);
    const inline = new InlineKeyboard().text('❌ Cancel', 'cancel_input');
    if (state.allowSkip) {
      inline.text('⏭ Skip this key', 'envvault:skip:' + state.projectId);
    }
    await renderStateMessage(ctx, state, `Send value for ${nextKey} (masked). Or Cancel.`, {
      reply_markup: inline,
    });
    return;
  }
  await finishEnvVaultSequence(ctx, state);
}

async function finishEnvVaultSequence(ctx, state) {
  const keys = await listEnvVarKeys(state.projectId, state.envSetId);
  const missingRequired = (state.requiredKeys || []).filter((key) => !keys.includes(key));
  const lines = [
    '✅ Updated',
    'Env Vault update complete.',
    `Added: ${state.added.length}`,
    `Skipped: ${state.skipped.length}`,
  ];
  if (missingRequired.length) {
    lines.push(`Missing required: ${missingRequired.join(', ')}`);
  }
  clearUserState(ctx.from.id);
  await renderStateMessage(ctx, state, lines.join('\n'), {
    reply_markup: buildBackKeyboard(`envvault:menu:${state.projectId}`),
  });
}

async function handleEnvVaultCustomKeyInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send the ENV key name.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(state.backCallback || 'main:back'),
    });
    return;
  }
  const key = normalizeEnvKeyInput(text);
  await promptEnvVaultValue(ctx, state.projectId, key, { messageContext: state.messageContext });
}

async function handleEnvVaultValueInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a value.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(state.backCallback || 'main:back'),
    });
    return;
  }
  if (state.allowSkip && text.toLowerCase() === 'skip') {
    state.skipped.push(state.currentKey);
    state.queue.shift();
    await promptNextEnvVaultKey(ctx, state);
    return;
  }

  const validation = validateEnvValue(state.currentKey, text);
  if (!validation.valid) {
    await ctx.reply(`❌ ${validation.message}`);
    return;
  }
  try {
    await upsertEnvVar(state.projectId, state.currentKey, text, state.envSetId);
    state.added.push(state.currentKey);
    state.queue.shift();
    await promptNextEnvVaultKey(ctx, state);
  } catch (error) {
    await ctx.reply(`Failed to save key: ${error.message}`);
  }
}

async function skipEnvVaultKey(ctx, projectId) {
  const state = getUserState(ctx.from.id);
  if (!state || state.type !== 'env_vault_value' || state.projectId !== projectId) {
    await ctx.reply('Nothing to skip.');
    return;
  }
  state.skipped.push(state.currentKey);
  state.queue.shift();
  await promptNextEnvVaultKey(ctx, state);
}

async function handleEnvVaultImportInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send KEY=VALUE lines.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(state.backCallback || 'main:back'),
    });
    return;
  }
  const envSetId = await ensureProjectEnvSet(state.projectId);
  const { entries, skipped } = parseEnvVaultImportText(text);
  let added = 0;
  const skippedReasons = {};

  for (const entry of skipped) {
    skippedReasons[entry.reason] = (skippedReasons[entry.reason] || 0) + 1;
  }

  for (const entry of entries) {
    const validation = validateEnvValue(entry.key, entry.value);
    if (!validation.valid) {
      skippedReasons.invalid_value = (skippedReasons.invalid_value || 0) + 1;
      continue;
    }
    try {
      await upsertEnvVar(state.projectId, entry.key, entry.value, envSetId);
      added += 1;
    } catch (error) {
      skippedReasons.save_failed = (skippedReasons.save_failed || 0) + 1;
    }
  }
  const skippedTotal = Object.values(skippedReasons).reduce((sum, count) => sum + count, 0);
  const skippedDetails = Object.entries(skippedReasons)
    .map(([reason, count]) => `${reason}: ${count}`)
    .join(', ');
  clearUserState(ctx.from.id);
  const summaryLines = [
    'Import complete.',
    `Stored: ${added}`,
    `Skipped: ${skippedTotal}`,
    skippedDetails ? `Skipped details: ${skippedDetails}` : null,
  ].filter(Boolean);
  await renderStateMessage(ctx, state, summaryLines.join('\n'), {
    reply_markup: buildBackKeyboard(`envvault:menu:${state.projectId}`),
  });
}

async function renderEnvVaultProjectTypeMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const options = getProjectTypeOptions();
  const current = resolveProjectType(project);
  const inline = new InlineKeyboard();
  options.forEach((option) => {
    const label = option.id === current ? `✅ ${option.label}` : option.label;
    inline.text(label, `envvault:recommend_type:${projectId}:${option.id}`).row();
  });
  inline.text('⬅️ Back', `envvault:menu:${projectId}`);
  await renderOrEdit(ctx, 'Choose a project type for recommended keys:', { reply_markup: inline });
}

async function renderEnvVaultRecommendedOptions(ctx, projectId, typeId) {
  const template = getProjectTypeTemplate(typeId);
  if (!template || template.id === 'other') {
    await renderOrEdit(ctx, 'No template for this project type. Set project type to enable recommended keys.', {
      reply_markup: buildBackKeyboard(`envvault:recommend:${projectId}`),
    });
    return;
  }
  const requiredCount = template?.required?.length || 0;
  const optionalCount = template?.optional?.length || 0;
  const inline = new InlineKeyboard()
    .text(`✅ Add all required (${requiredCount})`, `envvault:recommend_required:${projectId}:${typeId}`)
    .row()
    .text(`➕ Add optional pack (${optionalCount})`, `envvault:recommend_optional:${projectId}:${typeId}`)
    .row()
    .text('📋 Pick individually', `envvault:recommend_pick:${projectId}:${typeId}`)
    .row()
    .text('⬅️ Back', `envvault:recommend:${projectId}`);
  await renderOrEdit(ctx, `Recommended keys for ${template?.label || typeId}:`, { reply_markup: inline });
}

async function startEnvVaultRecommendedSequence(ctx, projectId, typeId, mode) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const resolvedType = typeId || resolveProjectType(project);
  const template = getProjectTypeTemplate(resolvedType);
  const keys = mode === 'optional' ? template.optional || [] : template.required || [];
  if (!keys.length) {
    await renderOrEdit(ctx, 'No template for this project type. Set project type to enable recommended keys.', {
      reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`),
    });
    return;
  }
  const keyNames = keys.map((entry) => entry.key);
  await promptEnvVaultValue(ctx, projectId, keyNames[0], {
    queue: keyNames,
    allowSkip: true,
    skipExisting: true,
    requiredKeys: (template.required || []).map((entry) => entry.key),
    messageContext: getMessageTargetFromCtx(ctx),
  });
}

async function startEnvVaultPickMenu(ctx, projectId, typeId) {
  const template = getProjectTypeTemplate(typeId);
  const keys = [...(template.required || []), ...(template.optional || [])].map((entry) => entry.key);
  const state = {
    type: 'env_vault_pick',
    projectId,
    typeId,
    selectedKeys: [],
    availableKeys: keys,
  };
  setUserState(ctx.from.id, state);
  await renderEnvVaultPickMenu(ctx, state);
}

async function renderEnvVaultPickMenu(ctx, state) {
  const inline = new InlineKeyboard();
  state.availableKeys.forEach((key) => {
    const selected = state.selectedKeys.includes(key);
    inline.text(`${selected ? '✅' : '➕'} ${key}`, `envvault:recommend_toggle:${state.projectId}:${key}:${state.typeId}`).row();
  });
  inline.text('✅ Done', `envvault:recommend_confirm:${state.projectId}`).row();
  inline.text('⬅️ Back', `envvault:recommend:${state.projectId}`);
  await renderOrEdit(ctx, 'Pick keys to add:', { reply_markup: inline });
}

async function toggleEnvVaultPickKey(ctx, projectId, key, typeId) {
  const state = getUserState(ctx.from.id);
  if (!state || state.type !== 'env_vault_pick') {
    await renderOrEdit(ctx, 'Pick session expired.', { reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`) });
    return;
  }
  const selected = new Set(state.selectedKeys);
  if (selected.has(key)) {
    selected.delete(key);
  } else {
    selected.add(key);
  }
  state.selectedKeys = Array.from(selected);
  state.typeId = typeId || state.typeId;
  setUserState(ctx.from.id, state);
  await renderEnvVaultPickMenu(ctx, state);
}

async function confirmEnvVaultPickKeys(ctx, projectId) {
  const state = getUserState(ctx.from.id);
  if (!state || state.type !== 'env_vault_pick') {
    await renderOrEdit(ctx, 'Pick session expired.', { reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`) });
    return;
  }
  if (!state.selectedKeys.length) {
    await ctx.reply('No keys selected.');
    return;
  }
  const template = getProjectTypeTemplate(state.typeId || 'other');
  await promptEnvVaultValue(ctx, projectId, state.selectedKeys[0], {
    queue: state.selectedKeys,
    allowSkip: true,
    skipExisting: true,
    requiredKeys: (template.required || []).map((entry) => entry.key),
  });
}

function buildProjectWebhookPath(projectId) {
  return `${TELEGRAM_WEBHOOK_PATH_PREFIX}/${projectId}`;
}

function buildProjectWebhookUrl(project, webhookPath) {
  if (!project?.renderServiceUrl) return null;
  return new URL(webhookPath, project.renderServiceUrl).toString();
}

function buildTelegramSetupView(project, record, notice) {
  const tokenStatus = record?.botTokenEnc ? 'set' : 'not set';
  const lines = [
    `🤖 Telegram Setup — ${project.name || project.id}`,
    notice || null,
    `Token: ${tokenStatus}`,
    `Webhook: ${record?.webhookUrl || '-'}`,
    `Last set: ${record?.lastSetAt || '-'}`,
    `Last test: ${record?.lastTestAt || '-'} ${record?.lastTestStatus ? `(${record.lastTestStatus})` : ''}`,
    `Enabled: ${record?.enabled ? 'yes' : 'no'}`,
  ].filter(Boolean);

  const inline = new InlineKeyboard()
    .text(record?.botTokenEnc ? '🔑 Update bot token' : '🔑 Set bot token', `tgbot:set_token:${project.id}`)
    .row()
    .text('🔗 Set webhook', `tgbot:set_webhook:${project.id}`)
    .row()
    .text('🧪 Run test', `tgbot:test:${project.id}`)
    .row()
    .text('🧾 Status', `tgbot:status:${project.id}`)
    .row();

  if (record?.botTokenEnc) {
    inline.text('🧹 Clear token', `tgbot:clear_token:${project.id}`).row();
  }
  inline.text('⬅️ Back', `proj:open:${project.id}`);

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderTelegramSetupMenu(ctx, projectId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const record = await getProjectTelegramBot(projectId);
  const view = buildTelegramSetupView(project, record, notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function renderTelegramSetupMenuForMessage(messageContext, projectId, notice) {
  if (!messageContext) return;
  const project = await getProjectById(projectId);
  if (!project) return;
  const record = await getProjectTelegramBot(projectId);
  const view = buildTelegramSetupView(project, record, notice);
  try {
    await bot.api.editMessageText(
      messageContext.chatId,
      messageContext.messageId,
      view.text,
      normalizeTelegramExtra({ reply_markup: view.keyboard }),
    );
  } catch (error) {
    console.error('[UI] Failed to update telegram setup message', error);
  }
}

async function handleTelegramTokenInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send the token.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(state.backCallback || 'main:back'),
    });
    return;
  }
  await upsertTelegramBotToken(state.projectId, text);
  clearUserState(ctx.from.id);
  await renderTelegramSetupMenuForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderTelegramSetupMenu(ctx, state.projectId, '✅ Updated');
  }
}

async function setProjectTelegramWebhook(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  if (!project.renderServiceUrl) {
    await renderOrEdit(ctx, 'Project server URL missing. Set it first (Edit URLs).', {
      reply_markup: buildBackKeyboard(`proj:render_menu:${projectId}`),
    });
    return;
  }
  const token = await getTelegramBotToken(projectId);
  if (!token) {
    await renderOrEdit(ctx, 'Bot token missing. Set the token first.', {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
    return;
  }
  const webhookPath = buildProjectWebhookPath(projectId);
  const webhookUrl = buildProjectWebhookUrl(project, webhookPath);
  if (!webhookUrl) {
    await renderOrEdit(ctx, 'Unable to derive webhook URL.', {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
    return;
  }

  try {
    await setWebhook(token, webhookUrl, false);
    const info = await getWebhookInfo(token);
    await updateTelegramWebhook(projectId, {
      webhookUrl,
      webhookPath,
      lastSetAt: new Date().toISOString(),
      enabled: true,
    });
    const lines = [
      'Webhook configured.',
      `URL: ${info?.url || webhookUrl}`,
      `Pending updates: ${info?.pending_update_count ?? '-'}`,
      info?.last_error_message ? `Last error: ${info.last_error_message}` : null,
    ].filter(Boolean);
    await renderOrEdit(ctx, lines.join('\n'), {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
  } catch (error) {
    await updateTelegramWebhook(projectId, {
      webhookUrl,
      webhookPath,
      lastSetAt: new Date().toISOString(),
      enabled: false,
    });
    await renderOrEdit(ctx, `Failed to set webhook: ${error.message}`, {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
  }
}

async function runTelegramWebhookTest(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const token = await getTelegramBotToken(projectId);
  if (!token) {
    await renderOrEdit(ctx, 'Bot token missing. Set the token first.', {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
    return;
  }
  try {
    await sendMessage(token, ctx.from.id, `✅ Webhook test OK for ${project.name || project.id}`);
    await updateTelegramTestStatus(projectId, 'ok');
    await renderOrEdit(ctx, 'Test message sent.', {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
  } catch (error) {
    await updateTelegramTestStatus(projectId, error.message);
    await renderOrEdit(ctx, `Test failed: ${error.message}`, {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
  }
}

async function renderTelegramStatus(ctx, projectId) {
  const token = await getTelegramBotToken(projectId);
  if (!token) {
    await renderOrEdit(ctx, 'Bot token missing. Set the token first.', {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
    return;
  }
  try {
    const info = await getWebhookInfo(token);
    const lines = [
      `Webhook URL: ${info?.url || '-'}`,
      `Pending updates: ${info?.pending_update_count ?? '-'}`,
      info?.last_error_message ? `Last error: ${info.last_error_message}` : null,
    ].filter(Boolean);
    await renderOrEdit(ctx, lines.join('\n'), {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
  } catch (error) {
    await renderOrEdit(ctx, `Failed to load webhook info: ${error.message}`, {
      reply_markup: buildBackKeyboard(`tgbot:menu:${projectId}`),
    });
  }
}

function buildEnvVaultUnavailableMessage(prefix) {
  const headline = prefix || MASTER_KEY_ERROR_MESSAGE;
  const lines = [
    headline,
    prefix ? MASTER_KEY_ERROR_MESSAGE : null,
    '',
    'Generate a key:',
    'Linux/macOS: openssl rand -hex 32',
    'Windows PowerShell: [Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))',
  ].filter(Boolean);
  return lines.join('\n');
}

function isEnvVaultAvailable() {
  return getMasterKeyStatus().ok;
}

async function ensureSupabaseEnvSet() {
  if (!isEnvVaultAvailable()) {
    return null;
  }
  return ensureDefaultEnvVarSet(SUPABASE_ENV_VAULT_PROJECT_ID);
}

async function resolveSupabaseConnectionDsn(connection) {
  if (!connection?.envKey) {
    return { dsn: null, source: null, error: 'missing envKey' };
  }
  if (!isEnvVaultAvailable()) {
    return { dsn: null, source: connection.envKey, error: MASTER_KEY_ERROR_MESSAGE };
  }
  const envSetId = await ensureSupabaseEnvSet();
  if (!envSetId) {
    return { dsn: null, source: connection.envKey, error: 'Env Vault not initialized' };
  }
  try {
    const dsn = await getEnvVarValue(SUPABASE_ENV_VAULT_PROJECT_ID, connection.envKey, envSetId);
    if (!dsn) {
      return { dsn: null, source: connection.envKey, error: `env ${connection.envKey} missing` };
    }
    return { dsn, source: connection.envKey, error: null };
  } catch (error) {
    console.error('[supabase] Failed to read DSN from Env Vault', error);
    await forwardSelfLog('error', 'Failed to read Supabase DSN from Env Vault', {
      context: {
        envKey: connection.envKey,
        error: error?.message,
      },
      stack: error?.stack,
    });
    return { dsn: null, source: connection.envKey, error: 'Env Vault read failed' };
  }
}

async function getSupabasePool(connectionId, dsn) {
  let pool = supabasePools.get(connectionId);
  if (!pool) {
    pool = new Pool({ connectionString: dsn });
    supabasePools.set(connectionId, pool);
  }
  return pool;
}

function normalizeSupabaseQuery(sql, options = {}) {
  if (typeof sql === 'string') {
    return { text: sql, ...options };
  }
  return sql;
}

function isSensitiveColumnName(name) {
  if (!name) return false;
  const pattern = /(password|pass|token|secret|api_key|service_role|bearer|key)/i;
  return pattern.test(name);
}

function sanitizeCellValue(value) {
  if (value == null) return 'null';
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (error) {
      return '[unserializable]';
    }
  }
  return String(value);
}

function formatCellValue(value, limit = SUPABASE_CELL_TRUNCATE_LIMIT) {
  const raw = sanitizeCellValue(value);
  const flattened = raw.replace(/[\r\n\t]+/g, ' ');
  return truncateText(flattened, limit);
}

function applyRowMasking(row) {
  const masked = {};
  Object.entries(row).forEach(([key, value]) => {
    masked[key] = isSensitiveColumnName(key) ? '***masked***' : value;
  });
  return masked;
}

function quoteIdentifier(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function formatRowsAsCodeBlock(rows, columnNames, truncatedNote) {
  const header = columnNames.join(' | ');
  const lines = [header];
  rows.forEach((row) => {
    const line = columnNames
      .map((col) => formatCellValue(row[col]))
      .join(' | ');
    lines.push(line);
  });
  if (truncatedNote) {
    lines.push(truncatedNote);
  }
  return `<pre>${escapeHtml(lines.join('\n'))}</pre>`;
}

function decodeSupabaseTableName(encoded) {
  try {
    return decodeURIComponent(encoded);
  } catch (error) {
    return encoded;
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
  const statuses = await Promise.all(
    connections.map(async (connection) => {
      const dsnInfo = await resolveSupabaseConnectionDsn(connection);
      if (!dsnInfo.dsn) {
        return {
          connection,
          dsnStatus: `❌ ${dsnInfo.error}`,
          connectStatus: 'n/a',
        };
      }
      try {
        const pool = await getSupabasePool(connection.id, dsnInfo.dsn);
        await pool.query({ text: 'SELECT 1', query_timeout: SUPABASE_QUERY_TIMEOUT_MS });
        return { connection, dsnStatus: '✅ present', connectStatus: '✅ ok' };
      } catch (error) {
        const reason = truncateText(error.message || 'failed', 60);
        return { connection, dsnStatus: '✅ present', connectStatus: `❌ ${reason}` };
      }
    }),
  );
  const lines = ['Supabase connections:'];
  const inline = new InlineKeyboard();
  statuses.forEach((status) => {
    lines.push(
      `• ${status.connection.name} (${status.connection.id}) — env: ${status.connection.envKey} — DSN: ${status.dsnStatus} — Connect: ${status.connectStatus}`,
    );
    inline
      .text(`🗄️ ${status.connection.name}`, `supabase:conn:${status.connection.id}`)
      .row();
  });
  inline.text('⬅️ Back', 'supabase:back');
  await renderOrEdit(ctx, truncateMessage(lines.join('\n'), SUPABASE_MESSAGE_LIMIT), {
    reply_markup: inline,
  });
}

async function renderSupabaseConnectionMenu(ctx, connectionId) {
  const connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    await ctx.reply('Supabase connection not found.');
    return;
  }
  const dsnInfo = await resolveSupabaseConnectionDsn(connection);
  const lines = [
    `DB Explorer: ${connection.name}`,
    `Env key: ${connection.envKey}`,
    `DSN: ${dsnInfo.dsn ? '✅ present' : `❌ ${dsnInfo.error}`}`,
  ];
  const inline = new InlineKeyboard()
    .text('📋 Tables', `supabase:tables:${connectionId}`)
    .row()
    .text('🔎 Query', `supabase:sql:${connectionId}`)
    .row()
    .text('⬅️ Back', 'supabase:connections');

  await renderOrEdit(ctx, truncateMessage(lines.join('\n'), SUPABASE_MESSAGE_LIMIT), {
    reply_markup: inline,
  });
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
      if (amount > 60) {
        if (amount % 60 !== 0) {
          throw new Error('Minutes over 60 must be divisible by 60.');
        }
        const hours = amount / 60;
        return {
          cron: `0 */${hours} * * *`,
          label: `Every ${hours} hour${hours === 1 ? '' : 's'}`,
        };
      }
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
  const validation = validateCronExpression(raw);
  if (!validation.valid) {
    throw new Error(validation.message);
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

function clearCronJobsCache() {
  lastCronJobsCache = null;
  lastCronJobsFetchedAt = 0;
}

async function fetchCronJobs() {
  if (lastCronJobsCache && Date.now() - lastCronJobsFetchedAt < CRON_JOBS_CACHE_TTL_MS) {
    return lastCronJobsCache;
  }
  const { jobs, someFailed } = await listJobs();
  const normalized = jobs.map(normalizeCronJob).filter((job) => job && job.id != null);
  lastCronJobsCache = { jobs: normalized, someFailed };
  lastCronJobsFetchedAt = Date.now();
  return lastCronJobsCache;
}

async function fetchCronJob(jobId) {
  const job = await getJob(jobId);
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

const CRON_WEEKDAYS = [
  { label: 'Mo', value: 1 },
  { label: 'Tu', value: 2 },
  { label: 'We', value: 3 },
  { label: 'Th', value: 4 },
  { label: 'Fr', value: 5 },
  { label: 'Sa', value: 6 },
  { label: 'Su', value: 0 },
];

function normalizeScheduleArray(value, fallback = [-1]) {
  const normalized = normalizeCronField(value);
  if (normalized == null) return [...fallback];
  if (normalized === -1) return [-1];
  if (Array.isArray(normalized)) return normalized;
  return [Number(normalized)].filter(Number.isFinite);
}

function buildDefaultCronSchedule(timezone) {
  const minutes = [];
  for (let i = 0; i < 60; i += 5) {
    minutes.push(i);
  }
  return {
    timezone: timezone || 'UTC',
    minutes,
    hours: [-1],
    mdays: [-1],
    months: [-1],
    wdays: [-1],
    expiresAt: null,
  };
}

function buildCronScheduleFromJob(job, timezone) {
  const schedule = job?.schedule || {};
  return {
    timezone: schedule.timezone || job?.timezone || timezone || 'UTC',
    minutes: normalizeScheduleArray(schedule.minutes),
    hours: normalizeScheduleArray(schedule.hours),
    mdays: normalizeScheduleArray(schedule.mdays),
    months: normalizeScheduleArray(schedule.months),
    wdays: normalizeScheduleArray(schedule.wdays),
    expiresAt: schedule.expiresAt ?? null,
  };
}

function scheduleFieldValue(values) {
  if (!Array.isArray(values) || values.length === 0) return -1;
  return values.includes(-1) ? -1 : values;
}

function summarizeSchedule(schedule) {
  const summary = describeCronSchedule(
    {
      minutes: scheduleFieldValue(schedule.minutes),
      hours: scheduleFieldValue(schedule.hours),
      expression: schedule?.cron,
    },
    { includeAllHours: true },
  );
  const lines = [
    `Timezone: ${schedule.timezone || 'UTC'}`,
    `Minutes: ${formatScheduleValue(schedule.minutes, 'every minute')}`,
    `Hours: ${formatScheduleValue(schedule.hours, 'every hour')}`,
    `Days of month: ${formatScheduleValue(schedule.mdays, 'every day')}`,
    `Months: ${formatScheduleValue(schedule.months, 'every month')}`,
    `Weekdays: ${formatScheduleValue(schedule.wdays, 'every day')}`,
    `Summary: ${summary}`,
  ];
  return lines.join('\n');
}

function buildEveryNMinutesSchedule(interval, timezone) {
  const minutes = [];
  for (let i = 0; i < 60; i += interval) {
    minutes.push(i);
  }
  return {
    timezone: timezone || 'UTC',
    minutes,
    hours: [-1],
    mdays: [-1],
    months: [-1],
    wdays: [-1],
    expiresAt: null,
  };
}

function buildEveryNHoursSchedule(interval, timezone) {
  const hours = [];
  if (interval === 1) {
    hours.push(-1);
  } else {
    for (let i = 0; i < 24; i += interval) {
      hours.push(i);
    }
  }
  return {
    timezone: timezone || 'UTC',
    minutes: [0],
    hours,
    mdays: [-1],
    months: [-1],
    wdays: [-1],
    expiresAt: null,
  };
}

function buildDailySchedule(time, timezone) {
  return {
    timezone: timezone || 'UTC',
    minutes: [time.minutes],
    hours: [time.hours],
    mdays: [-1],
    months: [-1],
    wdays: [-1],
    expiresAt: null,
  };
}

function buildWeeklySchedule(days, time, timezone) {
  return {
    timezone: timezone || 'UTC',
    minutes: [time.minutes],
    hours: [time.hours],
    mdays: [-1],
    months: [-1],
    wdays: days.length ? days : [-1],
    expiresAt: null,
  };
}

function parseTimeInput(raw) {
  const text = raw?.trim();
  if (!text) return null;
  const match = text.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) return null;
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return null;
  return { hours, minutes };
}

function parseNumberList(raw, { min, max }) {
  const text = raw?.trim();
  if (!text) return null;
  const items = text
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((value) => Number.isFinite(value));
  if (!items.length) return null;
  const unique = Array.from(new Set(items));
  const valid = unique.every((value) => value >= min && value <= max);
  if (!valid) return null;
  return unique.sort((a, b) => a - b);
}

function uniqueSorted(values) {
  return Array.from(new Set(values)).sort((a, b) => a - b);
}

function getWeekdayLabel(value) {
  const match = CRON_WEEKDAYS.find((day) => day.value === value);
  return match ? match.label : String(value);
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
  let label = title === '(unnamed)' ? '⏰ Job' : `⏰ ${title}`;
  if (job?.enabled === false) {
    label += ' (off)';
  }
  if (label.length > maxLength) {
    label = `${label.slice(0, maxLength - 1)}…`;
  }
  return label;
}

function buildCronWizardCancelKeyboard() {
  return new InlineKeyboard().text('❌ Cancel', 'cronwiz:cancel');
}

function buildCronWizardBackCancelKeyboard(backAction, backLabel = '⬅️ Back') {
  return new InlineKeyboard()
    .text(backLabel, backAction)
    .text('❌ Cancel', 'cronwiz:cancel');
}

async function startCronCreateWizard(ctx, url, backCallback) {
  const cronSettings = await getEffectiveCronSettings();
  const schedule = buildDefaultCronSchedule(cronSettings.defaultTimezone);
  const state = {
    type: 'cron_wizard',
    mode: 'create',
    step: url ? 'schedule' : 'url',
    backCallback,
    draft: {
      url: url || '',
      schedule,
      timezone: cronSettings.defaultTimezone,
      name: '',
    },
    lastError: null,
    temp: {
      url,
      schedule,
      pattern: null,
      previousStep: null,
      inputType: url ? null : 'url',
      urlNextStep: url ? null : 'choose-pattern',
    },
  };
  setUserState(ctx.from.id, state);
  if (!url) {
    await renderCronWizardMessage(ctx, state, 'Send target URL (e.g. keep-alive or deploy hook).', {
      reply_markup: buildCronWizardCancelKeyboard(),
    });
    return;
  }
  await renderCronWizardPatternMenu(ctx, state);
}

async function startCronEditScheduleWizard(ctx, jobId, backCallback) {
  let job;
  try {
    job = await fetchCronJob(jobId);
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'get',
      error,
      userId: ctx.from?.id,
      projectId: null,
      correlationId,
    });
    if (
      await renderCronRateLimitIfNeeded(ctx, error, {
        reply_markup: buildBackKeyboard(backCallback || 'cron:list'),
      }, correlationId)
    ) {
      return;
    }
    await renderOrEdit(
      ctx,
      formatCronApiErrorNotice('Failed to load cron job', error, correlationId),
      { reply_markup: buildBackKeyboard(backCallback || 'cron:list') },
    );
    return;
  }
  if (!job) {
    await renderOrEdit(ctx, 'Cron job not found.', {
      reply_markup: buildBackKeyboard(backCallback || 'cron:list'),
    });
    return;
  }

  const cronSettings = await getEffectiveCronSettings();
  const schedule = buildCronScheduleFromJob(job, cronSettings.defaultTimezone);
  const state = {
    type: 'cron_wizard',
    mode: 'edit',
    step: 'schedule',
    backCallback: backCallback || `cron:job:${jobId}`,
    draft: {
      url: job.url || '',
      schedule,
      timezone: cronSettings.defaultTimezone,
      name: getCronJobDisplayName(job),
    },
    lastError: null,
    temp: {
      schedule,
      jobId,
      pattern: null,
      previousStep: null,
    },
  };
  setUserState(ctx.from.id, state);
  await renderCronWizardEditEntry(ctx, state);
}

async function renderCronWizardEditEntry(ctx, state) {
  const summary = summarizeSchedule(state.temp.schedule);
  const inline = new InlineKeyboard()
    .text('♻️ Change pattern', 'cronwiz:entry:pattern')
    .row()
    .text('⚙️ Advanced fields', 'cronwiz:entry:advanced')
    .row()
    .text('❌ Cancel', 'cronwiz:cancel');
  await renderCronWizardMessage(
    ctx,
    state,
    `Current schedule:\n${summary}\n\nChoose how you want to edit:`,
    { reply_markup: inline },
  );
}

async function renderCronWizardPatternMenu(ctx, state) {
  const inline = new InlineKeyboard()
    .text('⏱ Every N minutes', 'cronwiz:pattern:minutes')
    .text('🕒 Every N hours', 'cronwiz:pattern:hours')
    .row()
    .text('🌅 Daily at time', 'cronwiz:pattern:daily')
    .text('📅 Weekly at time', 'cronwiz:pattern:weekly')
    .row()
    .text('⚙️ Advanced fields', 'cronwiz:pattern:advanced')
    .row()
    .text('❌ Cancel', 'cronwiz:cancel');
  await renderCronWizardMessage(ctx, state, 'Choose how often this job should run:', {
    reply_markup: inline,
  });
}

async function renderCronWizardMinutesMenu(ctx) {
  const inline = new InlineKeyboard()
    .text('5m', 'cronwiz:minutes:5')
    .text('10m', 'cronwiz:minutes:10')
    .text('15m', 'cronwiz:minutes:15')
    .text('30m', 'cronwiz:minutes:30')
    .text('60m', 'cronwiz:minutes:60')
    .row()
    .text('Custom…', 'cronwiz:minutes:custom')
    .text('⬅️ Back', 'cronwiz:back:pattern')
    .row()
    .text('❌ Cancel', 'cronwiz:cancel');
  await renderCronWizardMessage(ctx, getUserState(ctx.from.id), 'Every how many minutes?', {
    reply_markup: inline,
  });
}

async function renderCronWizardHoursMenu(ctx) {
  const inline = new InlineKeyboard()
    .text('1h', 'cronwiz:hours:1')
    .text('2h', 'cronwiz:hours:2')
    .text('3h', 'cronwiz:hours:3')
    .text('6h', 'cronwiz:hours:6')
    .text('12h', 'cronwiz:hours:12')
    .row()
    .text('24h', 'cronwiz:hours:24')
    .text('Custom…', 'cronwiz:hours:custom')
    .row()
    .text('⬅️ Back', 'cronwiz:back:pattern')
    .text('❌ Cancel', 'cronwiz:cancel');
  await renderCronWizardMessage(ctx, getUserState(ctx.from.id), 'Every how many hours?', {
    reply_markup: inline,
  });
}

async function renderCronWizardDailyMenu(ctx) {
  const inline = new InlineKeyboard()
    .text('06:00', 'cronwiz:daily:06:00')
    .text('08:00', 'cronwiz:daily:08:00')
    .text('09:00', 'cronwiz:daily:09:00')
    .text('10:00', 'cronwiz:daily:10:00')
    .row()
    .text('12:00', 'cronwiz:daily:12:00')
    .text('15:00', 'cronwiz:daily:15:00')
    .text('18:00', 'cronwiz:daily:18:00')
    .text('21:00', 'cronwiz:daily:21:00')
    .row()
    .text('Custom…', 'cronwiz:daily:custom')
    .text('⬅️ Back', 'cronwiz:back:pattern')
    .row()
    .text('❌ Cancel', 'cronwiz:cancel');
  await renderCronWizardMessage(ctx, getUserState(ctx.from.id), 'Choose a daily run time:', {
    reply_markup: inline,
  });
}

function buildWeeklyDayKeyboard(selectedDays) {
  const inline = new InlineKeyboard();
  CRON_WEEKDAYS.forEach((day, index) => {
    const selected = selectedDays.includes(day.value);
    const label = selected ? `✅ ${day.label}` : day.label;
    inline.text(label, `cronwiz:weekly_day:${day.value}`);
    if (index % 3 === 2) {
      inline.row();
    }
  });
  inline.row().text('✅ Done', 'cronwiz:weekly_done').text('⬅️ Back', 'cronwiz:back:pattern');
  inline.row().text('❌ Cancel', 'cronwiz:cancel');
  return inline;
}

async function renderCronWizardWeeklyDaysMenu(ctx, state) {
  const selectedDays = (state.temp.schedule.wdays || []).filter((day) => day !== -1);
  await renderCronWizardMessage(
    ctx,
    state,
    `Select weekdays (${selectedDays.length ? selectedDays.map(getWeekdayLabel).join(', ') : 'none'}):`,
    { reply_markup: buildWeeklyDayKeyboard(selectedDays) },
  );
}

async function renderCronWizardWeeklyTimeMenu(ctx) {
  const inline = new InlineKeyboard()
    .text('06:00', 'cronwiz:weekly_time:06:00')
    .text('08:00', 'cronwiz:weekly_time:08:00')
    .text('09:00', 'cronwiz:weekly_time:09:00')
    .text('10:00', 'cronwiz:weekly_time:10:00')
    .row()
    .text('12:00', 'cronwiz:weekly_time:12:00')
    .text('15:00', 'cronwiz:weekly_time:15:00')
    .text('18:00', 'cronwiz:weekly_time:18:00')
    .text('21:00', 'cronwiz:weekly_time:21:00')
    .row()
    .text('Custom…', 'cronwiz:weekly_time:custom')
    .text('⬅️ Back', 'cronwiz:back:weekly_days')
    .row()
    .text('❌ Cancel', 'cronwiz:cancel');
  await renderCronWizardMessage(ctx, getUserState(ctx.from.id), 'Choose a weekly run time:', {
    reply_markup: inline,
  });
}

async function renderCronWizardAdvancedMenu(ctx, state) {
  const summary = summarizeSchedule(state.temp.schedule);
  const inline = new InlineKeyboard()
    .text('🧮 Edit minutes', 'cronwiz:advanced:minutes')
    .row()
    .text('🕒 Edit hours', 'cronwiz:advanced:hours')
    .row()
    .text('📆 Edit month days', 'cronwiz:advanced:mdays')
    .row()
    .text('📅 Edit months', 'cronwiz:advanced:months')
    .row()
    .text('📊 Edit weekdays', 'cronwiz:advanced:wdays')
    .row()
    .text('🌐 Edit timezone', 'cronwiz:advanced:timezone')
    .row()
    .text('✅ Done', 'cronwiz:advanced:done')
    .row()
    .text('⬅️ Back', 'cronwiz:advanced:back')
    .text('❌ Cancel', 'cronwiz:cancel');
  await renderCronWizardMessage(ctx, state, `Advanced schedule fields:\n${summary}`, {
    reply_markup: inline,
  });
}

function buildAdvancedFieldKeyboard(field) {
  const inline = new InlineKeyboard();
  inline.text('All', `cronwiz:field:${field}:all`).row();

  if (field === 'minutes') {
    inline
      .text('Every 5m', 'cronwiz:field:minutes:preset:every5')
      .text('Every 10m', 'cronwiz:field:minutes:preset:every10')
      .row();
  }
  if (field === 'hours') {
    inline
      .text('Every 2h', 'cronwiz:field:hours:preset:every2')
      .text('Work hours', 'cronwiz:field:hours:preset:work')
      .row();
  }
  if (field === 'mdays') {
    inline
      .text('1st', 'cronwiz:field:mdays:preset:first')
      .text('15th', 'cronwiz:field:mdays:preset:mid')
      .row();
  }
  if (field === 'months') {
    inline
      .text('Quarterly', 'cronwiz:field:months:preset:quarterly')
      .row();
  }
  if (field === 'wdays') {
    inline
      .text('Weekdays', 'cronwiz:field:wdays:preset:weekdays')
      .text('Weekends', 'cronwiz:field:wdays:preset:weekends')
      .row();
  }

  inline
    .text('Custom list…', `cronwiz:field:${field}:custom`)
    .row()
    .text('⬅️ Back', 'cronwiz:advanced:back')
    .text('❌ Cancel', 'cronwiz:cancel');
  return inline;
}

async function renderCronWizardAdvancedFieldMenu(ctx, field) {
  await renderCronWizardMessage(ctx, getUserState(ctx.from.id), `Edit ${field}.`, {
    reply_markup: buildAdvancedFieldKeyboard(field),
  });
}

async function renderCronWizardConfirm(ctx, state) {
  const summary = summarizeSchedule(state.temp.schedule);
  const inline = new InlineKeyboard()
    .text('✅ Use this schedule', 'cronwiz:confirm:use')
    .text('♻️ Adjust', 'cronwiz:confirm:adjust')
    .row()
    .text('❌ Cancel', 'cronwiz:cancel');
  const lines = [`Proposed schedule:\n${summary}`];
  if (state.mode === 'create') {
    if (state.draft?.url) {
      lines.push('', `URL: ${state.draft.url}`);
    }
  }
  await renderCronWizardMessage(ctx, state, lines.join('\n'), { reply_markup: inline });
}

function buildCronWizardDraftSummary(state) {
  const summary = summarizeSchedule(state?.temp?.schedule);
  const lines = [`Schedule: ${summary}`];
  if (state?.draft?.url) {
    lines.push(`URL: ${state.draft.url}`);
  }
  if (state?.draft?.name) {
    lines.push(`Name: ${state.draft.name}`);
  }
  return lines.join('\n');
}

async function renderCronWizardError(ctx, state, { error, hint, operation }) {
  const correlationId = buildCronCorrelationId();
  const isUrlError = isCronUrlValidationError(error);
  logCronApiError({
    operation,
    error,
    userId: ctx.from?.id,
    projectId: state?.projectId,
    correlationId,
  });
  state.lastError = {
    status: error?.status ?? null,
    message: error?.message ?? 'Cron API error',
    isUrlError,
  };
  const message = formatCronApiErrorMessage({ error, hint, correlationId });
  const summary = buildCronWizardDraftSummary(state);
  const text = summary ? `${message}\n\n${summary}` : message;
  await renderCronWizardMessage(ctx, state, text, {
    reply_markup: buildCronWizardErrorKeyboard(),
  });
}

async function attemptCronWizardCreate(ctx, state) {
  const cronSettings = await getEffectiveCronSettings();
  const schedule = state.temp?.schedule;
  const url = state.draft?.url;
  if (!schedule || !url) {
    await renderCronWizardMessage(ctx, state, 'Missing schedule or URL. Please re-enter the URL.', {
      reply_markup: buildCronWizardCancelKeyboard(),
    });
    setCronWizardStep(state, 'url', 'url');
    state.temp.inputType = 'url';
    state.temp.urlNextStep = 'choose-pattern';
    return;
  }
  const jobName =
    state.draft?.name && state.draft.name.trim()
      ? state.draft.name.trim()
      : `path-applier:custom:${Date.now()}`;
  state.draft.name = jobName;
  try {
    const payload = buildCronJobPayload({
      name: jobName,
      url,
      schedule,
      timezone: cronSettings.defaultTimezone,
      enabled: true,
    });
    const created = await createJob(payload);
    clearCronJobsCache();
    clearUserState(ctx.from.id);
    await ctx.reply(`Cron job created (id: #${created.id}, title: ${jobName}).`);
    await renderCronMenu(ctx);
  } catch (error) {
    setCronWizardStep(state, 'confirm', 'confirm');
    const hint = isCronUrlValidationError(error)
      ? 'Please re-enter the URL.'
      : 'Please re-enter the schedule.';
    await renderCronWizardError(ctx, state, {
      error,
      hint,
      operation: 'create',
    });
  }
}

async function renderCronMenu(ctx) {
  const cronSettings = await getEffectiveCronSettings();
  if (!cronSettings.enabled) {
    await renderOrEdit(ctx, 'Cron integration is disabled in settings.', {
      reply_markup: buildBackKeyboard('main:back'),
    });
    return;
  }
  if (!CRON_API_TOKEN) {
    await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_TOKEN missing).', {
      reply_markup: buildBackKeyboard('main:back'),
    });
    return;
  }
  let jobs = [];
  try {
    const response = await fetchCronJobs();
    jobs = response.jobs;
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'list',
      error,
      userId: ctx.from?.id,
      projectId: null,
      correlationId,
    });
    if (
      await renderCronRateLimitIfNeeded(ctx, error, {
        reply_markup: buildBackKeyboard('main:back'),
      }, correlationId)
    ) {
      return;
    }
    await renderOrEdit(
      ctx,
      formatCronApiErrorNotice('Failed to list cron jobs', error, correlationId),
      { reply_markup: buildBackKeyboard('main:back') },
    );
    return;
  }

  const lines = ['⏰ Cron jobs', '', `Total jobs: ${jobs.length}`];
  const inline = new InlineKeyboard()
    .text('📋 List jobs', 'cron:list')
    .row()
    .text('➕ Create job', 'cron:create')
    .row()
    .text('🧷 Link job to project', 'cronlink:menu')
    .row()
    .text('🧹 Show “Other” only', 'cronlink:other')
    .row()
    .text('🔎 Filter by project', 'cronlink:filter_menu')
    .row()
    .text('⬅️ Back', 'main:back');

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderCronJobList(ctx, options = {}) {
  if (!CRON_API_TOKEN) {
    await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_TOKEN missing).', {
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
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'list',
      error,
      userId: ctx.from?.id,
      projectId: null,
      correlationId,
    });
    if (
      await renderCronRateLimitIfNeeded(ctx, error, {
        reply_markup: buildBackKeyboard('cron:menu'),
      }, correlationId)
    ) {
      return;
    }
    await renderOrEdit(
      ctx,
      formatCronApiErrorNotice('Failed to list cron jobs', error, correlationId),
      { reply_markup: buildBackKeyboard('cron:menu') },
    );
    return;
  }
  const projects = await loadProjects();
  const links = await listCronJobLinks();
  const linkMap = new Map(links.map((link) => [String(link.cronJobId), link]));
  await autoLinkCronJobs(jobs, projects, linkMap);

  const filtered = filterCronJobsByProject(jobs, projects, linkMap, options);
  const grouped = groupCronJobs(filtered, projects, linkMap);

  const lines = ['Cron jobs:'];
  if (!grouped.length) {
    lines.push('No cron jobs found.');
  }
  grouped.forEach((group) => {
    group.jobs.forEach((job) => {
      const schedule = describeCronSchedule(job);
      lines.push(
        `[${group.label}] — ${getCronJobDisplayName(job)} — ${job.enabled ? 'Enabled' : 'Disabled'} — ${schedule}`,
      );
    });
  });
  if (someFailed) {
    lines.push('', '⚠️ Some jobs failed to load from cron-job.org.');
  }

  const inline = new InlineKeyboard();
  grouped.forEach((group) => {
    group.jobs.forEach((job) => {
      const label = buildCronJobButtonLabel(job);
      inline.text(label, `cron:job:${job.id}`).row();
    });
  });
  inline.text('⬅️ Back', 'cron:menu');

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

function resolveCronProjectId(job, link, projects) {
  if (link?.projectId) return link.projectId;
  const match = findProjectForCronUrl(projects, job?.url);
  return match?.id || null;
}

function resolveCronProjectLabel(job, link, projects) {
  if (link?.label) return link.label;
  const projectId = resolveCronProjectId(job, link, projects);
  if (!projectId) return 'Other';
  const project = findProjectById(projects, projectId);
  return project?.name || projectId || 'Other';
}

function normalizeUrlForMatch(url) {
  try {
    const parsed = new URL(url);
    return `${parsed.protocol}//${parsed.host}${parsed.pathname}`.replace(/\/$/, '');
  } catch (error) {
    return null;
  }
}

function urlMatchesBase(targetUrl, baseUrl) {
  const target = normalizeUrlForMatch(targetUrl);
  const base = normalizeUrlForMatch(baseUrl);
  if (!target || !base) return false;
  if (!target.startsWith(base)) return false;
  return true;
}

function findProjectForCronUrl(projects, url) {
  if (!url) return null;
  const baseUrl = getPublicBaseUrl().replace(/\/$/, '');
  const keepAliveMatch = projects.find((project) =>
    urlMatchesBase(url, `${baseUrl}/keep-alive/${project.id}`),
  );
  if (keepAliveMatch) return keepAliveMatch;

  return projects.find((project) => {
    const candidates = [
      project.renderServiceUrl,
      project.renderDeployHookUrl,
      project.renderServiceUrl ? `${project.renderServiceUrl.replace(/\/$/, '')}${TELEGRAM_WEBHOOK_PATH_PREFIX}/${project.id}` : null,
    ].filter(Boolean);
    return candidates.some((candidate) => urlMatchesBase(url, candidate));
  }) || null;
}

async function autoLinkCronJobs(jobs, projects, linkMap) {
  for (const job of jobs) {
    const jobId = String(job.id);
    if (linkMap.has(jobId)) continue;
    const match = findProjectForCronUrl(projects, job.url);
    if (!match) continue;
    const link = await upsertCronJobLink(jobId, match.id, null);
    console.info('[cron] auto-linked job', { jobId, projectId: match.id });
    linkMap.set(jobId, link);
  }
}

function filterCronJobsByProject(jobs, projects, linkMap, options) {
  if (!options?.projectId && !options?.otherOnly) return jobs;
  return jobs.filter((job) => {
    const link = linkMap.get(String(job.id));
    const projectId = resolveCronProjectId(job, link, projects);
    if (options.otherOnly) return !projectId;
    return projectId === options.projectId;
  });
}

function groupCronJobs(jobs, projects, linkMap) {
  const grouped = new Map();
  jobs.forEach((job) => {
    const link = linkMap.get(String(job.id));
    const label = resolveCronProjectLabel(job, link, projects);
    if (!grouped.has(label)) grouped.set(label, []);
    grouped.get(label).push(job);
  });

  const groups = Array.from(grouped.entries()).map(([label, entries]) => {
    const sorted = entries.sort((a, b) => {
      if (a.enabled !== b.enabled) return a.enabled ? -1 : 1;
      return getCronJobDisplayName(a).localeCompare(getCronJobDisplayName(b));
    });
    return { label, jobs: sorted };
  });

  return groups.sort((a, b) => {
    if (a.label === 'Other') return 1;
    if (b.label === 'Other') return -1;
    return a.label.localeCompare(b.label);
  });
}

async function renderCronLinkMenu(ctx) {
  if (!CRON_API_TOKEN) {
    await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_TOKEN missing).', {
      reply_markup: buildBackKeyboard('cron:menu'),
    });
    return;
  }
  let jobs = [];
  try {
    const response = await fetchCronJobs();
    jobs = response.jobs || [];
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'list',
      error,
      userId: ctx.from?.id,
      projectId: null,
      correlationId,
    });
    if (
      await renderCronRateLimitIfNeeded(ctx, error, {
        reply_markup: buildBackKeyboard('cron:menu'),
      }, correlationId)
    ) {
      return;
    }
    await renderOrEdit(
      ctx,
      formatCronApiErrorNotice('Failed to list cron jobs', error, correlationId),
      { reply_markup: buildBackKeyboard('cron:menu') },
    );
    return;
  }
  const projects = await loadProjects();
  const links = await listCronJobLinks();
  const linkMap = new Map(links.map((link) => [String(link.cronJobId), link]));
  await autoLinkCronJobs(jobs, projects, linkMap);
  const grouped = groupCronJobs(jobs, projects, linkMap);

  const lines = ['🧷 Link cron jobs to projects:', ''];
  grouped.forEach((group) => {
    group.jobs.forEach((job) => {
      lines.push(`[${group.label}] ${getCronJobDisplayName(job)}`);
    });
  });

  const inline = new InlineKeyboard();
  grouped.forEach((group) => {
    group.jobs.forEach((job) => {
      const label = buildCronJobButtonLabel(job);
      inline.text(label, `cronlink:select:${job.id}`).row();
    });
  });
  inline.text('⬅️ Back', 'cron:menu');

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderCronProjectFilterMenu(ctx) {
  const projects = await loadProjects();
  const inline = new InlineKeyboard();
  inline.text('📋 All projects', 'cron:list').row();
  projects.forEach((project) => {
    inline.text(project.name || project.id, `cronlink:filter:${project.id}`).row();
  });
  inline.text('🧹 Other only', 'cronlink:other').row();
  inline.text('⬅️ Back', 'cron:menu');
  await renderOrEdit(ctx, 'Filter cron jobs by project:', { reply_markup: inline });
}

async function renderCronLinkProjectPicker(ctx, jobId, messageContext) {
  const projects = await loadProjects();
  const link = await getCronJobLink(jobId);
  const current = link?.projectId || null;
  const lines = ['Select a project for this cron job:'];
  const inline = new InlineKeyboard();
  projects.forEach((project) => {
    const label = project.id === current ? `✅ ${project.name || project.id}` : project.name || project.id;
    inline.text(label, `cronlink:set:${jobId}:${project.id}`).row();
  });
  inline.text(current ? '✅ Other' : 'Other', `cronlink:set_other:${jobId}`).row();
  inline.text('🏷 Set label', `cronlink:label:${jobId}`).row();
  inline.text('⬅️ Back', 'cronlink:menu');
  if (messageContext) {
    await renderStateMessage(ctx, { messageContext }, lines.join('\n'), { reply_markup: inline });
    return;
  }
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function updateCronJobLink(ctx, jobId, projectId, messageContext) {
  const link = await getCronJobLink(jobId);
  const label = link?.label || null;
  await upsertCronJobLink(jobId, projectId, label);
  await renderCronLinkProjectPicker(ctx, jobId, messageContext);
}

async function handleCronLinkLabelInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a label.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(state.backCallback || 'cron:menu'),
    });
    return;
  }
  const link = await getCronJobLink(state.jobId);
  await upsertCronJobLink(state.jobId, link?.projectId || null, text);
  clearUserState(ctx.from.id);
  await renderCronLinkProjectPicker(ctx, state.jobId, state.messageContext);
}

async function renderCronJobDetails(ctx, jobId, options = {}) {
  let job;
  try {
    job = await fetchCronJob(jobId);
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'get',
      error,
      userId: ctx.from?.id,
      projectId: null,
      correlationId,
    });
    if (
      await renderCronRateLimitIfNeeded(ctx, error, {
        reply_markup: buildBackKeyboard(options.backCallback || 'cron:menu'),
      }, correlationId)
    ) {
      return;
    }
    await renderOrEdit(
      ctx,
      formatCronApiErrorNotice('Failed to load cron job', error, correlationId),
      { reply_markup: buildBackKeyboard(options.backCallback || 'cron:menu') },
    );
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
  const projects = await loadProjects();
  const link = await getCronJobLink(jobId);
  const projectLabel = resolveCronProjectLabel(job, link, projects);
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
    `Project: ${projectLabel}`,
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
    .text('🧷 Link project', `cronlink:select:${jobId}`)
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
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'get',
      error,
      userId: ctx.from?.id,
      projectId: null,
      correlationId,
    });
    if (
      await renderCronRateLimitIfNeeded(ctx, error, {
        reply_markup: buildBackKeyboard('cron:menu'),
      }, correlationId)
    ) {
      return;
    }
    await renderOrEdit(
      ctx,
      formatCronApiErrorNotice('Failed to load cron job', error, correlationId),
      { reply_markup: buildBackKeyboard('cron:menu') },
    );
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
  await renderOrEdit(
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
  await renderOrEdit(ctx, 'Send new URL for this job. Or press Cancel.', {
    reply_markup: buildCancelKeyboard(),
  });
}

async function promptCronNameInput(ctx, jobId, backCallback) {
  setUserState(ctx.from.id, {
    type: 'cron_edit_name',
    jobId,
    backCallback,
  });
  await renderOrEdit(ctx, 'Send new name for this job (or type "clear" to remove).', {
    reply_markup: buildCancelKeyboard(),
  });
}

async function promptCronTimezoneInput(ctx, jobId, backCallback) {
  setUserState(ctx.from.id, {
    type: 'cron_edit_timezone',
    jobId,
    backCallback,
  });
  await renderOrEdit(ctx, 'Send timezone (e.g. Europe/Berlin). Or press Cancel.', {
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
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
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
    const validation = validateCronUrlInput(text);
    if (!validation.valid) {
      await ctx.reply(`Invalid URL: ${validation.message}`);
      return;
    }
    await startCronCreateWizard(ctx, validation.url, state.backCallback);
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
    clearCronJobsCache();
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'update',
      error,
      userId: ctx.from?.id,
      projectId: state.projectId,
      correlationId,
    });
    if (await replyCronRateLimitIfNeeded(ctx, error, correlationId)) {
      return;
    }
    await ctx.reply(formatCronApiErrorNotice('Failed to update cron job', error, correlationId));
  }
}

async function handleCronEditUrlMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a URL or press Cancel.');
    return;
  }
  const validation = validateCronUrlInput(text);
  if (!validation.valid) {
    await ctx.reply(`Invalid URL: ${validation.message}`);
    return;
  }
  try {
    const job = await fetchCronJob(state.jobId);
    const payload = buildCronJobUpdatePayload(job, { url: validation.url });
    await updateJob(state.jobId, payload);
    clearCronJobsCache();
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'update',
      error,
      userId: ctx.from?.id,
      projectId: state.projectId,
      correlationId,
    });
    if (await replyCronRateLimitIfNeeded(ctx, error, correlationId)) {
      return;
    }
    await ctx.reply(formatCronApiErrorNotice('Failed to update cron job', error, correlationId));
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
    clearCronJobsCache();
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'update',
      error,
      userId: ctx.from?.id,
      projectId: state.projectId,
      correlationId,
    });
    if (await replyCronRateLimitIfNeeded(ctx, error, correlationId)) {
      return;
    }
    await ctx.reply(formatCronApiErrorNotice('Failed to update cron job', error, correlationId));
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
    clearCronJobsCache();
    clearUserState(ctx.from.id);
    await ctx.reply('Cron job updated.');
    await renderCronJobDetails(ctx, state.jobId, { backCallback: state.backCallback });
  } catch (error) {
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'update',
      error,
      userId: ctx.from?.id,
      projectId: state.projectId,
      correlationId,
    });
    if (await replyCronRateLimitIfNeeded(ctx, error, correlationId)) {
      return;
    }
    await ctx.reply(formatCronApiErrorNotice('Failed to update cron job', error, correlationId));
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
  const urlValidation = validateCronUrlInput(targetUrl);
  if (!urlValidation.valid) {
    await ctx.reply(`Invalid URL: ${urlValidation.message}`);
    return;
  }

  const payload = buildCronJobPayload({
    name: jobName,
    url: urlValidation.url,
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
          clearCronJobsCache();
        } catch (error) {
          const correlationId = buildCronCorrelationId();
          logCronApiError({
            operation: 'delete',
            error,
            userId: ctx.from?.id,
            projectId: project.id,
            correlationId,
          });
          if (await replyCronRateLimitIfNeeded(ctx, error, correlationId)) {
            return;
          }
          await ctx.reply(
            formatCronApiErrorNotice('Failed to delete existing cron job', error, correlationId),
          );
        }
      }
    }
    const created = await createJob(payload);
    clearCronJobsCache();
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
    const correlationId = buildCronCorrelationId();
    logCronApiError({
      operation: 'create',
      error,
      userId: ctx.from?.id,
      projectId: project.id,
      correlationId,
    });
    if (await replyCronRateLimitIfNeeded(ctx, error, correlationId)) {
      return;
    }
    await ctx.reply(formatCronApiErrorNotice('Failed to create cron job', error, correlationId));
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
  await renderOrEdit(ctx, `Send the SQL query to execute on ${connection.name}.\n(Or press Cancel)`, {
    reply_markup: buildCancelKeyboard(),
  });
}

async function listSupabaseTables(ctx, connectionId) {
  try {
    const { rows } = await runSupabaseQuery(connectionId, {
      text: `
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
      `,
    });
    const tableNames = rows.map((row) => row.table_name);
    const inline = new InlineKeyboard();
    tableNames.forEach((name) => {
      inline.text(name, `supabase:table:${connectionId}:${encodeURIComponent(name)}`).row();
    });
    inline.text('⬅️ Back', `supabase:conn:${connectionId}`);
    const lines = [
      `Tables (${tableNames.length})`,
      '',
      ...tableNames.map((name) => `• ${name}`),
    ];
    await renderOrEdit(ctx, truncateMessage(lines.join('\n'), SUPABASE_MESSAGE_LIMIT), {
      reply_markup: inline,
    });
  } catch (error) {
    console.error('[supabase] Failed to list tables', error);
    await renderOrEdit(ctx, `SQL error: ${error.message}`, {
      reply_markup: buildBackKeyboard(`supabase:conn:${connectionId}`),
    });
  }
}

async function fetchSupabaseTableColumns(connectionId, tableName) {
  const { rows } = await runSupabaseQuery(connectionId, {
    text: `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
      ORDER BY ordinal_position;
    `,
    values: [tableName],
  });
  return rows;
}

async function fetchSupabasePrimaryKeyColumns(connectionId, tableName) {
  const qualified = `public.${quoteIdentifier(tableName)}`;
  const { rows } = await runSupabaseQuery(connectionId, {
    text: `
      SELECT a.attname
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE i.indrelid = $1::regclass AND i.indisprimary;
    `,
    values: [qualified],
  });
  return rows.map((row) => row.attname);
}

async function fetchSupabaseRowEstimate(connectionId, tableName) {
  const qualified = `public.${quoteIdentifier(tableName)}`;
  const { rows } = await runSupabaseQuery(connectionId, {
    text: 'SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = $1::regclass;',
    values: [qualified],
  });
  return rows[0]?.estimate ?? null;
}

async function renderSupabaseTableDetails(ctx, connectionId, tableName) {
  const connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    await renderOrEdit(ctx, 'Supabase connection not found.', {
      reply_markup: buildBackKeyboard('supabase:connections'),
    });
    return;
  }
  try {
    const [columns, estimate] = await Promise.all([
      fetchSupabaseTableColumns(connectionId, tableName),
      fetchSupabaseRowEstimate(connectionId, tableName),
    ]);
    const lines = [
      `Table: ${tableName}`,
      '',
      'Columns:',
      ...columns.map((column) => `• ${column.column_name} (${column.data_type})`),
      '',
      `Row count (estimate): ${estimate ?? 'unknown'}`,
    ];
    const inline = new InlineKeyboard()
      .text('👁 View rows', `supabase:rows:${connectionId}:${encodeURIComponent(tableName)}:0`)
      .row()
      .text('🔢 Row count', `supabase:count:${connectionId}:${encodeURIComponent(tableName)}`)
      .row()
      .text('⬅️ Back', `supabase:tables:${connectionId}`);
    await renderOrEdit(ctx, truncateMessage(lines.join('\n'), SUPABASE_MESSAGE_LIMIT), {
      reply_markup: inline,
    });
  } catch (error) {
    console.error('[supabase] Failed to load table details', error);
    await renderOrEdit(ctx, `SQL error: ${error.message}`, {
      reply_markup: buildBackKeyboard(`supabase:tables:${connectionId}`),
    });
  }
}

async function renderSupabaseTableCount(ctx, connectionId, tableName) {
  try {
    const estimate = await fetchSupabaseRowEstimate(connectionId, tableName);
    let exact = null;
    let countError = null;
    try {
      const { rows } = await runSupabaseQuery(connectionId, {
        text: `SELECT COUNT(*)::bigint AS count FROM ${quoteIdentifier('public')}.${quoteIdentifier(tableName)};`,
        query_timeout: SUPABASE_QUERY_TIMEOUT_MS,
      });
      exact = rows[0]?.count ?? null;
    } catch (error) {
      countError = error;
    }
    const lines = [
      `Table: ${tableName}`,
      '',
      `Row count (estimate): ${estimate ?? 'unknown'}`,
      exact ? `Row count (exact): ${exact}` : null,
      countError ? `Exact count unavailable: ${truncateText(countError.message, 80)}` : null,
    ].filter(Boolean);
    const inline = new InlineKeyboard()
      .text('👁 View rows', `supabase:rows:${connectionId}:${encodeURIComponent(tableName)}:0`)
      .row()
      .text('⬅️ Back', `supabase:table:${connectionId}:${encodeURIComponent(tableName)}`);
    await renderOrEdit(ctx, truncateMessage(lines.join('\n'), SUPABASE_MESSAGE_LIMIT), {
      reply_markup: inline,
    });
  } catch (error) {
    console.error('[supabase] Failed to count rows', error);
    await renderOrEdit(ctx, `SQL error: ${error.message}`, {
      reply_markup: buildBackKeyboard(`supabase:table:${connectionId}:${encodeURIComponent(tableName)}`),
    });
  }
}

async function renderSupabaseTableRows(ctx, connectionId, tableName, page) {
  const safePage = Number.isFinite(page) && page >= 0 ? page : 0;
  const offset = safePage * SUPABASE_ROWS_PAGE_SIZE;
  try {
    const columns = await fetchSupabaseTableColumns(connectionId, tableName);
    const columnNames = columns.map((column) => column.column_name);
    const primaryKeys = await fetchSupabasePrimaryKeyColumns(connectionId, tableName);
    const orderBy = primaryKeys.length
      ? `ORDER BY ${primaryKeys.map((key) => quoteIdentifier(key)).join(', ')}`
      : '';
    const { rows } = await runSupabaseQuery(connectionId, {
      text: `
        SELECT *
        FROM ${quoteIdentifier('public')}.${quoteIdentifier(tableName)}
        ${orderBy}
        LIMIT $1 OFFSET $2;
      `,
      values: [SUPABASE_ROWS_PAGE_SIZE + 1, offset],
    });
    const hasNext = rows.length > SUPABASE_ROWS_PAGE_SIZE;
    const trimmed = rows.slice(0, SUPABASE_ROWS_PAGE_SIZE).map(applyRowMasking);
    const inline = new InlineKeyboard();
    if (safePage > 0) {
      inline.text(
        '⬅ Prev',
        `supabase:rows:${connectionId}:${encodeURIComponent(tableName)}:${safePage - 1}`,
      );
    }
    if (hasNext) {
      inline.text(
        '➡ Next',
        `supabase:rows:${connectionId}:${encodeURIComponent(tableName)}:${safePage + 1}`,
      );
    }
    if (safePage > 0 || hasNext) {
      inline.row();
    }
    inline.text('⬅️ Back', `supabase:table:${connectionId}:${encodeURIComponent(tableName)}`);
    const headerLines = [
      `Table: ${tableName}`,
      `Rows: ${SUPABASE_ROWS_PAGE_SIZE} (page ${safePage + 1})`,
      '',
    ].map(escapeHtml);
    let displayed = trimmed;
    let body = trimmed.length
      ? formatRowsAsCodeBlock(displayed, columnNames)
      : '<pre>(no rows)</pre>';
    let message = `${headerLines.join('\n')}${body}`;
    while (message.length > SUPABASE_MESSAGE_LIMIT && displayed.length > 1) {
      displayed = displayed.slice(0, -1);
      body = formatRowsAsCodeBlock(displayed, columnNames, '(truncated)');
      message = `${headerLines.join('\n')}${body}`;
    }
    if (message.length > SUPABASE_MESSAGE_LIMIT) {
      body = '<pre>(truncated)</pre>';
      message = `${headerLines.join('\n')}${body}`;
    }
    await renderOrEdit(ctx, message, {
      reply_markup: inline,
      parse_mode: 'HTML',
    });
  } catch (error) {
    console.error('[supabase] Failed to load rows', error);
    await renderOrEdit(ctx, `SQL error: ${error.message}`, {
      reply_markup: buildBackKeyboard(`supabase:table:${connectionId}:${encodeURIComponent(tableName)}`),
    });
  }
}

async function runSupabaseSql(ctx, connectionId, sql) {
  try {
    const result = await runSupabaseQuery(connectionId, sql);
    if (result.rows && result.rows.length) {
      const lines = result.rows.slice(0, 50).map((row) => formatSqlRow(row));
      const output = truncateMessage(lines.join('\n'), SUPABASE_MESSAGE_LIMIT);
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
  const dsnInfo = await resolveSupabaseConnectionDsn(connection);
  if (!dsnInfo.dsn) {
    throw new Error(dsnInfo.error || `Supabase DSN not configured for ${connection.name}.`);
  }
  const pool = await getSupabasePool(connectionId, dsnInfo.dsn);
  return pool.query(normalizeSupabaseQuery(sql));
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
  await ensureAnswerCallback(ctx);
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
    const validation = validateWorkingDirInput(ctx.message.text);
    if (!validation.ok) {
      await ctx.reply(validation.error);
      return;
    }
    state.draft.workingDir = validation.value;
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
    workingDirCustom: 'Send working directory path (absolute).\n(Or press Cancel)',
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
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard('gsettings:menu'),
    });
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
    await renderProjectSettingsForMessage(state.messageContext, newId, '✅ Updated');
    if (!state.messageContext) {
      await renderProjectSettings(ctx, newId, '✅ Updated');
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
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, '✅ Updated');
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
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, '✅ Updated');
  }
}

async function handleEditWorkingDirStep(ctx, state) {
  const rawText = ctx.message.text;
  if (!rawText) {
    await respond(ctx, 'Please send text.');
    return;
  }
  const trimmed = rawText.trim();
  if (trimmed.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await respond(ctx, 'Operation cancelled.');
    await renderMainMenu(ctx);
    return;
  }

  const projects = await loadProjects();
  const idx = projects.findIndex((project) => project.id === state.projectId);
  if (idx === -1) {
    await respond(ctx, 'Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const project = projects[idx];
  let nextWorkingDir = trimmed;
  let isWorkingDirCustom = true;

  if (trimmed === '-') {
    if (!project.repoSlug) {
      await respond(ctx, 'Cannot auto-set workingDir without repoSlug.');
      return;
    }
    const defaultDir = getDefaultWorkingDir(project.repoSlug);
    if (!defaultDir) {
      await respond(ctx, 'Cannot derive workingDir from repoSlug.');
      return;
    }
    nextWorkingDir = defaultDir;
    isWorkingDirCustom = false;
  } else {
    const validation = validateWorkingDirInput(rawText);
    if (!validation.ok) {
      await respond(ctx, validation.error);
      return;
    }
    nextWorkingDir = validation.value;
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
  const validation = await validateWorkingDir({ ...project, workingDir: nextWorkingDir });
  const notice = formatWorkingDirValidationNotice(validation);
  if (!validation.ok) {
    console.warn('[workingDir] Validation failed after save', {
      projectId: state.projectId,
      code: validation.code,
      workingDir: nextWorkingDir,
      expectedCheckoutDir: validation.expectedCheckoutDir,
    });
  }
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, notice);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, notice);
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
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, '✅ Updated');
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
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, '✅ Updated');
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
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, '✅ Updated');
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
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, '✅ Updated');
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
  await renderGlobalSettingsForMessage(state.messageContext, '✅ Updated');
  if (!state.messageContext) {
    await renderGlobalSettings(ctx, '✅ Updated');
  }
}

async function runProjectDiagnostics(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await respond(ctx, 'Project not found.');
    return;
  }

  const command = project.diagnosticCommand || project.testCommand;
  if (!command) {
    await respond(ctx, 'No diagnostic/test command configured for this project.');
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
      await respond(
        ctx,
        'This project is not fully configured: repoSlug is missing. Use "📝 Edit repo" to set it.',
      );
      return;
    }
    console.error('Failed to prepare repository for diagnostics', error);
  }

  const workingDir = project.workingDir || repoInfo?.workingDir;
  if (!workingDir) {
    await respond(ctx, 'No working directory configured for this project.');
    return;
  }

  if (!project.workingDir) {
    await updateProjectField(projectId, 'workingDir', workingDir);
  }

  const validation = await validateWorkingDir({ ...project, workingDir });
  if (!validation.ok) {
    console.warn('[diagnostics] Working dir validation failed', {
      projectId,
      code: validation.code,
      workingDir,
      expectedCheckoutDir: validation.expectedCheckoutDir,
    });
    await respond(ctx, formatWorkingDirValidationMessage(validation));
    return;
  }

  const result = await runCommandInProject({ ...project, workingDir }, command);
  if (result.exitCode === 0) {
    await respond(
      ctx,
      `🧪 Diagnostics finished successfully.\nProject: ${project.name || project.id}\nDuration: ${result.durationMs} ms\n\nLast output:\n${result.stdout || '(no output)'}`,
    );
    return;
  }

  const classification = classifyDiagnosticsError({ result, project, workingDir, validation });
  if (classification?.reason === 'WORKING_DIR_INVALID') {
    await respond(ctx, classification.message);
    return;
  }

  const errorExcerpt = result.stderr || result.stdout || '(no output)';
  await respond(
    ctx,
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

async function renderProjectSettings(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const globalSettings = await loadGlobalSettings();
  const view = buildProjectSettingsView(project, globalSettings, notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function buildProjectSettingsView(project, globalSettings, notice) {
  const effectiveBase = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  const isDefault = globalSettings.defaultProjectId === project.id;
  const name = project.name || project.id;
  const tokenKey = project.githubTokenEnvKey || 'GITHUB_TOKEN';
  const tokenLabel = tokenKey === 'GITHUB_TOKEN' ? 'GITHUB_TOKEN (default)' : tokenKey;
  const projectTypeLabel = getProjectTypeLabel(project);

  const lines = [
    `Project: ${isDefault ? '⭐ ' : ''}${name} (id: ${project.id})`,
    notice || null,
    '',
    `Project type: ${projectTypeLabel}`,
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
  ].filter((line) => line !== null);

  const inline = new InlineKeyboard()
    .text('✏️ Edit project', `proj:project_menu:${project.id}`)
    .text('🌱 Change base branch', `proj:change_base:${project.id}`)
    .row()
    .text('🏷 Project type', `proj:project_type:${project.id}`)
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
    .text('🔐 Env Vault', `envvault:menu:${project.id}`)
    .text('🤖 Telegram Setup', `tgbot:menu:${project.id}`)
    .row()
    .text('📝 SQL runner', `proj:sql_menu:${project.id}`)
    .row()
    .text('📣 Log alerts', `projlog:menu:${project.id}`)
    .row();

  if (!isDefault) {
    inline.text('⭐ Set as default project', `proj:set_default:${project.id}`).row();
  }

  inline.text('🗑 Delete project', `proj:delete:${project.id}`).text('⬅️ Back', 'proj:list');

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectTypeMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const options = getProjectTypeOptions();
  const current = project.projectType || project.project_type || 'other';
  const inline = new InlineKeyboard();
  options.forEach((option) => {
    const label = option.id === current ? `✅ ${option.label}` : option.label;
    inline.text(label, `proj:project_type_set:${projectId}:${option.id}`).row();
  });
  inline.text('⬅️ Back', `proj:open:${projectId}`);
  await renderOrEdit(ctx, `Select project type for ${project.name || project.id}:`, {
    reply_markup: inline,
  });
}

async function updateProjectType(ctx, projectId, typeId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  project.projectType = typeId;
  project.project_type = typeId;
  await saveProjects(projects);
  await renderProjectSettings(ctx, projectId);
}

function buildProjectLogAlertsView(project, settings) {
  const forwarding = normalizeProjectLogSettings(settings);
  const levelsLabel = forwarding.levels.length ? forwarding.levels.join(' / ') : 'error';
  const selected = new Set(forwarding.levels);
  const destinationLabel = forwarding.destinationChatId || 'not set';
  const lines = [
    `📣 Log alerts — ${project.name || project.id}`,
    '',
    `Status: ${forwarding.enabled ? 'Enabled' : 'Disabled'}`,
    `Levels: ${levelsLabel}`,
    `Destination chat: ${destinationLabel}`,
  ];

  const inline = new InlineKeyboard()
    .text(forwarding.enabled ? '✅ Enabled' : '🚫 Disabled', `projlog:toggle:${project.id}`)
    .row()
    .text(`❗ Errors: ${selected.has('error') ? 'ON' : 'OFF'}`, `projlog:level:error:${project.id}`)
    .text(`⚠️ Warnings: ${selected.has('warn') ? 'ON' : 'OFF'}`, `projlog:level:warn:${project.id}`)
    .row()
    .text(`ℹ️ Info: ${selected.has('info') ? 'ON' : 'OFF'}`, `projlog:level:info:${project.id}`)
    .row()
    .text('✏️ Set chat_id', `projlog:set_chat:${project.id}`)
    .text('📌 Use this chat', `projlog:use_chat:${project.id}`)
    .row()
    .text('🧹 Clear chat_id', `projlog:clear_chat:${project.id}`)
    .row()
    .text('🧾 Recent logs', `projlog:logs:${project.id}:0`)
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
  const settings = await getProjectLogSettingsWithDefaults(projectId);
  const view = buildProjectLogAlertsView(project, settings);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function renderProjectLogAlertsForMessage(messageContext, projectId, notice) {
  if (!messageContext) return;
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) return;
  const settings = await getProjectLogSettingsWithDefaults(projectId);
  const view = buildProjectLogAlertsView(project, settings);
  const text = notice ? `${notice}\n\n${view.text}` : view.text;
  try {
    await bot.api.editMessageText(
      messageContext.chatId,
      messageContext.messageId,
      text,
      normalizeTelegramExtra({ reply_markup: view.keyboard }),
    );
  } catch (error) {
    console.error('[UI] Failed to update log alerts message', error);
  }
}

function formatLogTimestamp(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toISOString();
}

function buildProjectLogListView(project, logs, page, hasNext) {
  const lines = [`🧾 Last logs — ${project.name || project.id}`, `Page: ${page + 1}`];
  if (!logs.length) {
    lines.push('', 'No logs stored yet.');
  } else {
    lines.push('');
    logs.forEach((log) => {
      const timestamp = formatLogTimestamp(log.timestamp || log.createdAt);
      const levelLabel = log.level ? log.level.toUpperCase() : 'UNKNOWN';
      const message = truncateText(log.message, 120);
      lines.push(`• ${timestamp} — ${levelLabel} — ${log.service}: ${message}`);
    });
  }

  const inline = new InlineKeyboard();
  logs.forEach((log) => {
    const label = `${(log.level || 'log').toUpperCase()} ${truncateText(log.message, 24)}`;
    inline.text(label, `projlog:log:${project.id}:${log.id}:${page}`).row();
  });

  if (page > 0) {
    inline.text('⬅️ Prev', `projlog:logs:${project.id}:${page - 1}`);
  }
  if (hasNext) {
    inline.text('➡️ Next', `projlog:logs:${project.id}:${page + 1}`);
  }
  if (page > 0 || hasNext) {
    inline.row();
  }
  inline.text('⬅️ Back', `projlog:menu:${project.id}`);

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectLogList(ctx, projectId, page) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const safePage = Math.max(0, Number.isFinite(page) ? page : 0);
  const offset = safePage * 10;
  const logs = await listRecentLogs(projectId, 11, offset);
  const pageLogs = logs.slice(0, 10);
  const hasNext = logs.length > 10;
  const view = buildProjectLogListView(project, pageLogs, safePage, hasNext);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function buildProjectLogDetailView(project, logEntry, page) {
  const timestamp = formatLogTimestamp(logEntry.timestamp || logEntry.createdAt);
  const lines = [
    `🧾 Log details — ${project.name || project.id}`,
    '',
    `Level: ${(logEntry.level || 'unknown').toUpperCase()}`,
    `Service: ${logEntry.service || '-'}`,
    `Env: ${logEntry.env || '-'}`,
    `Time: ${timestamp}`,
    `Message: ${truncateText(logEntry.message, 1000) || '(no message)'}`,
  ];

  if (logEntry.stack) {
    lines.push(`Stack: ${truncateText(logEntry.stack, 3000)}`);
  }

  const contextText = formatContext(logEntry.context, 1500);
  if (contextText) {
    lines.push(`Context: ${contextText}`);
  }

  const inline = new InlineKeyboard()
    .text('⬅️ Back to logs', `projlog:logs:${project.id}:${page}`)
    .row()
    .text('⬅️ Back to alerts', `projlog:menu:${project.id}`);

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectLogDetail(ctx, projectId, logId, page) {
  if (!logId) {
    await renderProjectLogList(ctx, projectId, page);
    return;
  }
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const logEntry = await getRecentLogById(projectId, logId);
  if (!logEntry) {
    await renderProjectLogList(ctx, projectId, page);
    return;
  }
  const view = buildProjectLogDetailView(project, logEntry, page);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function handleProjectLogChatInput(ctx, state) {
  const value = ctx.message?.text?.trim();
  if (!value) {
    await ctx.reply('Please provide a chat_id value.', { reply_markup: buildCancelKeyboard() });
    return;
  }
  if (!/^-?\\d+$/.test(value)) {
    await ctx.reply('Invalid chat_id format. Please send a numeric chat_id.', {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }
  const settings = await getProjectLogSettingsWithDefaults(state.projectId);
  settings.destinationChatId = value;
  await upsertProjectLogSettings(state.projectId, settings);
  clearUserState(ctx.from.id);
  await renderProjectLogAlertsForMessage(state.messageContext, state.projectId, '✅ Updated');
  if (!state.messageContext) {
    await renderProjectLogAlerts(ctx, state.projectId);
  }
}

async function renderProjectSettingsForMessage(messageContext, projectId, notice) {
  if (!messageContext) {
    return;
  }
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    return;
  }
  const globalSettings = await loadGlobalSettings();
  const view = buildProjectSettingsView(project, globalSettings, notice);
  try {
    await bot.api.editMessageText(
      messageContext.chatId,
      messageContext.messageId,
      view.text,
      normalizeTelegramExtra({ reply_markup: view.keyboard }),
    );
  } catch (error) {
    if (isButtonDataInvalidError(error)) {
      await handleTelegramUiError({ reply: (...args) => bot.api.sendMessage(messageContext.chatId, ...args) }, error);
      return;
    }
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

async function renderProjectSqlMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const envSetId = await ensureProjectEnvSet(projectId);
  const envStatus = await buildEnvVaultDbStatus(project, envSetId);
  const supabaseStatus = await buildSupabaseBindingStatus(project);
  const missingRequired = envStatus.missingRequired || [];

  const lines = [
    `📝 SQL runner — ${project.name || project.id}`,
    '',
    `Env Vault: ${envStatus.summary}`,
    `Supabase binding: ${supabaseStatus.summary}`,
  ];

  const inline = new InlineKeyboard();
  if (envStatus.ready) {
    inline.text('🔐 Use Env Vault', `envvault:sql:${projectId}`).row();
  } else if (missingRequired.length) {
    inline.text('➕ Add missing required keys', `envvault:add_missing:${projectId}`).row();
  }
  if (project.supabaseConnectionId) {
    inline.text('🗄 Use Supabase binding', `proj:sql_supabase:${projectId}`).row();
  }
  inline.text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function ensureProjectEnvSet(projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) return null;
  let envSetId = project.defaultEnvSetId;
  if (!envSetId) {
    envSetId = await ensureDefaultEnvVarSet(projectId);
    project.defaultEnvSetId = envSetId;
    project.default_env_set_id = envSetId;
    await saveProjects(projects);
  }
  return envSetId;
}

async function buildEnvVaultDbStatus(project, envSetId) {
  const keys = await listEnvVarKeys(project.id, envSetId);
  const hasDsn = keys.includes('DATABASE_URL') || keys.includes('SUPABASE_DSN');
  const hasServiceRole = keys.includes('SUPABASE_SERVICE_ROLE_KEY') || keys.includes('SUPABASE_SERVICE_ROLE');
  const hasSupabasePair = keys.includes('SUPABASE_URL') && hasServiceRole;
  if (hasDsn || hasSupabasePair) {
    return { ready: true, summary: '✅ DB ready', missingRequired: [] };
  }
  const missing = [];
  if (!keys.includes('SUPABASE_URL')) missing.push('SUPABASE_URL');
  if (!hasServiceRole) missing.push('SUPABASE_SERVICE_ROLE_KEY');
  if (!keys.includes('DATABASE_URL')) missing.push('DATABASE_URL');
  if (!keys.includes('SUPABASE_DSN')) missing.push('SUPABASE_DSN');
  return { ready: false, summary: `⚠️ Missing: ${missing.join(', ')}`, missingRequired: missing };
}

async function buildSupabaseBindingStatus(project) {
  if (!project.supabaseConnectionId) {
    return { ready: false, summary: 'not configured' };
  }
  const connection = await findSupabaseConnection(project.supabaseConnectionId);
  if (!connection) {
    return { ready: false, summary: 'missing connection' };
  }
  const dsnInfo = await resolveSupabaseConnectionDsn(connection);
  if (!dsnInfo.dsn) {
    return { ready: false, summary: dsnInfo.error || `env ${connection.envKey} missing` };
  }
  return { ready: true, summary: `✅ ${connection.id}` };
}

async function resolveEnvVaultConnection(projectId, envSetId) {
  const envVars = await listEnvVars(projectId, envSetId);
  const lookup = new Map(envVars.map((entry) => [entry.key, entry]));

  const dsnKey = lookup.get('DATABASE_URL') || lookup.get('SUPABASE_DSN');
  if (dsnKey) {
    const value = await getEnvVarValue(projectId, dsnKey.key, envSetId);
    return { dsn: value, source: dsnKey.key };
  }

  const supabaseUrl = lookup.get('SUPABASE_URL');
  const serviceRole = lookup.get('SUPABASE_SERVICE_ROLE_KEY') || lookup.get('SUPABASE_SERVICE_ROLE');
  if (supabaseUrl && serviceRole) {
    const urlValue = await getEnvVarValue(projectId, 'SUPABASE_URL', envSetId);
    const roleKey = serviceRole.key;
    const roleValue = await getEnvVarValue(projectId, roleKey, envSetId);
    const dsn = buildSupabaseDsnFromUrl(urlValue, roleValue);
    return { dsn, source: `SUPABASE_URL+${roleKey}` };
  }

  return { dsn: null, source: null };
}

function buildSupabaseDsnFromUrl(supabaseUrl, serviceRoleKey) {
  const parsed = new URL(supabaseUrl);
  const projectRef = parsed.hostname.split('.')[0];
  const password = encodeURIComponent(serviceRoleKey);
  return `postgres://postgres:${password}@db.${projectRef}.supabase.co:5432/postgres?sslmode=require`;
}

async function runEnvVaultQuery(projectId, envSetId, sql) {
  const connection = await resolveEnvVaultConnection(projectId, envSetId);
  if (!connection.dsn) {
    throw new Error('Missing ENV Vault DB connection details.');
  }
  const poolKey = `${projectId}:${envSetId}`;
  let pool = envVaultPools.get(poolKey);
  if (!pool) {
    pool = new Pool({ connectionString: connection.dsn });
    envVaultPools.set(poolKey, pool);
  }
  return pool.query(sql);
}

async function runEnvVaultSql(ctx, projectId, envSetId, sql) {
  try {
    const result = await runEnvVaultQuery(projectId, envSetId, sql);
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

async function startProjectSqlInput(ctx, projectId, source) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const envSetId = await ensureProjectEnvSet(projectId);
  if (source === 'env_vault') {
    const status = await buildEnvVaultDbStatus(project, envSetId);
    if (!status.ready) {
      await renderOrEdit(ctx, `Env Vault DB not ready.\n${status.summary}`, {
        reply_markup: buildBackKeyboard(`proj:sql_menu:${projectId}`),
      });
      return;
    }
  }
  setUserState(ctx.from.id, {
    type: 'project_sql_input',
    projectId,
    envSetId,
    source,
    backCallback: `proj:sql_menu:${projectId}`,
    messageContext: getMessageTargetFromCtx(ctx),
  });
  await renderOrEdit(ctx, 'Send the SQL query to execute.\n(Or press Cancel)', {
    reply_markup: buildCancelKeyboard(),
  });
}

async function handleProjectSqlInput(ctx, state) {
  const sql = ctx.message.text?.trim();
  if (!sql) {
    await ctx.reply('Please send the SQL query as text.');
    return;
  }
  if (sql.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(state.backCallback || 'main:back'),
    });
    return;
  }

  if (state.source === 'supabase') {
    const project = await getProjectById(state.projectId, ctx);
    if (!project?.supabaseConnectionId) {
      await ctx.reply('Supabase binding is not configured.');
    } else {
      await runSupabaseSql(ctx, project.supabaseConnectionId, sql);
    }
  } else {
    await runEnvVaultSql(ctx, state.projectId, state.envSetId, sql);
  }

  clearUserState(ctx.from.id);
  await renderProjectSqlMenu(ctx, state.projectId);
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
  if (!CRON_API_TOKEN) {
    await renderOrEdit(ctx, 'Cron integration is not configured (CRON_API_TOKEN missing).', {
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
  await renderOrEdit(
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
      clearCronJobsCache();
    } catch (error) {
      const correlationId = buildCronCorrelationId();
      logCronApiError({
        operation: 'delete',
        error,
        userId: ctx.from?.id,
        projectId,
        correlationId,
      });
      if (
        await renderCronRateLimitIfNeeded(ctx, error, {
          reply_markup: buildBackKeyboard(`projcron:menu:${projectId}`),
        }, correlationId)
      ) {
        return;
      }
      await renderOrEdit(
        ctx,
        formatCronApiErrorNotice('Failed to delete cron job during unlink', error, correlationId),
        { reply_markup: buildBackKeyboard(`projcron:menu:${projectId}`) },
      );
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
  const view = await buildDataCenterView(ctx);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function buildDataCenterView(ctx) {
  const connections = await loadSupabaseConnections();
  const cronSettings = await getEffectiveCronSettings();
  const cronStatus = await getCronStatusLine(ctx, cronSettings);
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
  const view = await buildDataCenterView(null);
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

function buildGlobalSettingsView(settings, projects, notice) {
  const defaultProject = settings.defaultProjectId
    ? findProjectById(projects, settings.defaultProjectId)
    : undefined;
  const selfLogForwarding = getEffectiveSelfLogForwarding(settings);
  const lines = [
    notice || null,
    `defaultBaseBranch: ${settings.defaultBaseBranch || DEFAULT_BASE_BRANCH}`,
    `defaultProjectId: ${settings.defaultProjectId || '-'}` +
      (defaultProject ? ` (${defaultProject.name || defaultProject.id})` : ''),
    `selfLogForwarding: ${selfLogForwarding.enabled ? 'enabled' : 'disabled'} (${selfLogForwarding.levels.join('/')})`,
  ].filter(Boolean);
  return { text: lines.join('\n'), keyboard: buildSettingsKeyboard() };
}

async function renderGlobalSettings(ctx, notice) {
  const settings = await loadGlobalSettings();
  const projects = await loadProjects();
  const view = buildGlobalSettingsView(settings, projects, notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function renderGlobalSettingsForMessage(messageContext, notice) {
  if (!messageContext) return;
  const settings = await loadGlobalSettings();
  const projects = await loadProjects();
  const view = buildGlobalSettingsView(settings, projects, notice);
  try {
    await bot.api.editMessageText(
      messageContext.chatId,
      messageContext.messageId,
      view.text,
      normalizeTelegramExtra({ reply_markup: view.keyboard }),
    );
  } catch (error) {
    console.error('[UI] Failed to update global settings message', error);
  }
}

function buildSettingsKeyboard() {
  return new InlineKeyboard()
    .text('📣 Bot log alerts', 'gsettings:bot_log_alerts')
    .row()
    .text('📶 Ping test', 'gsettings:ping_test')
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
    .text('🧾 Recent logs', 'gsettings:bot_logs:0')
    .row()
    .text('⬅️ Back', 'gsettings:menu');

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderSelfLogAlerts(ctx) {
  const settings = await loadGlobalSettings();
  const view = buildSelfLogAlertsView(settings);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function buildSelfLogListView(logs, page, hasNext) {
  const lines = ['🧾 Recent bot logs', `Page: ${page + 1}`];
  if (!logs.length) {
    lines.push('', 'No logs stored yet.');
  } else {
    lines.push('');
    logs.forEach((log) => {
      const timestamp = formatLogTimestamp(log.createdAt);
      const levelLabel = log.level ? log.level.toUpperCase() : 'UNKNOWN';
      const message = truncateText(log.message, 120);
      lines.push(`• ${timestamp} — ${levelLabel}: ${message}`);
    });
  }

  const inline = new InlineKeyboard();
  logs.forEach((log) => {
    const label = `${(log.level || 'log').toUpperCase()} ${truncateText(log.message, 24)}`;
    inline.text(label, `gsettings:bot_log:${log.id}:${page}`).row();
  });
  if (page > 0) {
    inline.text('⬅️ Prev', `gsettings:bot_logs:${page - 1}`);
  }
  if (hasNext) {
    inline.text('➡️ Next', `gsettings:bot_logs:${page + 1}`);
  }
  if (page > 0 || hasNext) {
    inline.row();
  }
  inline.text('⬅️ Back', 'gsettings:bot_log_alerts');

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderSelfLogList(ctx, page) {
  const safePage = Math.max(0, Number.isFinite(page) ? page : 0);
  const offset = safePage * 10;
  const logs = await listSelfLogs(11, offset);
  const pageLogs = logs.slice(0, 10);
  const hasNext = logs.length > 10;
  const view = buildSelfLogListView(pageLogs, safePage, hasNext);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function buildSelfLogDetailView(logEntry, page) {
  const timestamp = formatLogTimestamp(logEntry.createdAt);
  const lines = [
    '🧾 Bot log detail',
    '',
    `Level: ${(logEntry.level || 'unknown').toUpperCase()}`,
    `Time: ${timestamp}`,
    `Message: ${truncateText(logEntry.message, 1500) || '(no message)'}`,
  ];

  if (logEntry.stack) {
    lines.push(`Stack: ${truncateText(logEntry.stack, 3000)}`);
  }

  const contextText = formatContext(logEntry.context, 1500);
  if (contextText) {
    lines.push(`Context: ${contextText}`);
  }

  const inline = new InlineKeyboard()
    .text('⬅️ Back to logs', `gsettings:bot_logs:${page}`)
    .row()
    .text('⬅️ Back to alerts', 'gsettings:bot_log_alerts');

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderSelfLogDetail(ctx, logId, page) {
  if (!logId) {
    await renderSelfLogList(ctx, page);
    return;
  }
  const logEntry = await getSelfLogById(logId);
  if (!logEntry) {
    await renderSelfLogList(ctx, page);
    return;
  }
  const view = buildSelfLogDetailView(logEntry, page);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function runPingTest(ctx) {
  const checks = [];
  const projects = await loadProjects();
  const globalSettings = await loadGlobalSettings();
  const defaultProject =
    (globalSettings.defaultProjectId && findProjectById(projects, globalSettings.defaultProjectId)) ||
    projects[0];

  const addCheck = (status, label, detail, hint) => {
    checks.push({ status, label, detail, hint });
  };

  const formatCheck = (check) => {
    const icon = check.status === 'ok' ? '✅' : check.status === 'warn' ? '⚠️' : '❌';
    const detail = check.detail ? ` — ${check.detail}` : '';
    const hint = check.hint ? `\n   ↳ ${check.hint}` : '';
    return `${icon} ${check.label}${detail}${hint}`;
  };

  const gitVersion = await checkGitBinary();
  if (gitVersion.ok) {
    addCheck('ok', 'Git binary', gitVersion.detail);
  } else {
    addCheck('fail', 'Git binary', gitVersion.detail, 'Install git in the runtime environment.');
  }

  if (!defaultProject) {
    addCheck('fail', 'Project', 'No project configured', 'Add a project and set repo slug.');
  } else {
    addCheck('ok', 'Project', `${defaultProject.name || defaultProject.id}`);
  }

  let repoInfo = null;
  if (defaultProject) {
    try {
      repoInfo = getRepoInfo(defaultProject);
      addCheck('ok', 'Repo configured', repoInfo.repoSlug);
    } catch (error) {
      addCheck('fail', 'Repo configured', error.message, 'Set owner/repo in project settings.');
    }
  }

  const tokenInfo = defaultProject
    ? await resolveGithubToken(defaultProject)
    : { token: null, source: null, error: 'no project' };
  if (!tokenInfo.token) {
    addCheck('fail', 'GitHub token', tokenInfo.error || 'missing', 'Set GITHUB_TOKEN or configure env vault.');
  } else {
    addCheck('ok', 'GitHub token', `${tokenInfo.source} (${tokenInfo.key})`);
  }

  if (repoInfo) {
    const apiCheck = await checkGithubApi(repoInfo, tokenInfo.token);
    addCheck(apiCheck.status, 'GitHub API', apiCheck.detail, apiCheck.hint);

    const workingDirCheck = await checkWorkingDir(defaultProject, repoInfo.workingDir);
    addCheck(workingDirCheck.status, 'Working dir', workingDirCheck.detail, workingDirCheck.hint);

    const baseBranch = defaultProject?.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
    const branchCheck = await checkRemoteBranch(repoInfo.repoUrl, tokenInfo.token, baseBranch);
    addCheck(branchCheck.status, 'Base branch', branchCheck.detail, branchCheck.hint);

    const fetchCheck = await checkGitFetch(repoInfo.repoUrl, tokenInfo.token, baseBranch);
    addCheck(fetchCheck.status, 'Git fetch', fetchCheck.detail, fetchCheck.hint);
  }

  const configDb = await checkConfigDbStatus();
  addCheck(configDb.ok ? 'ok' : 'fail', 'Config DB', configDb.message, configDb.ok ? null : 'Set PATH_APPLIER_CONFIG_DSN.');

  const supabaseCheck = await checkSupabaseConnections();
  supabaseCheck.forEach((entry) => addCheck(entry.status, entry.label, entry.detail, entry.hint));

  const cronCheck = await checkCronApi();
  addCheck(cronCheck.status, 'Cron API', cronCheck.detail, cronCheck.hint);

  const telegramCheck = await checkTelegramSetup(defaultProject);
  addCheck(telegramCheck.status, 'Telegram', telegramCheck.detail, telegramCheck.hint);

  const lines = ['📶 Ping test', '', ...checks.map(formatCheck)];
  const inline = new InlineKeyboard().text('🔁 Retry', 'gsettings:ping_test');
  if (defaultProject?.id) {
    inline
      .row()
      .text('📝 Fix repo', `proj:edit_repo:${defaultProject.id}`)
      .text('🔑 Fix token', `proj:edit_github_token:${defaultProject.id}`);
  }
  inline.row().text('⬅️ Back', 'gsettings:menu');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function checkGitBinary() {
  try {
    const { stdout } = await execShell('git --version');
    const detail = stdout.trim() || 'available';
    return { ok: true, detail };
  } catch (error) {
    if (error.code === 'ENOENT') {
      return { ok: false, detail: 'git not installed' };
    }
    return { ok: false, detail: error.message || 'git check failed' };
  }
}

async function execShell(command, options = {}) {
  const { exec } = require('child_process');
  const { promisify } = require('util');
  const execAsync = promisify(exec);
  return execAsync(command, options);
}

function applyGitTokenToUrl(repoUrl, token) {
  if (!token) return repoUrl;
  const url = new URL(repoUrl);
  url.username = token;
  return url.toString();
}

async function resolveGithubToken(project) {
  const key = project?.githubTokenEnvKey || 'GITHUB_TOKEN';
  const envToken = process.env[key] || process.env.GITHUB_TOKEN;
  if (envToken) {
    return { token: envToken, source: 'env', key };
  }
  if (!isEnvVaultAvailable()) {
    return { token: null, source: null, key, error: MASTER_KEY_ERROR_MESSAGE };
  }
  try {
    const envSetId = await ensureProjectEnvSet(project.id);
    const vaultToken = await getEnvVarValue(project.id, key, envSetId);
    if (vaultToken) {
      return { token: vaultToken, source: 'env vault', key };
    }
  } catch (error) {
    return { token: null, source: null, key, error: error.message };
  }
  return { token: null, source: null, key, error: 'missing' };
}

function isNodeProject(project) {
  const type = resolveProjectType(project);
  return ['node-api', 'node-bot', 'node'].includes(type);
}

async function resolvePackageJsonPath(baseDir) {
  const candidate = path.join(baseDir, 'package.json');
  try {
    await fs.stat(candidate);
    return candidate;
  } catch (error) {
    return null;
  }
}

async function suggestWorkingDirFromPackageJson(checkoutDir) {
  if (!checkoutDir) return null;
  const rootPackage = await resolvePackageJsonPath(checkoutDir);
  if (rootPackage) {
    return checkoutDir;
  }
  try {
    const entries = await fs.readdir(checkoutDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const candidateDir = path.join(checkoutDir, entry.name);
      const nestedPackage = await resolvePackageJsonPath(candidateDir);
      if (nestedPackage) {
        return candidateDir;
      }
    }
  } catch (error) {
    return null;
  }
  return null;
}

async function validateWorkingDir(project) {
  const workingDir = project?.workingDir;
  const repoSlug = project?.repoSlug;
  const checkoutDir = repoSlug ? getDefaultWorkingDir(repoSlug) : null;
  const expectedCheckoutDir = checkoutDir ? path.resolve(checkoutDir) : null;

  if (!workingDir) {
    return {
      ok: false,
      code: 'DIR_MISSING',
      details: 'workingDir missing',
      expectedCheckoutDir,
      suggestedWorkingDir: checkoutDir || null,
    };
  }

  if (!path.isAbsolute(workingDir)) {
    return {
      ok: false,
      code: 'NOT_ABSOLUTE',
      details: 'workingDir must be an absolute path',
      expectedCheckoutDir,
      suggestedWorkingDir: checkoutDir || null,
    };
  }

  const resolvedWorkingDir = path.resolve(workingDir);
  if (expectedCheckoutDir && !resolvedWorkingDir.startsWith(expectedCheckoutDir)) {
    return {
      ok: false,
      code: 'OUTSIDE_REPO',
      details: `workingDir is outside repo checkout (${expectedCheckoutDir})`,
      expectedCheckoutDir,
      suggestedWorkingDir: checkoutDir || null,
    };
  }

  try {
    await fs.stat(resolvedWorkingDir);
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return {
        ok: false,
        code: 'DIR_MISSING',
        details: 'workingDir does not exist',
        expectedCheckoutDir,
        suggestedWorkingDir: checkoutDir || null,
      };
    }
    return {
      ok: false,
      code: 'UNKNOWN',
      details: truncateText(error.message || 'unknown error', 120),
      expectedCheckoutDir,
      suggestedWorkingDir: checkoutDir || null,
    };
  }

  if (isNodeProject(project)) {
    const packagePath = await resolvePackageJsonPath(resolvedWorkingDir);
    if (!packagePath) {
      const suggestedWorkingDir = await suggestWorkingDirFromPackageJson(checkoutDir);
      return {
        ok: false,
        code: 'PACKAGE_JSON_MISSING',
        details: 'package.json not found under workingDir',
        expectedCheckoutDir,
        suggestedWorkingDir: suggestedWorkingDir || checkoutDir || null,
      };
    }
  }

  return {
    ok: true,
    code: 'OK',
    details: resolvedWorkingDir,
    expectedCheckoutDir,
    suggestedWorkingDir: checkoutDir || null,
  };
}

function formatWorkingDirValidationNotice(result) {
  if (result.ok) {
    return '✅ Working dir saved and validated.';
  }
  const lines = [`⚠️ Working dir saved but invalid (${result.code}).`];
  if (result.details) {
    lines.push(`Reason: ${result.details}`);
  }
  if (result.expectedCheckoutDir) {
    lines.push(`Expected repo root: ${result.expectedCheckoutDir}`);
  }
  if (result.suggestedWorkingDir) {
    lines.push(`Suggested workingDir: ${result.suggestedWorkingDir}`);
  }
  return lines.join('\n');
}

function formatWorkingDirValidationMessage(result) {
  if (result.ok) {
    return null;
  }
  const lines = ['Diagnostics blocked: workingDir is invalid.'];
  if (result.details) {
    lines.push(`Reason: ${result.details}`);
  }
  if (result.expectedCheckoutDir) {
    lines.push(`Expected repo root: ${result.expectedCheckoutDir}`);
  }
  if (result.suggestedWorkingDir) {
    lines.push(`Suggested workingDir: ${result.suggestedWorkingDir}`);
  }
  return lines.join('\n');
}

function formatWorkingDirHint(result) {
  if (result.ok) {
    return null;
  }
  const lines = ['Working dir invalid.'];
  if (result.details) {
    lines.push(`Reason: ${result.details}`);
  }
  if (result.expectedCheckoutDir) {
    lines.push(`Expected repo root: ${result.expectedCheckoutDir}`);
  }
  if (result.suggestedWorkingDir) {
    lines.push(`Suggested workingDir: ${result.suggestedWorkingDir}`);
  }
  return lines.join('\n');
}

function validateWorkingDirInput(rawValue) {
  if (rawValue == null) {
    return { ok: false, error: 'Please send a working directory path.' };
  }
  const value = String(rawValue);
  if (!value.trim()) {
    return { ok: false, error: 'Please send a working directory path.' };
  }
  if (value.length > 300) {
    return { ok: false, error: 'Working directory path is too long (max 300 chars).' };
  }
  if (value.includes('\u0000')) {
    return { ok: false, error: 'Working directory path contains invalid characters.' };
  }
  if (value !== value.trimEnd()) {
    return { ok: false, error: 'Working directory path cannot include trailing spaces.' };
  }
  const trimmed = value.trim();
  if (!path.isAbsolute(trimmed)) {
    return { ok: false, error: 'Working directory must be an absolute path.' };
  }
  return { ok: true, value: trimmed };
}

function classifyDiagnosticsError({ result, project, workingDir, validation }) {
  const combined = `${result?.stderr || ''}\n${result?.stdout || ''}`;
  const hasPackageJson =
    /package\.json/i.test(combined) && /(ENOENT|no such file|not found)/i.test(combined);
  if (!hasPackageJson) {
    return null;
  }

  const suggestedWorkingDir =
    validation?.suggestedWorkingDir ||
    (project?.repoSlug ? getDefaultWorkingDir(project.repoSlug) : null);
  const expectedCheckoutDir =
    validation?.expectedCheckoutDir ||
    (project?.repoSlug ? path.resolve(getDefaultWorkingDir(project.repoSlug)) : null);

  const lines = [
    'Diagnostics failed: package.json not found; workingDir likely wrong.',
    `Working dir: ${workingDir || '-'}`,
  ];
  if (expectedCheckoutDir) {
    lines.push(`Expected repo root: ${expectedCheckoutDir}`);
  }
  if (suggestedWorkingDir) {
    lines.push(`Suggested workingDir: ${suggestedWorkingDir}`);
  }
  return {
    reason: 'WORKING_DIR_INVALID',
    message: lines.join('\n'),
  };
}

async function checkGithubApi(repoInfo, token) {
  const url = `https://api.github.com/repos/${repoInfo.repoSlug}`;
  try {
    const response = await requestUrlWithHeaders('GET', url, {
      'User-Agent': 'path-applier-bot',
      Authorization: token ? `Bearer ${token}` : undefined,
    });
    if (response.status === 401) {
      return { status: 'fail', detail: '401 unauthorized', hint: 'Check the GitHub token.' };
    }
    if (response.status === 403) {
      return { status: 'fail', detail: '403 forbidden', hint: 'Token lacks access or rate-limited.' };
    }
    if (response.status === 404) {
      return { status: 'fail', detail: '404 repo not found', hint: 'Verify owner/repo.' };
    }
    if (response.status >= 400) {
      return { status: 'fail', detail: `HTTP ${response.status}`, hint: 'Check GitHub API access.' };
    }
    const latency = response.durationMs ? `~${response.durationMs} ms` : null;
    const detail = response.body?.default_branch
      ? `ok (${latency || 'fast'}) default: ${response.body.default_branch}`
      : `ok (${latency || 'fast'})`;
    return { status: 'ok', detail };
  } catch (error) {
    return { status: 'fail', detail: 'unreachable', hint: truncateText(error.message, 80) };
  }
}

async function checkWorkingDir(project, workingDir) {
  const validation = await validateWorkingDir({ ...(project || {}), workingDir });
  if (validation.ok) {
    return { status: 'ok', detail: validation.details || workingDir };
  }
  return {
    status: 'fail',
    detail: validation.details || 'invalid',
    hint: formatWorkingDirHint(validation),
  };
}

async function checkRemoteBranch(repoUrl, token, branch) {
  if (!branch) {
    return { status: 'fail', detail: 'missing', hint: 'Set base branch.' };
  }
  try {
    const remoteUrl = applyGitTokenToUrl(repoUrl, token);
    const { stdout } = await execShell(`git ls-remote --heads ${remoteUrl} ${branch}`);
    if (!stdout.trim()) {
      return { status: 'fail', detail: 'not found', hint: `Branch ${branch} missing on remote.` };
    }
    return { status: 'ok', detail: branch };
  } catch (error) {
    const reason = classifyGitError(error);
    return { status: 'fail', detail: reason.detail, hint: reason.hint };
  }
}

async function checkGitFetch(repoUrl, token, branch) {
  try {
    const remoteUrl = applyGitTokenToUrl(repoUrl, token);
    const { stdout } = await execShell(`git ls-remote --heads ${remoteUrl} ${branch}`);
    if (!stdout.trim()) {
      return { status: 'fail', detail: 'branch missing', hint: 'Check base branch name.' };
    }
    return { status: 'ok', detail: 'ok' };
  } catch (error) {
    const reason = classifyGitError(error);
    return { status: 'fail', detail: reason.detail, hint: reason.hint };
  }
}

function classifyGitError(error) {
  const message = String(error.stderr || error.stdout || error.message || '').toLowerCase();
  if (message.includes('could not read username')) {
    return { detail: 'missing credentials', hint: 'Token not applied to remote URL.' };
  }
  if (message.includes('not found') || message.includes('repository not found') || message.includes('404')) {
    return { detail: 'repo not found', hint: 'Verify owner/repo and access.' };
  }
  if (message.includes('403') || message.includes('forbidden')) {
    return { detail: 'forbidden', hint: 'Token lacks access or is rate-limited.' };
  }
  if (message.includes('enotfound') || message.includes('could not resolve host')) {
    return { detail: 'dns/network error', hint: 'Check network/DNS connectivity.' };
  }
  return { detail: truncateText(message || 'git error', 80), hint: 'Check git remote and token.' };
}

async function checkSupabaseConnections() {
  const connections = await loadSupabaseConnections();
  if (!connections.length) {
    return [{ status: 'warn', label: 'Supabase', detail: 'no connections configured' }];
  }
  const results = [];
  for (const connection of connections) {
    const dsnInfo = await resolveSupabaseConnectionDsn(connection);
    if (!dsnInfo.dsn) {
      results.push({
        status: 'fail',
        label: `Supabase (${connection.name})`,
        detail: dsnInfo.error || 'missing DSN',
        hint: `Set ${connection.envKey} in Env Vault.`,
      });
      continue;
    }
    try {
      const pool = await getSupabasePool(connection.id, dsnInfo.dsn);
      await pool.query({ text: 'SELECT 1', query_timeout: SUPABASE_QUERY_TIMEOUT_MS });
      results.push({ status: 'ok', label: `Supabase (${connection.name})`, detail: 'ok' });
    } catch (error) {
      results.push({
        status: 'fail',
        label: `Supabase (${connection.name})`,
        detail: truncateText(error.message || 'connect failed', 60),
        hint: 'Verify DSN/SSL settings.',
      });
    }
  }
  return results;
}

async function checkCronApi() {
  if (!CRON_API_TOKEN) {
    return { status: 'warn', detail: 'token missing', hint: 'Set CRON_API_TOKEN.' };
  }
  try {
    await listJobs();
    return { status: 'ok', detail: 'ok' };
  } catch (error) {
    if (error?.status === 429) {
      return { status: 'warn', detail: 'rate limited', hint: 'Try again later.' };
    }
    return {
      status: 'fail',
      detail: truncateText(error.message || 'error', 80),
      hint: 'Check CRON_API_TOKEN and endpoint.',
    };
  }
}

async function checkTelegramSetup(project) {
  if (!project) {
    return { status: 'warn', detail: 'no project selected' };
  }
  if (!isEnvVaultAvailable()) {
    return { status: 'fail', detail: 'env vault unavailable', hint: MASTER_KEY_ERROR_MESSAGE };
  }
  const record = await getProjectTelegramBot(project.id);
  if (!record?.botTokenEnc) {
    return { status: 'fail', detail: 'token missing', hint: 'Set Telegram bot token in project settings.' };
  }
  return { status: 'ok', detail: record.webhookUrl ? 'token set + webhook set' : 'token set' };
}

async function requestUrlWithHeaders(method, targetUrl, headers = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const isHttps = url.protocol === 'https:';
    const lib = isHttps ? https : http;
    const options = {
      method,
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: `${url.pathname}${url.search}`,
      headers: Object.fromEntries(Object.entries(headers).filter(([, value]) => value)),
    };
    const start = Date.now();
    const req = lib.request(options, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        let parsed = data;
        try {
          parsed = JSON.parse(data);
        } catch (error) {
          // leave as string
        }
        resolve({ status: res.statusCode, body: parsed, durationMs: Date.now() - start });
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => {
      req.destroy(new Error('Request timed out'));
    });
    req.end();
  });
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

async function initializeConfig() {
  try {
    await loadProjects();
    await loadGlobalSettings();
    await loadCronSettings();
  } catch (error) {
    console.error('Failed to load initial configuration', error);
    throw new Error('Startup aborted: failed to load initial configuration.');
  }
}

async function loadConfig() {
  await initializeConfig();
}

async function initDb() {
  await testConfigDbConnection();
}

async function initEnvVault() {
  const status = getMasterKeyStatus();
  if (status.ok) {
    console.log('Env Vault: OK');
    return true;
  }
  const reason =
    status.error === 'missing'
      ? 'ENV_VAULT_MASTER_KEY not set.'
      : MASTER_KEY_ERROR_MESSAGE;
  throw new Error(`Startup aborted: ${reason}`);
}

function startHttpServer() {
  return new Promise((resolve, reject) => {
    const server = http.createServer(async (req, res) => {
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

      if (req.method === 'POST' && url.pathname === '/ingest/logs') {
        const expectedKey = process.env.LOG_INGEST_KEY || process.env.PATH_APPLIER_LOG_INGEST_KEY;
        if (!expectedKey) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'LOG_INGEST_KEY is not configured.' }));
          return;
        }

        const providedKey = url.searchParams.get('key');
        if (!providedKey || providedKey !== expectedKey) {
          res.writeHead(403, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Invalid key.' }));
          return;
        }

        let payload;
        try {
          const rawBody = await readRequestBody(req);
          payload = rawBody ? JSON.parse(rawBody) : null;
        } catch (error) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
          return;
        }

        try {
          const result = await logIngestService.ingestLog(payload);
          if (!result.ok) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, error: result.error || 'Invalid payload.' }));
            return;
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true }));
        } catch (error) {
          console.error('[log-ingest] failed to handle request', error);
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'internal error' }));
        }
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
          const forwarding = await getProjectLogSettingsWithDefaults(projectId);

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

          const targetChatId = forwarding.destinationChatId;
          if (!targetChatId) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true, forwarded: false, reason: 'no destination' }));
            return;
          }
          const message = formatProjectLogMessage(project, event);
          await sendSafeMessage(BOT_TOKEN, targetChatId, message);
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

      res.statusCode = 200;
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      res.end('OK');
    });

    server
      .listen(PORT, () => {
        console.error(`[boot] http listening on ${PORT}`);
        resolve();
      })
      .on('error', (err) => {
        console.error('[http] server error', err);
        reject(err);
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

module.exports = {
  startBot,
  loadConfig,
  initDb,
  initEnvVault,
  respond,
  ensureAnswerCallback,
  validateWorkingDir,
  classifyDiagnosticsError,
};
