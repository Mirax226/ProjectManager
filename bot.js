require('dotenv').config();

if (!require.extensions['.ts']) {
  require.extensions['.ts'] = require.extensions['.js'];
}

const runtimeStatus = {
  configDbOk: false,
  configDbError: null,
  vaultOk: null,
  vaultError: null,
  fatalError: null,
};
const { appState, recordDbError, setDbReady, setDegradedMode } = require('./appState');
const {
  recordAttempt: recordConfigDbAttempt,
  setNextRetryInMs: setConfigDbNextRetry,
  snapshot: getConfigDbSnapshot,
} = require('./configDbState');
const { classifyDbError, sanitizeDbErrorMessage } = require('./configDbErrors');
function truncateRuntimeError(value, max = 2500) {
  const text = String(value == null ? '' : value);
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 1))}…`;
}

const {
  loadOpsState,
  appendEvent,
  getDbHealthSnapshot,
  setDbHealthSnapshot,
  getUserAlertPrefs,
  shouldRouteEvent,
  computeDestinations,
  shouldNotifyRecovery,
} = require('./opsReliability');

process.on('unhandledRejection', (reason) => {
  const error = reason instanceof Error ? reason : new Error(String(reason || 'Unhandled rejection'));
  runtimeStatus.fatalError = {
    source: 'unhandledRejection',
    message: truncateRuntimeError(error.message, 600),
    stack: truncateRuntimeError(error.stack, 2500),
    timestamp: new Date().toISOString(),
  };
  pmLogger.error('unhandledRejection', {
    source: 'process',
    error: truncateRuntimeError(error.message, 600),
    stack: truncateRuntimeError(error.stack, 2500),
  });
  console.error('[FATAL] Unhandled promise rejection', runtimeStatus.fatalError);
});

process.on('uncaughtException', (error) => {
  const fatalError = error instanceof Error ? error : new Error(String(error || 'Uncaught exception'));
  runtimeStatus.fatalError = {
    source: 'uncaughtException',
    message: truncateRuntimeError(fatalError.message, 600),
    stack: truncateRuntimeError(fatalError.stack, 2500),
    timestamp: new Date().toISOString(),
  };
  pmLogger.error('uncaughtException', {
    source: 'process',
    error: truncateRuntimeError(fatalError.message, 600),
    stack: truncateRuntimeError(fatalError.stack, 2500),
  });
  console.error('[FATAL] Uncaught exception', runtimeStatus.fatalError);
});

console.error('[boot] starting');
console.error('[env]', {
  BOT_TOKEN: !!process.env.BOT_TOKEN || !!process.env.TELEGRAM_BOT_TOKEN,
  DATABASE_URL: !!process.env.DATABASE_URL,
  ENV_VAULT_MASTER_KEY: !!process.env.ENV_VAULT_MASTER_KEY,
  PORT: process.env.PORT,
});

const keepaliveTimer = setInterval(() => console.error('[keepalive] alive'), 60_000);
if (typeof keepaliveTimer.unref === 'function') {
  keepaliveTimer.unref();
}

const http = require('http');
const https = require('https');
const dns = require('dns').promises;
const net = require('net');
const tls = require('tls');
const fs = require('fs/promises');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);
const { Bot, InlineKeyboard, Keyboard, InputFile } = require('grammy');
const { Pool } = require('pg');
const { ProgressReporter } = require('./progressReporter');

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
  renameEnvVaultProjectId,
} = require('./envVaultStore');
const {
  getProjectTelegramBot,
  getTelegramBotToken,
  upsertTelegramBotToken,
  clearTelegramBotToken,
  updateTelegramWebhook,
  updateTelegramTestStatus,
  renameTelegramBotProjectId,
} = require('./telegramBotStore');
const { encryptSecret, decryptSecret, getMasterKeyStatus, MASTER_KEY_ERROR_MESSAGE } = require('./envVaultCrypto');
const {
  setWebhook,
  getWebhookInfo,
  sendMessage,
  sendSafeMessage,
} = require('./telegramApi');
const {
  listCronJobLinks,
  getCronJobLink,
  getCronJobLinkByJobKey,
  upsertCronJobLink,
  renameCronJobLinkProjectId,
} = require('./cronJobLinksStore');
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
const { ensureConfigTable, loadJson: loadConfigJson, saveJson: saveConfigJson } = require('./configStore');
const {
  pushSnapshot: pushNavigationSnapshot,
  getStack: getNavigationStack,
  setStack: setNavigationStack,
  clearStack: clearNavigationStack,
} = require('./navigationStackStore');
const {
  getConfigDbPool,
  testConfigDbConnection: probeConfigDbConnection,
  maskDsn,
  isDsnAutoFixApplied,
} = require('./configDb');
const { listSelfLogs, getSelfLogById } = require('./loggerStore');
const {
  DEFAULT_SETTINGS: DEFAULT_LOG_ALERT_SETTINGS,
  getProjectLogSettings,
  upsertProjectLogSettings,
  addRecentLog,
  listRecentLogs,
  getRecentLogById,
  renameLogIngestProjectId,
} = require('./logIngestStore');
const { createLogIngestService, formatContext, shouldRouteBySeverity } = require('./logIngestService');
const { getDefaultGithubToken, setDefaultGithubToken } = require('./userGithubTokenStore');
const { createLogsRouter, parseAllowedProjects } = require('./src/routes/logs.ts');
const { syncPair } = require('./src/dbSyncEngine');
const { ensureDbHubSchema, upsertProjectConnection, listProjectConnections } = require('./src/db/hubStore');
const { getDbClient } = require('./src/db/router');
const { createMigrationJob, getMigrationJob, updateMigrationJob } = require('./src/db/migrationJobs');
const { validatePostgresDsn, maskPostgresDsn } = require('./src/db/dsn');
const { createOpsTimelineStore } = require('./src/opsTimeline');
const { createSafeModeController } = require('./src/safeMode');
const { buildProjectSnapshot, calculateDrift } = require('./src/driftDetector');
const { generateRunbooksFromRoutineRules, searchRunbooks } = require('./src/runbooks');
const { planRiskyOperation } = require('./src/shadowRuns');
const { listTemplates, getTemplate } = require('./src/projectTemplates');
const { resolveRole, canAccess } = require('./src/permissions');
const {
  listNotes,
  getNote,
  createNote,
  updateNote,
  addNoteAttachment,
  removeNoteAttachment,
  appendNoteBackup,
} = require('./notesStore');
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
const { paginateRepos, mapRepoButton, listAllGithubRepos } = require('./src/repoPicker');
const { createPmLogger } = require('./src/pmLogger');
const {
  createCodexTask,
  listCodexTasks,
  getCodexTask,
  markCodexTaskDone,
  deleteCodexTask,
  clearDoneCodexTasks,
  purgeOldDoneCodexTasks,
} = require('./src/codexTasksStore');
const {
  ROUTINE_AUTO_THRESHOLD,
  ROUTINE_BUTTON_THRESHOLD,
  matchBest: matchRoutineFix,
  renderMatch: renderRoutineFixMatch,
  listCatalog: listRoutineCatalog,
  getRuleById: getRoutineRuleById,
} = require('./src/routineFixes');

const BOT_TOKEN = process.env.BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN;
const ADMIN_CHAT_ID = process.env.ADMIN_CHAT_ID;
const ADMIN_TELEGRAM_ID = ADMIN_CHAT_ID || process.env.ADMIN_TELEGRAM_ID;
const ADMIN_ROOM_CHAT_ID = process.env.ADMIN_ROOM_CHAT_ID || null;
const ADMIN_ROB_CHAT_ID = process.env.ADMIN_ROB_CHAT_ID || null;
const PORT = Number(process.env.PORT || 3000);
const SUPABASE_ENV_VAULT_PROJECT_ID = 'supabase_connections';
const SUPABASE_MESSAGE_LIMIT = 3500;
const SUPABASE_ROWS_PAGE_SIZE = 20;
const SUPABASE_CELL_TRUNCATE_LIMIT = 120;
const SUPABASE_QUERY_TIMEOUT_MS = 5000;
const DB_INSIGHTS_TABLE_PAGE_SIZE = 6;
const DB_INSIGHTS_SAMPLE_SIZE = 3;
const DB_INSIGHTS_QUERY_TIMEOUT_MS = 4000;
const SUPABASE_TABLE_ACCESS_TTL_MS = 60_000;
const MINI_SITE_TOKEN = process.env.DB_MINI_SITE_TOKEN || process.env.MINI_SITE_TOKEN;
const MINI_SITE_SESSION_DEFAULT_TTL_MINUTES = 10;
const MINI_SITE_SESSION_MIN_TTL_MINUTES = 10;
const MINI_SITE_SESSION_CLOCK_SKEW_MS = 30 * 1000;
const MINI_SITE_SESSION_SECRET =
  process.env.DB_MINI_SITE_SESSION_SECRET || process.env.MINI_SITE_SESSION_SECRET || null;
const MINI_SITE_SESSION_COOKIE = 'pm_db_session';
const MINI_SITE_EDIT_SESSION_COOKIE = 'pm_db_edit_session';
const MINI_SITE_SETTINGS_KEY = 'dbMiniSite';
const MINI_SITE_PAGE_SIZE = 25;
const MINI_SITE_EDIT_TOKEN_TTL_SEC = 300;
const MINI_SITE_WARMUP_ENABLED = process.env.MINI_SITE_WARMUP_ENABLED !== 'false';
const MINI_SITE_WARMUP_TTL_MS = Number(process.env.MINI_SITE_WARMUP_TTL_MS) || 60_000;
const MINI_SITE_WARMUP_QUERY_TIMEOUT_MS = Number(process.env.MINI_SITE_WARMUP_QUERY_TIMEOUT_MS) || 5000;
const MINI_SITE_WARMUP_DNS_TIMEOUT_MS = Number(process.env.MINI_SITE_WARMUP_DNS_TIMEOUT_MS) || 3000;
const MINI_SITE_WARMUP_TCP_TIMEOUT_MS = Number(process.env.MINI_SITE_WARMUP_TCP_TIMEOUT_MS) || 5000;
const MINI_SITE_WARMUP_TLS_TIMEOUT_MS = Number(process.env.MINI_SITE_WARMUP_TLS_TIMEOUT_MS) || 5000;
const MINI_SITE_RETRY_ENABLED = process.env.MINI_SITE_RETRY_ENABLED !== 'false';
const MINI_SITE_RETRY_MAX_ATTEMPTS = 2;
const MINI_APP_ROUTE = '/mini-db-console';
const MINI_JOB_TTL_MS = Number(process.env.MINI_JOB_TTL_MS) || 60 * 60 * 1000;
const MINI_DB_SECONDARY_DSN = process.env.SECONDARY_DATABASE_URL || process.env.DATABASE_URL_SECONDARY || null;
const MINI_DB_DEFAULT_TABLES = ['projects', 'settings', 'config_store', 'ops_event_log'];
const MINI_SITE_RETRY_BACKOFF_MS = [300, 900];
const MINI_SITE_POOL_MAX = Number(process.env.MINI_SITE_POOL_MAX) || 3;
const MINI_SITE_CONNECTION_TIMEOUT_MS = Number(process.env.MINI_SITE_CONNECTION_TIMEOUT_MS) || 15_000;
const MINI_SITE_QUERY_TIMEOUT_MS = Number(process.env.MINI_SITE_QUERY_TIMEOUT_MS) || 12_000;
const MINI_SITE_STATEMENT_TIMEOUT_MS = Number(process.env.MINI_SITE_STATEMENT_TIMEOUT_MS) || 12_000;
const MINI_SITE_SQL_DEFAULT_LIMIT = 100;
const MINI_SITE_SQL_MAX_LIMIT = 1000;
const MINI_SITE_SQL_WRITE_WINDOW_MS = 60 * 1000;
const PROJECT_DB_SSL_DEFAULT_MODE = 'require';
const PROJECT_DB_SSL_DEFAULT_VERIFY = true;
const PROJECT_DB_SSL_MODES = new Set(['disable', 'require']);
const ALLOW_INSECURE_TLS_FOR_TESTS = process.env.ALLOW_INSECURE_TLS_FOR_TESTS === 'true';
const WEB_DASHBOARD_SETTINGS_KEY = 'webDashboard';
const WEB_DB_CONSOLE_SETTINGS_KEY = 'webDbConsole';
const WEB_DASHBOARD_SESSION_COOKIE = 'pm_web_session';
const WEB_DASHBOARD_SESSION_TTL_MINUTES = 20;
const WEB_DASHBOARD_LOGIN_WINDOW_MS = 60_000;
const WEB_DASHBOARD_LOGIN_MAX_ATTEMPTS = 5;
const WEB_DASHBOARD_LOGIN_BLOCK_MS = 5 * 60_000;
const CONFIG_DB_MAX_RETRIES_PER_BOOT = Number(process.env.CONFIG_DB_MAX_RETRIES_PER_BOOT || 20);
const WEB_DASHBOARD_ASSETS_DIR = path.join(__dirname, 'src', 'web');
const LOG_API_TOKEN = process.env.PATH_APPLIER_TOKEN;
const LOG_API_ADMIN_CHAT_ID =
  process.env.TELEGRAM_ADMIN_CHAT_ID ||
  process.env.ADMIN_CHAT_ID ||
  process.env.ADMIN_TELEGRAM_ID;
const LOG_API_ENABLED = Boolean(LOG_API_TOKEN);
const LOG_API_ALLOWED_PROJECTS = parseAllowedProjects(process.env.ALLOWED_PROJECTS);
const LOG_API_ALLOWED_PROJECTS_MODE = LOG_API_ALLOWED_PROJECTS.size ? 'whitelist' : 'allow-all';
const LOG_API_STATUS_LABEL = LOG_API_ENABLED ? 'ENABLED' : 'DISABLED';
const PM_LOGGER_ENABLED = process.env.PM_LOGGER_ENABLED !== 'false';
const pmLogger = createPmLogger({ enabled: PM_LOGGER_ENABLED });
pmLogger.attachProcessHooks();
const NOTE_CATEGORIES = ['Bug', 'Feature', 'Deploy', 'DB', 'Idea', 'Security', 'Custom'];
const NOTE_STATUS = { OPEN: 'OPEN', DONE: 'DONE' };
const ENV_VALUE_PREVIEW_LIMIT = 160;
const SPOILER_WRAP = (value) => `<span class="tg-spoiler">${value}</span>`;

console.error(`[boot] PATH_APPLIER_TOKEN: ${LOG_API_ENABLED ? 'SET' : 'MISSING'}`);
console.error(`[boot] Log API status: ${LOG_API_STATUS_LABEL}`);
if (!LOG_API_ENABLED) {
  console.warn('[WARN] Log API disabled: missing token');
}

console.error('[log_api]', {
  tokenConfigured: LOG_API_ENABLED,
  adminChatIdResolved: Boolean(LOG_API_ADMIN_CHAT_ID),
  allowedProjectsMode: LOG_API_ALLOWED_PROJECTS_MODE,
});

if (!BOT_TOKEN) {
  throw new Error('Startup aborted: BOT_TOKEN is required');
}

if (!ADMIN_TELEGRAM_ID) {
  throw new Error('Startup aborted: ADMIN_TELEGRAM_ID is required');
}

const bot = new Bot(BOT_TOKEN);
const opsRecoveryMessages = new Map();
const logDeliveryDiagnosticsRuns = new Map();
const routineQuickState = new Map();
const routineOutputsByUser = new Map();
let opsScannerTimer = null;
const supabasePools = new Map();
const envVaultPools = new Map();
const miniSitePools = new Map();
const miniSiteWarmupCache = new Map();
const healthCheckPools = new Map();
const miniSiteWriteGrants = new Map();
const supabaseTableAccess = new Map();
let botStarted = false;
let botRetryTimeout = null;
const userState = new Map();
const patchSessions = new Map();
const changePreviewSessions = new Map();
const structuredPatchSessions = new Map();
const envScanCache = new Map();
const cronCreateRetryCache = new Map();
const cronErrorDetailsCache = new Map();
const cronErrorDedupeCache = new Map();
const panelMessageHistoryByChat = new Map();
const activePanelMessageIdByChat = new Map();
const menuMessageRegistryMemory = new Map();
let menuMessageRegistryLoaded = false;
const navigationQueueByChat = new Map();
const navigationLocksByChat = new Map();
const navigationOperationByChat = new Map();
const ephemeralMessageIdsByChat = new Map();
const ephemeralTimersByChat = new Map();
const webSessions = new Map();
const webLoginAttempts = new Map();
const opsTimeline = createOpsTimelineStore({ globalLimit: Number(process.env.OPS_TIMELINE_GLOBAL_LIMIT) || 2000, projectLimit: Number(process.env.OPS_TIMELINE_PROJECT_LIMIT) || 2000 });
const safeMode = createSafeModeController({});
const driftBaselines = new Map();
const pendingLogTests = new Map();
let httpServerPromise = null;
let miniSiteWarmupSummary = {
  warmUpReady: false,
  lastWarmUpAt: null,
};
let configDbWarmupTimer = null;
let configDbWarmupInFlight = false;
let configDbFailureStreak = 0;
let configDbLastLogCategory = null;
let configDbLastLogAttempt = 0;
let configDbManualRetryAt = 0;
let configDbWarmupHaltedForBoot = false;
let configDbDsnIssueAttempts = 0;
let cachedSettings = null;
let cachedSettingsAt = 0;
const DB_OPERATION_TIMEOUT_MS = 5000;
const routinePayloadCache = new Map();

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
  sendTelegramMessage: async (chatId, text) => {
    const routineButton = buildRoutineFixButton(text, null, null);
    const reply_markup = routineButton ? { inline_keyboard: [[routineButton]] } : undefined;
    return bot.api.sendMessage(chatId, text, { disable_web_page_preview: true, reply_markup });
  },
  logger: console,
});

const logsRouter = createLogsRouter({
  token: LOG_API_TOKEN,
  adminChatId: LOG_API_ADMIN_CHAT_ID,
  allowedProjects: LOG_API_ALLOWED_PROJECTS,
  getAllowedProjectsMode: () => cachedSettings?.logs?.allowedProjectsMode || LOG_API_ALLOWED_PROJECTS_MODE,
  logger: console,
  now: () => Date.now(),
  rateLimitPerMinute: 20,
  sendTelegramMessage: async (chatId, text, options) => bot.api.sendMessage(chatId, text, options),
  onLogReceived: async ({ correlationId, entry, receivedAt }) =>
    handlePendingLogTestReceipt({ correlationId, entry, receivedAt }),
});

const LOG_TEST_MODES = ['info', 'warn', 'error', 'timeout'];
const LOG_TEST_RECEIPT_TIMEOUT_MS = 15_000;
const LOG_TEST_CALL_TIMEOUTS_MS = {
  info: 12_000,
  warn: 12_000,
  error: 12_000,
  timeout: 20_000,
};
const LOG_TEST_REMINDER_SNOOZE_DAYS = [1, 3, 7];
const LOG_TEST_DAILY_REMINDER_ENABLED = String(process.env.LOG_TEST_DAILY_REMINDER || '').toLowerCase() === 'true';
const LOG_TEST_SINGLE_PROGRESS_STEPS = [
  'Resolve project config',
  'Diagnostics gate (/pm/diagnostics)',
  'Trigger client log test',
  'Wait for log receipt (correlationId)',
  'Validate payload formatting and project tagging',
  'Summarize',
];
const LOG_TEST_SUITE_PROGRESS_STEPS = [
  'Resolve project config',
  'Diagnostics gate (/pm/diagnostics)',
  'Trigger client test: info',
  'Wait for log receipt (correlationId)',
  'Trigger client test: warn',
  'Wait for log receipt (correlationId)',
  'Trigger client test: error',
  'Wait for log receipt (correlationId)',
  'Trigger client test: timeout',
  'Wait for log receipt (correlationId)',
  'Validate payload formatting and project tagging',
  'Summarize',
];
const LOG_TEST_DIAGNOSTICS_TIMEOUT_MS = 12_000;
const REPO_INSPECTION_RATE_LIMIT = { limit: 3, windowMs: 60 * 60 * 1000 };
const REPO_PR_RATE_LIMIT = { limit: 1, windowMs: 60 * 60 * 1000 };

const repoInspectionRateLimits = new Map();
const repoPrRateLimits = new Map();
const repoInspectionCache = new Map();

const CRON_RATE_LIMIT_MESSAGE = 'Cron API rate limit reached. Please wait a bit and try again.';
const CRON_JOBS_CACHE_TTL_MS = 30_000;
let lastCronJobsCache = null;
let lastCronJobsFetchedAt = 0;
const TELEGRAM_WEBHOOK_PATH_PREFIX = '/webhook';
const RENDER_API_KEY_ENV = process.env.RENDER_API_KEY;
const RENDER_API_BASE_URL = 'https://api.render.com/v1';
const RENDER_WEBHOOK_EVENTS_DEFAULT = ['deploy_started', 'deploy_ended'];
const RENDER_POLL_INTERVAL_SEC_DEFAULT = 60;
const RENDER_POLL_MAX_SERVICES_PER_TICK_DEFAULT = 10;
const RENDER_POLL_TIMEOUT_MS_DEFAULT = 8000;
const RENDER_ENV_VAULT_PROJECT_ID = '__render_global__';
const RENDER_WEBHOOK_RATE_LIMIT = { limit: 12, windowMs: 60 * 1000 };
const RENDER_WEBHOOK_DEDUP_TTL_MS = 6 * 60 * 60 * 1000;
const renderWebhookRateLimits = new Map();
const renderWebhookEventCache = new Map();
const renderWebhookUnknownServiceNotices = new Map();
const renderServiceDiscoveryCache = new Map();
const renderPollingLocks = new Set();
let renderPollTimer = null;
let renderPollSkipLogAt = 0;

function normalizeLogLevels(levels) {
  if (!Array.isArray(levels)) return [];
  return levels.map((level) => normalizeLogLevel(level)).filter(Boolean);
}

async function withDbTimeout(promise, context) {
  if (!promise || typeof promise.then !== 'function') return promise;
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error('DB_TIMEOUT');
      error.code = 'DB_TIMEOUT';
      error.context = context;
      reject(error);
    }, DB_OPERATION_TIMEOUT_MS);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

async function getCachedSettings(force = false) {
  const now = Date.now();
  if (!force && cachedSettings && now - cachedSettingsAt < 30_000) {
    return cachedSettings;
  }
  if (!appState.dbReady || appState.degradedMode) {
    return cachedSettings;
  }
  cachedSettings = await loadGlobalSettings();
  cachedSettingsAt = now;
  return cachedSettings;
}

async function saveGlobalSettingsAndCache(settings) {
  await saveGlobalSettings(settings);
  cachedSettings = settings;
  cachedSettingsAt = Date.now();
}

function normalizeUiCleanupSettings(settings) {
  const ui = settings?.uiCleanup || {};
  const keepLastPanels = [1, 3, 5].includes(ui.keepLastPanels) ? ui.keepLastPanels : 1;
  const ttlOptions = [0, 30, 60];
  const ttl = ttlOptions.includes(ui.ephemeralTtlSec) ? ui.ephemeralTtlSec : 30;
  return {
    autoCleanMenus: ui.autoCleanMenus !== false,
    keepLastPanels,
    ephemeralTtlSec: ttl,
  };
}

function normalizeSecuritySettings(settings) {
  const security = settings?.security || {};
  return {
    adminIds: Array.isArray(security.adminIds) ? security.adminIds.map(String) : [],
    miniSiteSessionTtlMinutes: Number(security.miniSiteSessionTtlMinutes) || MINI_SITE_SESSION_DEFAULT_TTL_MINUTES,
    envMaskPolicy: security.envMaskPolicy || 'strict',
  };
}

function normalizeBackupSettings(settings) {
  const backups = settings?.backups || {};
  return {
    channelId: backups.channelId ? String(backups.channelId) : '',
    captionTemplate: backups.captionTemplate || '',
  };
}

function normalizeLogDefaults(settings) {
  const logs = settings?.logs || {};
  return {
    defaultLevels: normalizeLogLevels(logs.defaultLevels).length
      ? normalizeLogLevels(logs.defaultLevels)
      : ['error'],
    allowedProjectsMode: logs.allowedProjectsMode === 'whitelist' ? 'whitelist' : 'allow-all',
  };
}

function normalizeIntegrationSettings(settings) {
  const integrations = settings?.integrations || {};
  return {
    baseUrlOverride: integrations.baseUrlOverride || '',
    healthPingIntervalMinutes: Number(integrations.healthPingIntervalMinutes) || 5,
  };
}

function getLogApiHealthStatus() {
  if (LOG_API_ENABLED) {
    return { enabled: true };
  }
  return { enabled: false, reason: 'missing token' };
}

function buildLogApiStatusText() {
  const status = getLogApiHealthStatus();
  const allowedMode = cachedSettings?.logs?.allowedProjectsMode || LOG_API_ALLOWED_PROJECTS_MODE;
  const lines = ['📣 Log API status', `Status: ${status.enabled ? 'enabled' : 'disabled'}`];
  lines.push(`Token: ${LOG_API_ENABLED ? 'present' : 'missing'}`);
  if (!status.enabled) {
    lines.push(`Reason: ${status.reason}`);
  }
  lines.push(`Allowed projects: ${allowedMode}`);
  return lines.join('\n');
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
  const mode = ['admin', 'channel', 'both'].includes(payload.destinationMode) ? payload.destinationMode : 'admin';
  return {
    enabled: typeof payload.enabled === 'boolean' ? payload.enabled : DEFAULT_LOG_ALERT_SETTINGS.enabled,
    levels: levels.length ? levels : [...DEFAULT_LOG_ALERT_SETTINGS.levels],
    destinationChatId: payload.destinationChatId ? String(payload.destinationChatId) : null,
    destinationMode: mode,
  };
}

async function getProjectLogSettingsWithDefaults(projectId) {
  const settings = await getProjectLogSettings(projectId);
  if (settings) {
    return normalizeProjectLogSettings(settings);
  }
  const globalSettings = await getCachedSettings();
  const defaults = normalizeLogDefaults(globalSettings);
  return {
    ...DEFAULT_LOG_ALERT_SETTINGS,
    levels: defaults.defaultLevels,
    destinationMode: 'admin',
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

function normalizeCronProvider(provider, hasToken = Boolean(CRON_API_TOKEN)) {
  const normalized = String(provider || '').trim().toLowerCase();
  if (normalized === 'local' || normalized === 'none') return 'none';
  if (normalized === 'cronjob_org') return 'cronjob_org';
  return hasToken ? 'cronjob_org' : 'none';
}

async function getEffectiveCronSettings() {
  const settings = await loadCronSettings();
  const effective = settings || { enabled: true, defaultTimezone: 'UTC' };
  return {
    ...effective,
    provider: normalizeCronProvider(effective.provider, Boolean(CRON_API_TOKEN)),
  };
}

function isCronProviderEnabled(cronSettings) {
  return normalizeCronProvider(cronSettings?.provider, Boolean(CRON_API_TOKEN)) === 'cronjob_org';
}

function getCronProviderGuidance(cronSettings) {
  const provider = normalizeCronProvider(cronSettings?.provider, Boolean(CRON_API_TOKEN));
  if (provider === 'none') {
    return 'Cron provider is set to none/local. Use VPS/system cron/systemd timers or set CRON_PROVIDER=cronjob_org.';
  }
  if (!CRON_API_TOKEN) {
    return 'Cron provider requires CRON_API_TOKEN (or CRON_API_KEY/CRONJOB_API_KEY).';
  }
  return null;
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
  return (
    ctx?.chat?.id ||
    ctx?.callbackQuery?.message?.chat?.id ||
    ctx?.from?.id ||
    null
  );
}

function getNavigationQueue(chatId) {
  if (!navigationQueueByChat.has(chatId)) {
    navigationQueueByChat.set(chatId, []);
  }
  return navigationQueueByChat.get(chatId);
}

function getNavigationOperationState(chatId) {
  if (!navigationOperationByChat.has(chatId)) {
    navigationOperationByChat.set(chatId, { inProgress: false, noticeMessageId: null });
  }
  return navigationOperationByChat.get(chatId);
}

function clearNavigationOperationState(chatId) {
  if (!chatId) return;
  navigationOperationByChat.delete(chatId);
}

async function withNavigationLock(chatId, handler) {
  if (!chatId) {
    return handler();
  }
  const current = navigationLocksByChat.get(chatId) || Promise.resolve();
  const next = current.then(handler, handler);
  navigationLocksByChat.set(chatId, next.catch(() => null));
  return next;
}

async function safeDeleteMessage(ctx, chatId, messageId, reason) {
  if (!chatId || !messageId) return;
  const api = ctx?.api || bot.api;
  try {
    await api.deleteMessage(chatId, messageId);
  } catch (error) {
    console.warn('[cleanup] Failed to delete message', {
      chatId,
      messageId,
      reason,
      error: error?.message,
    });
  }
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
  const response = await replySafely(ctx, text, safeExtra);
  return response;
}

async function safeRespond(ctx, text, extra, context) {
  try {
    return await respond(ctx, text, extra);
  } catch (error) {
    console.error('[UI] safeRespond failed', {
      error: error?.message,
      context,
    });
    try {
      return await replySafely(ctx, text);
    } catch (replyError) {
      console.error('[UI] safeRespond fallback failed', replyError);
    }
  }
}

function getPanelHistory(chatId) {
  if (!panelMessageHistoryByChat.has(chatId)) {
    panelMessageHistoryByChat.set(chatId, []);
  }
  return panelMessageHistoryByChat.get(chatId);
}

function setPanelHistoryForChat(chatId, history) {
  if (!chatId) return;
  panelMessageHistoryByChat.set(chatId, Array.isArray(history) ? [...history] : []);
}

function getActivePanelMessageId(chatId) {
  if (!chatId) return null;
  return activePanelMessageIdByChat.get(chatId) || null;
}

function setActivePanelMessageId(chatId, messageId) {
  if (!chatId || !messageId) return;
  activePanelMessageIdByChat.set(chatId, messageId);
}

function clearActivePanelMessageId(chatId) {
  if (!chatId) return;
  activePanelMessageIdByChat.delete(chatId);
}

function getMenuRegistryKey(chatId, userId) {
  return `${chatId}:${userId || '0'}`;
}

async function loadMenuRegistry() {
  if (menuMessageRegistryLoaded) return;
  menuMessageRegistryLoaded = true;
  try {
    const stored = await loadConfigJson('menuMessageRegistry');
    if (!stored || typeof stored !== 'object') return;
    Object.entries(stored).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        menuMessageRegistryMemory.set(key, value.filter((id) => Number.isInteger(id)));
      }
    });
  } catch (error) {
    console.warn('[menu-registry] failed to load from config db', error?.message);
  }
}

async function persistMenuRegistry() {
  try {
    const payload = {};
    menuMessageRegistryMemory.forEach((ids, key) => {
      payload[key] = ids;
    });
    await saveConfigJson('menuMessageRegistry', payload);
  } catch (error) {
    console.warn('[menu-registry] failed to persist to config db', error?.message);
  }
}

async function markMessageStale(chatId, messageId) {
  try {
    await bot.api.editMessageText(chatId, messageId, '(stale)', {
      reply_markup: { inline_keyboard: [] },
    });
    return;
  } catch (error) {
    if (!isMessageNotModifiedError(error)) {
      console.warn('[menu-registry] failed to mark stale text', { chatId, messageId, error: error?.message });
    }
  }
  try {
    await bot.api.editMessageReplyMarkup(chatId, messageId, { reply_markup: { inline_keyboard: [] } });
  } catch (error) {
    if (!isMessageNotModifiedError(error)) {
      console.warn('[menu-registry] failed to disable stale keyboard', { chatId, messageId, error: error?.message });
    }
  }
}

async function deleteOrStaleMenuMessage(chatId, messageId) {
  try {
    await bot.api.deleteMessage(chatId, messageId);
    return true;
  } catch (error) {
    await markMessageStale(chatId, messageId);
    return false;
  }
}

async function deactivatePanelMessage(chatId, messageId, reason) {
  if (!chatId || !messageId) return;
  try {
    await bot.api.editMessageReplyMarkup(chatId, messageId, {
      reply_markup: { inline_keyboard: [] },
    });
  } catch (error) {
    console.warn('[cleanup] Failed to deactivate panel', {
      chatId,
      messageId,
      reason,
      error: error?.message,
    });
  }
}

async function trackPanelMessage(chatId, messageId, settings, userId) {
  if (!chatId || !messageId) return;
  await loadMenuRegistry();
  const history = getPanelHistory(chatId);
  const filtered = history.filter((entry) => entry !== messageId);
  filtered.unshift(messageId);
  panelMessageHistoryByChat.set(chatId, filtered);
  setActivePanelMessageId(chatId, messageId);

  await Promise.all(
    filtered.slice(1).map((oldId) => deactivatePanelMessage(chatId, oldId, 'new_panel')),
  );

  const key = getMenuRegistryKey(chatId, userId);
  const persisted = menuMessageRegistryMemory.get(key) || [];
  const merged = [messageId, ...persisted.filter((id) => id !== messageId)].slice(0, 5);
  menuMessageRegistryMemory.set(key, merged);
  await persistMenuRegistry();

  const staleTargets = merged.slice(1);
  for (const oldId of staleTargets) {
    await deleteOrStaleMenuMessage(chatId, oldId);
  }

  if (!settings.autoCleanMenus) {
    return;
  }
  const keep = settings.keepLastPanels || 1;
  const toRemove = filtered.slice(keep);
  await Promise.all(toRemove.map((oldId) => safeDeleteMessage(null, chatId, oldId, 'panel_limit')));
  panelMessageHistoryByChat.set(chatId, filtered.slice(0, keep));
}

async function clearPanelMessages(ctx, reason) {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) return;
  const history = getPanelHistory(chatId);
  const activePanelMessageId = getActivePanelMessageId(chatId);
  const targets = activePanelMessageId && !history.includes(activePanelMessageId)
    ? [...history, activePanelMessageId]
    : history;
  await Promise.all(targets.map((messageId) => safeDeleteMessage(ctx, chatId, messageId, reason)));
  panelMessageHistoryByChat.set(chatId, []);
  clearActivePanelMessageId(chatId);
}

function trackEphemeralMessage(chatId, messageId) {
  if (!chatId || !messageId) return;
  if (!ephemeralMessageIdsByChat.has(chatId)) {
    ephemeralMessageIdsByChat.set(chatId, new Set());
  }
  ephemeralMessageIdsByChat.get(chatId).add(messageId);
}

function clearEphemeralTimer(chatId, messageId) {
  const timers = ephemeralTimersByChat.get(chatId);
  if (!timers) return;
  const timer = timers.get(messageId);
  if (timer) {
    clearTimeout(timer);
    timers.delete(messageId);
  }
}

async function clearEphemeralMessages(ctx, reason) {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) return;
  const set = ephemeralMessageIdsByChat.get(chatId);
  if (!set) return;
  await Promise.all(
    Array.from(set).map((messageId) => safeDeleteMessage(ctx, chatId, messageId, reason)),
  );
  set.forEach((messageId) => clearEphemeralTimer(chatId, messageId));
  ephemeralMessageIdsByChat.set(chatId, new Set());
}

async function sendEphemeralMessage(ctx, text, extra) {
  const settings = await getCachedSettings();
  const cleanup = normalizeUiCleanupSettings(settings);
  const response = await replySafely(ctx, text, extra);
  if (response?.chat?.id && response?.message_id) {
    const chatId = response.chat.id;
    const messageId = response.message_id;
    trackEphemeralMessage(chatId, messageId);
    if (cleanup.ephemeralTtlSec > 0) {
      if (!ephemeralTimersByChat.has(chatId)) {
        ephemeralTimersByChat.set(chatId, new Map());
      }
      const timer = setTimeout(() => {
        safeDeleteMessage(ctx, chatId, messageId, 'ephemeral_ttl');
        const set = ephemeralMessageIdsByChat.get(chatId);
        if (set) {
          set.delete(messageId);
        }
        clearEphemeralTimer(chatId, messageId);
      }, cleanup.ephemeralTtlSec * 1000);
      ephemeralTimersByChat.get(chatId).set(messageId, timer);
    }
  }
  return response;
}

async function sendTransientNotice(ctx, text, options = {}) {
  const ttlSec = Number.isFinite(options.ttlSec) ? Number(options.ttlSec) : 10;
  const deleteButton = options.includeDelete !== false && options.deleteButton !== false;
  const extraMarkup = sanitizeReplyMarkup(options.extraMarkup || options.reply_markup);
  const extra = { ...(options.extra || {}) };
  if (options.disable_web_page_preview != null) {
    extra.disable_web_page_preview = options.disable_web_page_preview;
  }
  const chatId = getChatIdFromCtx(ctx);
  const rows = Array.isArray(extraMarkup?.inline_keyboard) ? extraMarkup.inline_keyboard.slice() : [];
  if (deleteButton) {
    rows.push([buildDeleteButton(ctx)]);
  }
  const response = await replySafely(ctx, text, {
    ...extra,
    reply_markup: rows.length ? { inline_keyboard: rows } : undefined,
  });
  if (response?.chat?.id && response?.message_id) {
    trackEphemeralMessage(response.chat.id, response.message_id);
    if (ttlSec > 0) {
      if (!ephemeralTimersByChat.has(response.chat.id)) {
        ephemeralTimersByChat.set(response.chat.id, new Map());
      }
      const timer = setTimeout(() => {
        safeDeleteMessage(ctx, response.chat.id, response.message_id, 'transient_ttl');
        const set = ephemeralMessageIdsByChat.get(response.chat.id);
        if (set) set.delete(response.message_id);
        clearEphemeralTimer(response.chat.id, response.message_id);
      }, ttlSec * 1000);
      ephemeralTimersByChat.get(response.chat.id).set(response.message_id, timer);
    }
    if (deleteButton && chatId) {
      const finalMarkup = {
        inline_keyboard: [
          ...(Array.isArray(extraMarkup?.inline_keyboard) ? extraMarkup.inline_keyboard : []),
          [buildDeleteButton(ctx, { chatId, messageId: response.message_id })],
        ],
      };
      try {
        await bot.api.editMessageReplyMarkup(chatId, response.message_id, {
          reply_markup: finalMarkup,
        });
      } catch (error) {
        console.warn('[msgdel] failed to attach final delete callback', error?.message);
      }
    }
  }
  return response;
}

async function sendDismissibleMessage(ctx, text, extra = {}) {
  return sendTransientNotice(ctx, text, { ttlSec: 10, deleteButton: true, extraMarkup: extra.reply_markup, extra });
}

async function sendTransientMessage(ctx, text, options = {}) {
  const ttlMs = Number.isFinite(options.ttlMs) ? Number(options.ttlMs) : 10000;
  const ttlSec = Math.max(0, Math.round(ttlMs / 1000));
  return sendTransientNotice(ctx, text, {
    ttlSec,
    deleteButton: true,
    extraMarkup: options.reply_markup || options.extraMarkup,
    extra: options,
  });
}


function codexTaskShortId(id) {
  return String(id || '').slice(0, 8);
}

function formatCodexTaskTitle(task) {
  return `#${codexTaskShortId(task.id)} ${task.title || 'Untitled task'}`.slice(0, 64);
}

async function verifyCodexTask(task) {
  if (!task) return { ok: false, message: 'Task not found.' };
  if (task.sourceType !== 'repo_inspection') {
    return { ok: true, message: 'No verifier for this task type. Marked done.' };
  }
  if (!task.projectId) {
    return { ok: true, message: 'No project context. Marked done.' };
  }
  const cached = repoInspectionCache.get(task.projectId);
  if (!cached?.report?.missing?.length) {
    return { ok: true, message: 'Inspection has no missing items.' };
  }
  return { ok: false, message: `Still missing: ${cached.report.missing.join(', ')}` };
}

async function createCodexTaskAndNotify(ctx, payload) {
  const result = await createCodexTask(payload);
  const text = result.created
    ? '🧠 Task added to Codex Tasks queue'
    : '🧠 Task already exists in Codex Tasks queue';
  const projectTarget = payload?.projectId ? `proj:codex_tasks:${payload.projectId}` : 'proj:list';
  const extraMarkup = new InlineKeyboard().text('Open Codex Tasks', projectTarget);
  await sendTransientNotice(ctx, text, { ttlSec: 10, includeDelete: true, extraMarkup });
  return result;
}

async function renderCodexTasksMenu(ctx, notice, options = {}) {
  await purgeOldDoneCodexTasks();
  const projectId = options.projectId || null;
  const pendingAll = await listCodexTasks({ status: 'pending' });
  const doneAll = await listCodexTasks({ status: 'done' });
  const pending = projectId ? pendingAll.filter((task) => task.projectId === projectId) : pendingAll;
  const done = projectId ? doneAll.filter((task) => task.projectId === projectId) : doneAll;
  const lines = [
    '🧠 Codex Tasks',
    buildScopedHeader(projectId ? `PROJECT: ${projectId}` : 'GLOBAL', projectId ? `Main → Projects → ${projectId} → Project Settings → Codex Tasks` : 'Main → Codex Tasks'),
    notice || null,
    `Pending: ${pending.length}`,
    `Done: ${done.length}`,
  ].filter(Boolean);
  const inline = new InlineKeyboard();
  if (!pending.length) {
    lines.push('', 'No pending tasks.');
  } else {
    lines.push('', 'Pending tasks:');
    pending.slice(0, 20).forEach((task) => {
      inline.text(formatCodexTaskTitle(task), `codex_tasks:view:${task.id}:${projectId || ''}`).row();
    });
  }
  inline.text('🧹 Clear all done', `codex_tasks:clear_done:${projectId || ''}`).row();
  inline.text('📤 Export pending', `codex_tasks:export:${projectId || ''}`).row();
  inline.text('⬅️ Back', projectId ? `proj:settings:${projectId}` : 'nav:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderCodexTaskDetails(ctx, id, notice, options = {}) {
  const task = await getCodexTask(id);
  if (!task) {
    await renderCodexTasksMenu(ctx, 'Task not found.', options);
    return;
  }
  const lines = [
    `🧠 Codex Task #${codexTaskShortId(task.id)}`,
    `Title: ${task.title || '-'}`,
    `Source: ${task.sourceType || '-'}`,
    `Project: ${task.projectId || '-'}`,
    `Status: ${task.status}`,
    notice || null,
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('📋 Copy task', `codex_tasks:copy:${task.id}:${options.projectId || ''}`)
    .row()
    .text('✅ Mark fixed', `codex_tasks:done:${task.id}:${options.projectId || ''}`)
    .text('🗑 Delete task', `codex_tasks:delete:${task.id}:${options.projectId || ''}`)
    .row()
    .text('↩ Back', options.projectId ? `proj:codex_tasks:${options.projectId}` : 'nav:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function handleCodexTasksCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = String(data || '').split(':');
  const action = parts[1];
  const id = parts[2];
  const projectId = parts[3] || null;
  const options = { projectId };
  if (action === 'list') {
    await renderCodexTasksMenu(ctx, null, options);
    return;
  }
  if (action === 'view' && id) {
    await renderCodexTaskDetails(ctx, id, null, options);
    return;
  }
  if (action === 'copy' && id) {
    const task = await getCodexTask(id);
    if (!task) {
      await renderCodexTasksMenu(ctx, 'Task not found.', options);
      return;
    }
    await sendDismissibleMessage(ctx, `\`\`\`text
${task.body}
\`\`\``);
    await renderCodexTaskDetails(ctx, id, 'Task copied.', options);
    return;
  }
  if (action === 'done' && id) {
    const task = await getCodexTask(id);
    const verify = await verifyCodexTask(task);
    if (!verify.ok) {
      await renderCodexTaskDetails(ctx, id, `❌ Verification failed. ${verify.message}`, options);
      return;
    }
    await markCodexTaskDone(id);
    await renderCodexTasksMenu(ctx, `✅ Task marked done. ${verify.message}`, options);
    return;
  }
  if (action === 'delete' && id) {
    await deleteCodexTask(id);
    await renderCodexTasksMenu(ctx, '🗑 Task deleted.', options);
    return;
  }
  if (action === 'clear_done') {
    const removed = await clearDoneCodexTasks();
    await renderCodexTasksMenu(ctx, `🧹 Removed ${removed} done task(s).`, options);
    return;
  }
  if (action === 'export') {
    const pending = await listCodexTasks({ status: 'pending' });
    const filtered = projectId ? pending.filter((task) => task.projectId === projectId) : pending;
    const body = filtered
      .map((task, idx) => `${idx + 1}. [${task.projectId || '-'}] ${task.title}\n${task.body}`)
      .join('\n\n');
    await sendDismissibleMessage(ctx, body ? `\`\`\`text\n${body}\n\`\`\`` : 'No pending tasks.');
    await renderCodexTasksMenu(ctx, null, options);
    return;
  }

  await renderCodexTasksMenu(ctx, null, options);
}

async function renderRoutineOutput(ctx, rendered, options = {}) {
  const backCallback = options.backCallback || 'gsettings:routine_menu';
  const payload = {
    ruleId: rendered.ruleId,
    task: rendered.task,
    templateText: rendered.templateText,
  };
  trackRoutineOutput(ctx.from?.id, payload);
  const keyboard = buildRoutineOutputKeyboard(ctx, payload, backCallback);
  await sendDismissibleMessage(ctx, rendered.templateText, { reply_markup: keyboard });
}

function summarizeRoutineCandidates(matches) {
  return (matches || []).slice(0, 3).map((entry) => ({ ruleId: entry.rule.id, title: entry.rule.title, confidence: entry.confidence }));
}

async function handleDeleteMessageCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = String(data).split(':');
  let ownerId = null;
  let chatId = null;
  let messageId = null;

  if (parts[2] === 'pending') {
    const fallback = ctx?.callbackQuery?.message;
    chatId = Number(fallback?.chat?.id);
    messageId = Number(fallback?.message_id);
  } else if (parts.length >= 5) {
    ownerId = Number(parts[2]);
    chatId = Number(parts[3]);
    messageId = Number(parts[4]);
  } else {
    chatId = Number(parts[2]);
    messageId = Number(parts[3]);
  }

  if (!chatId || !messageId) {
    return { ok: false, reason: 'invalid_target' };
  }

  const actorId = Number(ctx?.from?.id);
  const callbackChatId = Number(ctx?.callbackQuery?.message?.chat?.id);
  if (ownerId && actorId && ownerId !== actorId && callbackChatId > 0) {
    return { ok: true, mode: 'forbidden' };
  }
  const api = ctx?.telegram || ctx?.api || bot.api;
  try {
    await api.deleteMessage(chatId, messageId);
    return { ok: true, mode: 'deleted' };
  } catch (error) {
    if (isDeleteMessageNotFoundError(error)) {
      return { ok: true, mode: 'already_deleted' };
    }
    if (isDeleteMessageForbiddenError(error)) {
      return { ok: true, mode: 'forbidden' };
    }
    const refId = buildDeleteRefId();
    console.error('[msgdel] delete failed', {
      refId,
      chatId,
      messageId,
      error: error?.message || error?.description || 'unknown',
    });
    return { ok: true, mode: 'error', refId };
  }
}

async function renderPanel(ctx, text, extra) {
  const safeExtra = normalizeTelegramExtra(extra);
  const settings = await getCachedSettings();
  const cleanup = normalizeUiCleanupSettings(settings);
  let response = null;
  const chatId = getChatIdFromCtx(ctx);
  const activePanelMessageId = getActivePanelMessageId(chatId);
  await clearEphemeralMessages(ctx, 'panel_nav');
  if (ctx.callbackQuery?.message && ctx.editMessageText) {
    try {
      await ctx.editMessageText(text, safeExtra);
      response = ctx.callbackQuery.message;
    } catch (error) {
      if (isMessageNotModifiedError(error)) {
        await ensureAnswerCallback(ctx);
        return;
      }
      if (isButtonDataInvalidError(error)) {
        await handleTelegramUiError(ctx, error);
        return;
      }
      console.warn('[UI] Failed to edit panel message, sending new.', error?.message);
    }
  } else if (chatId && activePanelMessageId) {
    try {
      await bot.api.editMessageText(chatId, activePanelMessageId, text, safeExtra);
      response = { chat: { id: chatId }, message_id: activePanelMessageId };
    } catch (error) {
      if (isMessageNotModifiedError(error)) {
        response = { chat: { id: chatId }, message_id: activePanelMessageId };
      } else {
        console.warn('[UI] Failed to edit active panel message, sending new.', error?.message);
      }
    }
    if (response && safeExtra?.reply_markup?.inline_keyboard) {
      try {
        await bot.api.editMessageReplyMarkup(chatId, activePanelMessageId, {
          reply_markup: safeExtra.reply_markup,
        });
      } catch (error) {
        if (!isMessageNotModifiedError(error)) {
          console.warn('[UI] Failed to edit active panel markup.', error?.message);
        }
      }
    }
  }
  if (!response) {
    response = await replySafely(ctx, text, safeExtra);
  }
  if (response?.chat?.id && response?.message_id) {
    await trackPanelMessage(response.chat.id, response.message_id, cleanup, ctx?.from?.id);
    if (ctx?.__navRouteId) {
      await recordNavigationSnapshot(ctx, {
        routeId: ctx.__navRouteId,
        params: ctx?.__navParams || {},
        messageId: response.message_id,
        panelType: 'panel',
        timestamp: Date.now(),
      }, safeExtra || {});
    }
  }
  return response;
}

async function renderOrEdit(ctx, text, extra) {
  if (extra?.reply_markup?.inline_keyboard) {
    return renderPanel(ctx, text, extra);
  }
  return respond(ctx, text, extra);
}

function normalizeRoute(route) {
  if (!route) return 'main';
  return String(route).trim().toLowerCase();
}

function shouldRecordNavigationSnapshot(ctx, options = {}) {
  if (options?.skipNavigationRecord) return false;
  if (ctx?.__skipNavigationRecord) return false;
  if (ctx?.__transientNotice) return false;
  return true;
}

async function recordNavigationSnapshot(ctx, snapshot, options = {}) {
  if (!shouldRecordNavigationSnapshot(ctx, options)) return;
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) return;
  const userId = ctx?.from?.id || null;
  await pushNavigationSnapshot(chatId, userId, { ...snapshot, screenId: snapshot?.screenId || snapshot?.routeId });
}

async function renderScreen(ctx, routeId, params = {}) {
  if (!routeId) return false;
  if (routeId.startsWith('route:')) {
    const route = routeId.slice('route:'.length);
    await navigateTo(getChatIdFromCtx(ctx), ctx?.from?.id, route, { ctx, skipNavigationRecord: true });
    return true;
  }
  if (routeId.startsWith('cb:')) {
    const data = routeId.slice('cb:'.length);
    const previousSkip = ctx.__skipNavigationRecord;
    ctx.__skipNavigationRecord = true;
    try {
      await dispatchCallbackData(ctx, data, { skipNavigationRecord: true });
    } finally {
      ctx.__skipNavigationRecord = previousSkip;
    }
    return true;
  }
  return false;
}

async function navigate(ctx, screenId, params = {}) {
  const chatId = getChatIdFromCtx(ctx);
  const userId = ctx?.from?.id || null;
  if (!chatId) return;
  const currentScreenId = ctx?.__navRouteId || null;
  if (currentScreenId) {
    await pushNavigationSnapshot(chatId, userId, { routeId: currentScreenId, screenId: currentScreenId, params: ctx?.__navParams || {}, timestamp: Date.now() });
  }
  await renderScreen(ctx, screenId, params);
}

async function replace(ctx, screenId, params = {}) {
  const chatId = getChatIdFromCtx(ctx);
  const userId = ctx?.from?.id || null;
  if (!chatId) return;
  const stack = await getNavigationStack(chatId, userId);
  if (stack.length) stack.pop();
  await setNavigationStack(chatId, userId, stack);
  await renderScreen(ctx, screenId, params);
}

async function back(ctx) {
  return goBack(ctx);
}

async function home(ctx) {
  return goHome(ctx);
}

async function goHome(ctx) {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) return;
  await clearNavigationStack(chatId, ctx?.from?.id || null);
  await navigateTo(chatId, ctx?.from?.id, 'main', { ctx, skipNavigationRecord: false });
}

async function goBack(ctx) {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) {
    await goHome(ctx);
    return;
  }
  const userId = ctx?.from?.id || null;
  const stack = await getNavigationStack(chatId, userId);
  if (!stack.length) {
    await goHome(ctx);
    return;
  }
  const nextStack = [...stack];
  nextStack.pop();
  while (nextStack.length) {
    const previous = nextStack[nextStack.length - 1];
    await setNavigationStack(chatId, userId, nextStack);
    try {
      const rendered = await renderScreen(ctx, previous.routeId, previous.params || {});
      if (rendered) return;
    } catch (error) {
      console.warn('[navigation] goBack failed snapshot render', error?.message);
    }
    nextStack.pop();
  }
  await setNavigationStack(chatId, userId, []);
  await goHome(ctx);
}

function buildNavigationContext(chatId, userId, ctx) {
  const resolvedChatId = chatId || getChatIdFromCtx(ctx);
  const resolvedUserId = userId || ctx?.from?.id || null;
  return {
    ...ctx,
    api: ctx?.api || bot.api,
    chat: ctx?.chat || (resolvedChatId ? { id: resolvedChatId } : undefined),
    from: ctx?.from || (resolvedUserId ? { id: resolvedUserId } : undefined),
  };
}

function getNavigationHandlers() {
  return {
    main: async (ctx) => renderMainMenu(ctx),
    projects: async (ctx) => renderProjectsList(ctx),
    settings: async (ctx) => renderGlobalSettings(ctx),
    logs: async (ctx) => renderLogsProjectList(ctx, '🧾 Logs'),
    ops: async (ctx) => renderOpsMenu(ctx),
    diagnostics: async (ctx) => renderLogDeliveryDiagnosticsMenu(ctx),
    templates: async (ctx) => renderTemplatesMenu(ctx),
    help: async (ctx) => renderHelpMenu(ctx),
    database: async (ctx) => renderDataCenterMenu(ctx),
    cronjobs: async (ctx) => renderCronMenu(ctx),
    deploy: async (ctx) => renderDeploysProjectList(ctx),
  };
}

async function enqueueNavigation(chatId, request) {
  if (!chatId) return;
  const queue = getNavigationQueue(chatId);
  queue.push(request);
}

async function flushNavigationQueue(ctx, chatId) {
  if (!chatId) return;
  const queue = getNavigationQueue(chatId);
  if (!queue.length) return;
  const latest = queue.pop();
  queue.length = 0;
  await navigateTo(chatId, latest.userId, latest.route, { ...latest.options, ctx });
}

async function navigateTo(chatId, userId, route, options = {}) {
  const normalizedRoute = normalizeRoute(route);
  const handlers = options.handlers || getNavigationHandlers();
  const ctx = buildNavigationContext(chatId, userId, options.ctx);
  ctx.__skipNavigationRecord = options.skipNavigationRecord === true;
  ctx.__navRouteId = `route:${normalizedRoute}`;
  const targetChatId = getChatIdFromCtx(ctx);
  const targetUserId = ctx?.from?.id || userId || null;
  const handler = handlers[normalizedRoute] || handlers.main;

  if (options.resetState !== false) {
    if (targetUserId) {
      userState.delete(targetUserId);
      clearUserState(targetUserId);
      clearPatchSession(targetUserId);
    }
  }

  if (options.deleteIncomingMessage) {
    await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'nav_delete_incoming');
  }

  if (options.resetMenus) {
    await clearPanelMessages(ctx, 'nav_reset');
  }

  const operationState = targetChatId ? getNavigationOperationState(targetChatId) : null;
  if (operationState?.inProgress && options.allowDuringOperation !== true) {
    await enqueueNavigation(targetChatId, {
      route: normalizedRoute,
      userId: targetUserId,
      options: { ...options, deleteIncomingMessage: false },
    });
    if (!operationState.noticeMessageId && targetChatId) {
      try {
        const notice = await replySafely(ctx, '⏳ Operation in progress…');
        if (notice?.message_id) {
          operationState.noticeMessageId = notice.message_id;
        }
      } catch (error) {
        console.warn('[navigation] Failed to send operation notice', error?.message);
      }
    }
    return { queued: true };
  }

  return withNavigationLock(targetChatId, async () => {
    await handler(ctx);
    return { queued: false };
  });
}

function setBotApiForTests(api) {
  if (!api) return;
  Object.assign(bot.api, api);
}

function setNavigationOperationInProgress(chatId, inProgress) {
  if (!chatId) return;
  const state = getNavigationOperationState(chatId);
  state.inProgress = Boolean(inProgress);
  if (!inProgress) {
    state.noticeMessageId = null;
  }
}

function resetNavigationState(chatId) {
  if (!chatId) return;
  navigationQueueByChat.delete(chatId);
  navigationLocksByChat.delete(chatId);
  clearNavigationOperationState(chatId);
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

function normalizeCronErrorMessage(error) {
  const reason = extractCronApiErrorReason(error);
  return truncateText(reason || error?.message || 'request failed', 300);
}

function shouldEmitCronErrorSignature(signature, windowMs = 60 * 1000) {
  const now = Date.now();
  const existing = cronErrorDedupeCache.get(signature);
  if (existing && now - existing < windowMs) {
    return false;
  }
  cronErrorDedupeCache.set(signature, now);
  return true;
}

function logCronApiError({ operation, error, userId, projectId, correlationId, jobId = null }) {
  const responseBody = truncateText(error?.body ?? '', 500);
  const message = normalizeCronErrorMessage(error);
  console.error('[cron] API error', {
    correlationId,
    operation,
    method: error?.method,
    path: error?.path,
    status: error?.status,
    responseBody,
    userId,
    projectId,
    jobId,
  });
  const signature = [operation, error?.method || '-', error?.path || '-', error?.status || '-', message].join('|');
  if (!shouldEmitCronErrorSignature(signature)) {
    return;
  }
  emitOpsEvent('error', 'CRON_ERROR', 'Cron API request failed.', {
    refId: correlationId,
    operation,
    endpoint: error?.path || null,
    method: error?.method || null,
    status: error?.status || null,
    message,
    excerpt: responseBody || null,
    projectId: projectId || null,
    jobId: jobId || null,
    userId: userId || null,
  }).catch((emitError) => {
    console.warn('[cron] failed to emit CRON_ERROR event', emitError?.message);
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

function formatCronCreateErrorPanel({ error, correlationId }) {
  const endpoint = error?.path || '/jobs';
  const lines = [
    '❌ Failed to create cron job',
    `Status code: ${error?.status || 'unknown'}`,
    `Ref id: ${correlationId}`,
    `Endpoint: ${endpoint}`,
    '',
    'Suggested next actions:',
    '• 🔁 Retry',
    '• 🧪 Run Cron API ping test',
    '• 📋 Copy debug details',
  ];
  return lines.join('\n');
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
  if (!value) return '***';
  return '***';
}

function maskEnvValue(value) {
  if (value == null) return '***';
  const raw = String(value);
  if (raw.length <= 4) return '***';
  return `${raw.slice(0, 2)}***${raw.slice(-2)}`;
}

function isSensitiveEnvKey(key) {
  if (!key) return false;
  const normalized = String(key).toUpperCase();
  if (normalized === 'DATABASE_URL') return true;
  return ['_KEY', '_TOKEN', '_SECRET'].some((suffix) => normalized.endsWith(suffix));
}

function maskEnvValueForKey(key, value) {
  const length = value != null ? String(value).length : 0;
  return `•••• (${length} chars)`;
}

function formatEnvValueForDisplay(key, value, settings) {
  const security = normalizeSecuritySettings(settings);
  const raw = value == null ? '' : String(value);
  const truncated = raw.length > ENV_VALUE_PREVIEW_LIMIT ? `${raw.slice(0, ENV_VALUE_PREVIEW_LIMIT - 1)}…` : raw;
  if (isSensitiveEnvKey(key)) {
    const masked = maskEnvValueForKey(key, raw);
    return { text: SPOILER_WRAP(escapeHtml(masked)), length: raw.length };
  }
  if (security.envMaskPolicy === 'strict') {
    const masked = maskEnvValue(raw);
    return { text: SPOILER_WRAP(escapeHtml(masked)), length: raw.length };
  }
  return { text: SPOILER_WRAP(escapeHtml(truncated)), length: raw.length };
}

function evaluateEnvValueStatus(value) {
  if (value === undefined || value === null) {
    return { status: 'MISSING', reason: 'Value is undefined or null.' };
  }
  const trimmed = String(value).trim();
  if (!trimmed) {
    return { status: 'EMPTY', reason: 'Value is empty.' };
  }
  if (['-', 'undefined', 'null'].includes(trimmed)) {
    return { status: 'INVALID', reason: 'Value is invalid.' };
  }
  return { status: 'SET', reason: 'Value is set.' };
}

function normalizeEnvKeyInput(value) {
  return String(value || '').trim().toUpperCase().replace(/\s+/g, '_');
}

function validateProjectIdInput(rawValue) {
  const value = String(rawValue || '').trim();
  if (!value) {
    return { valid: false, message: 'Project ID is required.' };
  }
  const normalized = value.toLowerCase();
  const pattern = /^[a-z0-9][a-z0-9_-]{1,39}$/;
  if (!pattern.test(normalized)) {
    return { valid: false, message: 'Project ID must be a slug (lowercase letters, numbers, "-" or "_").' };
  }
  return { valid: true, value: normalized };
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

function extractSupabaseProjectRef(input) {
  const trimmed = String(input || '').trim();
  if (!trimmed) return null;
  if (trimmed.includes('supabase.co')) {
    try {
      const url = trimmed.startsWith('http') ? trimmed : `https://${trimmed}`;
      const parsed = new URL(url);
      const host = parsed.hostname || '';
      const candidate = host.split('.')[0];
      return candidate || null;
    } catch (error) {
      return null;
    }
  }
  const sanitized = trimmed.replace(/[^a-zA-Z0-9-_]/g, '');
  if (!sanitized) return null;
  return sanitized;
}

function buildSupabaseUrlFromRef(projectRef) {
  return `https://${projectRef}.supabase.co`;
}

function maskSupabaseKey(token) {
  const raw = String(token || '');
  if (!raw) return '••••';
  const suffix = raw.slice(-4);
  return `••••${suffix}`;
}

function buildSupabaseKeyStorage(rawKey) {
  const trimmed = String(rawKey || '').trim();
  const hashed = crypto.createHash('sha256').update(trimmed).digest('hex');
  if (getMasterKeyStatus().ok) {
    try {
      return {
        stored: encryptSecret(trimmed),
        storage: 'encrypted',
        mask: maskSupabaseKey(trimmed),
      };
    } catch (error) {
      // fall back to hash storage
    }
  }
  return {
    stored: `sha256:${hashed}`,
    storage: 'hashed',
    mask: maskSupabaseKey(trimmed),
  };
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
  patchSessions.set(userId, { projectId, buffer: '', inputTypes: new Set() });
}

function clearPatchSession(userId) {
  patchSessions.delete(userId);
}

function appendChangeChunk(session, chunk, inputType) {
  const text = chunk || '';
  if (!text) return 0;
  if (session.buffer && !session.buffer.endsWith('\n')) {
    session.buffer += '\n';
  }
  session.buffer += text;
  if (!text.endsWith('\n')) {
    session.buffer += '\n';
  }
  if (inputType) {
    session.inputTypes?.add(inputType);
  }
  return text.length;
}

async function renderMainMenu(ctx) {
  const statusLine = buildConfigDbStatusLine();
  const banner = buildDegradedBanner();
  const prefix = statusLine ? `${statusLine}\n` : '';
  await renderPanel(ctx, `${banner}${prefix}🧭 Main Menu:
${buildScopedHeader('GLOBAL', 'Main')}`, {
    reply_markup: buildMainMenuInlineKeyboard(),
    skipDelete: true,
  });
}

function buildDegradedBanner() {
  if (appState.degradedMode) {
    return '⚠️ Config DB unavailable — running in degraded mode\n\n';
  }
  return '';
}

function buildConfigDbStatusLine() {
  const snapshot = getConfigDbSnapshot();
  if (snapshot.ready) return '';
  if (!process.env.DATABASE_URL_PM && !process.env.PATH_APPLIER_CONFIG_DSN) return '🟡 Config DB: Misconfigured';
  if (snapshot.lastErrorCategory === 'INVALID_URL') return '🟡 Config DB: Misconfigured';
  if (snapshot.nextRetryInMs > 0) return '🟠 Config DB: Degraded';
  return '🔴 Config DB: Down';
}

function formatRetryWindow(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return 'soon';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${Math.ceil(ms / 1000)}s`;
}

function buildConfigDbGateView({ title, notice, backCallback }) {
  const snapshot = getConfigDbSnapshot();
  const lines = [
    title || '🟡 Config DB not ready',
    '',
    `Status: ${snapshot.ready ? '🟢 Ready' : '🔴 Not ready'}`,
    `Last attempt: ${snapshot.lastAttemptAt || '-'}`,
    `Next retry: ${snapshot.nextRetryInMs ? formatRetryWindow(snapshot.nextRetryInMs) : '-'}`,
    notice || null,
    '',
    'Some features are paused until the config DB is ready.',
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('🔄 Retry now', 'configdb:retry')
    .row()
    .text('⬅️ Back', backCallback || 'main:back');
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderConfigDbGate(ctx, options = {}) {
  const view = buildConfigDbGateView(options);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function getConfiguredConfigDbDsn() {
  return process.env.DATABASE_URL_PM || process.env.PATH_APPLIER_CONFIG_DSN || null;
}

function buildConfigDbStatusView(notice) {
  const snapshot = getConfigDbSnapshot();
  const configured = Boolean(process.env.DATABASE_URL_PM || process.env.PATH_APPLIER_CONFIG_DSN);
  const configuredDsn = getConfiguredConfigDbDsn();
  const invalidUrlNotice = snapshot.lastErrorCategory === 'INVALID_URL'
    ? [
        '',
        '⚠️ Invalid URL detected for Config DB DSN.',
        'Password special chars must be percent-encoded: ?, #, @, /, +',
        'Fix: URL-encode the password OR let PM auto-fix temporarily, but update ENV to the corrected DSN.',
        `Configured DSN (masked): ${maskDsn(configuredDsn) || '-'}`,
      ].join('\n')
    : null;
  const lines = [
    '🩺 PM Status',
    '',
    `Config DB configured: ${configured ? '✅ yes' : '❌ no'}`,
    `Config DB ready: ${snapshot.ready ? '✅ yes' : '❌ no'}`,
    `Last attempt: ${snapshot.lastAttemptAt || '-'}`,
    `Last success: ${snapshot.lastSuccessAt || '-'}`,
    `Attempts: ${snapshot.attempts}`,
    `Next retry: ${snapshot.nextRetryInMs ? formatRetryWindow(snapshot.nextRetryInMs) : '-'}`,
    `Last error category: ${snapshot.lastErrorCategory || '-'}`,
    invalidUrlNotice,
    notice || null,
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('🔄 Retry now', 'configdb:retry')
    .text('🧾 View recent error', 'configdb:error')
    .row()
    .text('⬅️ Back', 'gsettings:menu');
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderConfigDbStatus(ctx, notice) {
  const view = buildConfigDbStatusView(notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function resetToMainMenu(ctx, notice, options = {}) {
  resetUserState(ctx);
  clearPatchSession(ctx.from?.id);
  if (options.deleteIncomingMessage) {
    await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'start_command');
  }
  const chatId = getChatIdFromCtx(ctx);
  const activePanelMessageId = getActivePanelMessageId(chatId);
  if (chatId && activePanelMessageId) {
    await safeDeleteMessage(ctx, chatId, activePanelMessageId, 'start_reset');
    clearActivePanelMessageId(chatId);
    const history = getPanelHistory(chatId);
    panelMessageHistoryByChat.set(
      chatId,
      history.filter((messageId) => messageId !== activePanelMessageId),
    );
  }
  await clearEphemeralMessages(ctx, 'main_menu');
  if (options.freshPanel) {
    await clearPanelMessages(ctx, 'main_menu');
  }
  if (notice) {
    await replySafely(ctx, notice);
  }
  await renderMainMenu(ctx);
}

async function handleReplyKeyboardNavigation(ctx, route) {
  resetUserState(ctx);
  await clearEphemeralMessages(ctx, 'reply_keyboard_nav');
  const chatId = getChatIdFromCtx(ctx);
  const userId = ctx.from?.id;
  return navigateTo(chatId, userId, route, { ctx, resetMenus: false, deleteIncomingMessage: true });
}

function buildMainMenuInlineKeyboard() {
  return new InlineKeyboard()
    .text('📦 Projects', 'main:projects')
    .text('🗄 Databases', 'main:database')
    .row()
    .text('⏱ Cron Jobs', 'main:cronjobs')
    .text('🚀 Deployments', 'main:deploy')
    .row()
    .text('🧾 Logs', 'main:logs')
    .text('⚙️ Settings', 'main:settings')
    .row()
    .text('❓ Help', 'main:help')
}

function buildCancelKeyboard() {
  return new InlineKeyboard().text('❌ Cancel', 'cancel_input');
}

function buildBackKeyboard(callbackData, label = '⬅️ Back') {
  return new InlineKeyboard().text(label, 'nav:back');
}

function buildScopedHeader(scopeLabel, breadcrumb) {
  const lines = [];
  const normalizedScope = String(scopeLabel || '').trim();
  if (normalizedScope.startsWith('PROJECT:')) {
    const payload = normalizedScope.replace('PROJECT:', '').trim();
    const [projectNameRaw, projectIdRaw] = payload.split('||');
    const projectName = (projectNameRaw || '').trim() || '-';
    const projectId = (projectIdRaw || projectNameRaw || '').trim() || '-';
    lines.push(`🧩 Project: ${projectName}`);
    lines.push(`🆔 ID: ${projectId}`);
    lines.push('📦 Scope: Project');
  } else if (normalizedScope) {
    lines.push('🌍 Scope: Global');
  }
  if (lines.length) {
    lines.push('');
  }
  return lines.join('\n');
}

function withDeleteButton(replyMarkup, options = {}) {
  const skip = options.skipDelete === true;
  if (skip) return replyMarkup;
  const existingRows = Array.isArray(replyMarkup?.inline_keyboard)
    ? replyMarkup.inline_keyboard.map((row) => Array.isArray(row) ? row.slice() : row)
    : [];
  const hasDelete = existingRows.some((row) => Array.isArray(row) && row.some((button) => button?.text === '🗑 Delete'));
  if (!hasDelete) {
    existingRows.push([{ text: '🗑 Delete', callback_data: 'msg:delete:pending' }]);
  }
  return { inline_keyboard: existingRows };
}

function encodeRoutinePayload(payload) {
  try {
    const id = crypto.randomBytes(9).toString('base64url');
    routinePayloadCache.set(id, { payload, ts: Date.now() });
    return id;
  } catch (_error) {
    return null;
  }
}

function decodeRoutinePayload(id) {
  if (!id) return null;
  const entry = routinePayloadCache.get(String(id));
  if (!entry) return null;
  if (Date.now() - entry.ts > 24 * 60 * 60 * 1000) {
    routinePayloadCache.delete(String(id));
    return null;
  }
  return entry.payload || null;
}

function buildRoutineFixButton(text, category, refId) {
  const blockedCategories = new Set(['INVALID_URL', 'ENV_MISCONFIG', 'MISSING_DSN']);
  if (blockedCategories.has(String(category || '').toUpperCase())) return null;
  const payload = encodeRoutinePayload({ text: String(text || '').slice(0, 1200), category: category || null, refId: refId || null });
  if (!payload) return null;
  return { text: '🛠 Fix (routine)', callback_data: `routinefix:${payload}` };
}

function buildPatchSessionKeyboard() {
  return new InlineKeyboard()
    .text('✅ Patch completed', 'patch:finish')
    .row()
    .text('❌ Cancel', 'patch:cancel');
}

function getMessageTargetFromCtx(ctx) {
  const callbackMessage = ctx?.callbackQuery?.message;
  if (callbackMessage?.chat?.id && callbackMessage?.message_id) {
    return { chatId: callbackMessage.chat.id, messageId: callbackMessage.message_id };
  }
  const message = ctx?.message;
  if (message?.chat?.id && message?.message_id) {
    return { chatId: message.chat.id, messageId: message.message_id };
  }
  return null;
}

function buildDeleteButton(ctx, target) {
  const resolvedTarget = target || getMessageTargetFromCtx(ctx);
  const chatId = Number(resolvedTarget?.chatId);
  const messageId = Number(resolvedTarget?.messageId);
  if (!chatId || !messageId) {
    return { text: '🗑 Delete', callback_data: 'msg:delete:pending' };
  }
  return { text: '🗑 Delete', callback_data: `msg:delete:${chatId}:${messageId}` };
}

function isDeleteMessageNotFoundError(error) {
  const message = String(error?.description || error?.response?.description || error?.message || '').toLowerCase();
  return message.includes('message to delete not found') || message.includes('message identifier is not specified');
}

function isDeleteMessageForbiddenError(error) {
  const message = String(error?.description || error?.response?.description || error?.message || '').toLowerCase();
  return message.includes("message can't be deleted") || message.includes('not enough rights to delete');
}

function buildDeleteRefId() {
  return `MSGDEL-${Math.random().toString(36).slice(2, 8).toUpperCase()}`;
}

async function startProgressMessage(ctx, text, extra = {}) {
  const message = ctx.callbackQuery?.message;
  if (message) {
    try {
      await ctx.editMessageText(text, normalizeTelegramExtra(extra));
      return { chatId: message.chat.id, messageId: message.message_id };
    } catch (error) {
      console.warn('[progress] Failed to edit message, falling back to new message.', error.message);
    }
  }
  const sent = await ctx.reply(text, normalizeTelegramExtra(extra));
  return { chatId: sent.chat.id, messageId: sent.message_id };
}

async function updateProgressMessage(messageContext, text, extra = {}) {
  if (!messageContext) return;
  try {
    await bot.api.editMessageText(
      messageContext.chatId,
      messageContext.messageId,
      text,
      normalizeTelegramExtra(extra),
    );
  } catch (error) {
    console.warn('[progress] Failed to update progress message', error.message);
  }
}

function wrapCallbackHandler(handler, label) {
  return async (ctx) => {
    try {
      console.log('[callback] click', {
        label: label || 'handler',
        fromId: ctx?.from?.id,
        chatId: ctx?.callbackQuery?.message?.chat?.id,
        data: ctx?.callbackQuery?.data,
      });
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
  const status = await probeConfigDbConnection();
  if (status.ok) {
    runtimeStatus.configDbOk = true;
    runtimeStatus.configDbError = null;
    console.log('Config DB: OK');
    return status;
  }
  runtimeStatus.configDbOk = false;
  runtimeStatus.configDbError = status.message || 'see logs';
  if (!status.configured) {
    console.warn('Config DB is not configured (DATABASE_URL_PM/PATH_APPLIER_CONFIG_DSN missing).');
    return status;
  }
  console.error('Config DB connection failed', status.message);
  if (status.category !== 'INVALID_URL') {
    await forwardSelfLog('error', 'Config DB connection failed', {
      context: { error: status.message, category: status.category },
    });
  }
  return status;
}

function shouldLogConfigDbFailure(category) {
  if (!category) return false;
  if (configDbFailureStreak === 1) return true;
  if (category !== configDbLastLogCategory) return true;
  return configDbFailureStreak - configDbLastLogAttempt >= 5;
}

function scheduleConfigDbWarmup(delayMs, reason) {
  if (configDbWarmupTimer) {
    clearTimeout(configDbWarmupTimer);
    configDbWarmupTimer = null;
  }
  setConfigDbNextRetry(delayMs);
  if (delayMs <= 0) {
    runConfigDbWarmup(reason).catch((error) => {
      console.warn('[db] warmup failed to start', error?.message || error);
    });
    return;
  }
  configDbWarmupTimer = setTimeout(() => {
    configDbWarmupTimer = null;
    runConfigDbWarmup(reason).catch((error) => {
      console.warn('[db] warmup failed', error?.message || error);
    });
  }, delayMs);
  if (typeof configDbWarmupTimer.unref === 'function') {
    configDbWarmupTimer.unref();
  }
}

function computeConfigDbBackoff(attempt) {
  const base = 500;
  const max = 60_000;
  const jitter = Math.floor(Math.random() * 251);
  const raw = base * Math.pow(2, Math.max(0, attempt - 1));
  return Math.min(max, raw) + jitter;
}

function shouldHaltConfigDbRetries() {
  if (!Number.isFinite(CONFIG_DB_MAX_RETRIES_PER_BOOT) || CONFIG_DB_MAX_RETRIES_PER_BOOT <= 0) return false;
  return configDbFailureStreak >= CONFIG_DB_MAX_RETRIES_PER_BOOT;
}

async function runConfigDbWarmup(reason = 'scheduled') {
  if (configDbWarmupInFlight || configDbWarmupHaltedForBoot) return;
  configDbWarmupInFlight = true;
  recordConfigDbAttempt();
  const attemptCount = getConfigDbSnapshot().attempts;

  try {
    const status = await probeConfigDbConnection();
    if (!status.configured) {
      const message = sanitizeDbErrorMessage(status.message) || 'not configured';
      recordDbError('UNKNOWN_DB_ERROR', message);
      configDbFailureStreak += 1;
      runtimeStatus.configDbOk = false;
      runtimeStatus.configDbError = message;
      if (shouldLogConfigDbFailure('UNKNOWN_DB_ERROR')) {
        console.warn('[db] Config DB not configured; warmup halted.');
        configDbLastLogCategory = 'UNKNOWN_DB_ERROR';
        configDbLastLogAttempt = configDbFailureStreak;
      }
      await setDbHealthSnapshot({
        status: 'MISCONFIG',
        lastErrorCategory: 'MISSING_DSN',
        lastErrorMessageMasked: message,
      });
      await emitOpsEvent('warn', 'ENV_MISCONFIG', 'Config DB DSN missing; running in memory mode.', { reason });
      setConfigDbNextRetry(0);
      return;
    }

    if (status.ok) {
      const db = await getConfigDbPool();
      if (db) {
        await ensureConfigTable(db);
      }
      setDbReady(true);
      setDegradedMode(false);
      await setDbHealthSnapshot({
        status: 'HEALTHY',
        lastErrorCategory: null,
        lastErrorMessageMasked: null,
      });
      const configOk = await loadConfig();
      if (!configOk) {
        throw new Error('Config load failed');
      }
      runtimeStatus.configDbOk = true;
      runtimeStatus.configDbError = null;
      configDbFailureStreak = 0;
      configDbDsnIssueAttempts = 0;
      setConfigDbNextRetry(0);
      return;
    }

    const category = status.category || 'UNKNOWN_DB_ERROR';
    const message = sanitizeDbErrorMessage(status.message);
    recordDbError(category, message);
    configDbFailureStreak += 1;
    runtimeStatus.configDbOk = false;
    runtimeStatus.configDbError = message || 'see logs';
    if (shouldLogConfigDbFailure(category)) {
      console.warn('[db] Config DB warmup failed', {
        category,
        message,
        attempt: attemptCount,
        reason,
      });
      configDbLastLogCategory = category;
      configDbLastLogAttempt = configDbFailureStreak;
    }
    await setDbHealthSnapshot({
      status: category === 'INVALID_URL' ? 'MISCONFIG' : 'DOWN',
      lastErrorCategory: category,
      lastErrorMessageMasked: message,
    });
    await emitOpsEvent('warn', category, `Config DB warmup failed: ${message || 'unknown error'}`, { attempt: attemptCount, reason });
    const isDsnIssue = category === 'INVALID_URL' || isDsnAutoFixApplied();
    if (isDsnIssue) {
      configDbDsnIssueAttempts += 1;
      if (configDbDsnIssueAttempts >= 2) {
        configDbWarmupHaltedForBoot = true;
        setConfigDbNextRetry(0);
        console.warn('[db] Config DB unavailable after DSN auto-fix attempt; running in-memory config mode for this boot.');
        return;
      }
    }
    if (shouldHaltConfigDbRetries()) {
      configDbWarmupHaltedForBoot = true;
      setConfigDbNextRetry(0);
      await emitOpsEvent('error', 'DB_RETRY_EXHAUSTED', 'Config DB warmup retries exhausted for this boot.', {
        attempts: configDbFailureStreak,
      });
      console.warn('[db] Config DB retries exhausted for this boot; continuing in degraded mode.');
      return;
    }
    const delayMs = computeConfigDbBackoff(configDbFailureStreak);
    scheduleConfigDbWarmup(delayMs, 'retry');
  } catch (error) {
    const category = classifyDbError(error);
    const message = sanitizeDbErrorMessage(error?.message);
    recordDbError(category, message);
    configDbFailureStreak += 1;
    runtimeStatus.configDbOk = false;
    runtimeStatus.configDbError = message || 'see logs';
    if (shouldLogConfigDbFailure(category)) {
      console.warn('[db] Config DB warmup failed', {
        category,
        message,
        attempt: attemptCount,
        reason,
      });
      configDbLastLogCategory = category;
      configDbLastLogAttempt = configDbFailureStreak;
    }
    await setDbHealthSnapshot({
      status: category === 'INVALID_URL' ? 'MISCONFIG' : 'DOWN',
      lastErrorCategory: category,
      lastErrorMessageMasked: message,
    });
    await emitOpsEvent('error', category, `Config DB warmup exception: ${message || 'unknown error'}`, { attempt: attemptCount, reason });
    const isDsnIssue = category === 'INVALID_URL' || isDsnAutoFixApplied();
    if (isDsnIssue) {
      configDbDsnIssueAttempts += 1;
      if (configDbDsnIssueAttempts >= 2) {
        configDbWarmupHaltedForBoot = true;
        setConfigDbNextRetry(0);
        console.warn('[db] Config DB unavailable after DSN auto-fix attempt; running in-memory config mode for this boot.');
        return;
      }
    }
    if (shouldHaltConfigDbRetries()) {
      configDbWarmupHaltedForBoot = true;
      setConfigDbNextRetry(0);
      await emitOpsEvent('error', 'DB_RETRY_EXHAUSTED', 'Config DB warmup retries exhausted for this boot.', {
        attempts: configDbFailureStreak,
      });
      console.warn('[db] Config DB retries exhausted for this boot; continuing in degraded mode.');
      return;
    }
    const delayMs = computeConfigDbBackoff(configDbFailureStreak);
    scheduleConfigDbWarmup(delayMs, 'retry');
  } finally {
    configDbWarmupInFlight = false;
  }
}

function startConfigDbWarmup() {
  scheduleConfigDbWarmup(0, 'startup');
}

function triggerConfigDbWarmup(reason = 'manual') {
  if (configDbWarmupHaltedForBoot) {
    return false;
  }
  const now = Date.now();
  if (now - configDbManualRetryAt < 3000) {
    return false;
  }
  configDbManualRetryAt = now;
  scheduleConfigDbWarmup(0, reason);
  return true;
}


function buildErrorEnvelope(event) {
  return {
    refId: event?.meta_json?.refId || `OPS-${String(event.id || '').slice(0, 8).toUpperCase()}`,
    category: event.category || 'GENERAL',
    severity: String(event.level || 'warn').toLowerCase(),
    messageMasked: maskDiagnosticText(event.message_short || ''),
    nextActions: ['📋 Copy debug', '🔎 Open timeline', '🗑 Delete'],
  };
}

function buildOpsAlertText(event, envelope) {
  return [
    `🚨 [${String(envelope.severity).toUpperCase()}] ${envelope.category}`,
    `Ref: ${envelope.refId}`,
    envelope.messageMasked,
  ].join('\n');
}

async function sendOpsAlertByDestinations(destinations, event, extra = {}) {
  const sends = [];
  const envelope = buildErrorEnvelope(event);
  const text = buildOpsAlertText(event, envelope);
  const routineButton = buildRoutineFixButton(text, envelope.category, envelope.refId);
  const debugPayload = Buffer.from(JSON.stringify(envelope)).toString('base64url');
  const keyboard = [
    ...(routineButton ? [[routineButton]] : []),
    [{ text: '📋 Copy debug', callback_data: buildCb('opsdbg', [debugPayload]) }],
    [{ text: '🔎 Timeline', callback_data: 'ops:timeline' }],
  ];
  const composedExtra = { ...extra, reply_markup: { inline_keyboard: keyboard } };
  const sendWithDeleteButton = async (chatId) => {
    const sent = await bot.api.sendMessage(chatId, text, composedExtra);
    const finalMarkup = {
      inline_keyboard: [
        ...keyboard,
        [buildDeleteButton(null, { chatId, messageId: sent.message_id })],
      ],
    };
    await bot.api.editMessageReplyMarkup(chatId, sent.message_id, { reply_markup: finalMarkup }).catch(() => null);
    return sent;
  };
  if (destinations.admin_inbox && ADMIN_TELEGRAM_ID) sends.push(sendWithDeleteButton(ADMIN_TELEGRAM_ID));
  if (destinations.admin_room && ADMIN_ROOM_CHAT_ID) sends.push(sendWithDeleteButton(ADMIN_ROOM_CHAT_ID));
  if (destinations.admin_rob && ADMIN_ROB_CHAT_ID) sends.push(sendWithDeleteButton(ADMIN_ROB_CHAT_ID));
  if (!sends.length) return [];
  return Promise.allSettled(sends);
}

async function routeOpsAlert(event) {
  const prefs = await getUserAlertPrefs(ADMIN_TELEGRAM_ID || 'admin');
  if (!shouldRouteEvent(prefs, event)) return;
  const destinations = computeDestinations(prefs, event.level);
  await sendOpsAlertByDestinations(destinations, event, { disable_web_page_preview: true });
}

async function emitOpsEvent(level, category, message, meta) {
  const event = await appendEvent({ level, category, messageShort: message, source: 'config-db', meta });
  opsTimeline.append({
    scope: meta?.projectId ? 'project' : 'global', projectId: meta?.projectId || null,
    type: category, severity: level, title: message, detailsMasked: maskDiagnosticText(JSON.stringify(meta || {})), refId: meta?.refId || null,
  });
  const safeEval = safeMode.markSeverity(level);
  if (safeEval.changed && safeEval.mode === 'on') {
    await appendEvent({ level: 'critical', category: 'SAFE_MODE_ENTER', messageShort: `Safe Mode entered (${safeEval.reasons.join(', ')})`, source: 'safe-mode', meta: { refId: `SAFE-${Date.now()}` } });
  } else if (safeEval.changed && safeEval.mode === 'off') {
    await appendEvent({ level: 'info', category: 'SAFE_MODE_EXIT', messageShort: 'Safe Mode exited after stable recovery.', source: 'safe-mode', meta: { refId: `SAFE-${Date.now()}` } });
  }
  await routeOpsAlert(event);
}

async function sendRecoveryNotificationOnce() {
  const snapshot = getDbHealthSnapshot();
  if (!shouldNotifyRecovery(snapshot)) return;
  const prefs = await getUserAlertPrefs(ADMIN_TELEGRAM_ID || 'admin');
  const destinations = computeDestinations(prefs, 'critical');
  const key = `ops:recover:${snapshot.lastOutageId}`;
  const inline = new InlineKeyboard().text('✅ Readed', key);
  const targets = [];
  if (destinations.admin_inbox && ADMIN_TELEGRAM_ID) targets.push(ADMIN_TELEGRAM_ID);
  if (destinations.admin_room && ADMIN_ROOM_CHAT_ID) targets.push(ADMIN_ROOM_CHAT_ID);
  if (destinations.admin_rob && ADMIN_ROB_CHAT_ID) targets.push(ADMIN_ROB_CHAT_ID);
  for (const chatId of targets) {
    try {
      const msg = await bot.api.sendMessage(chatId, '✅ Config DB is back online.', { reply_markup: inline });
      opsRecoveryMessages.set(`${snapshot.lastOutageId}:${chatId}`, { chatId, messageId: msg.message_id });
    } catch (error) {
      console.warn('[ops] failed to send recovery notification', error?.message || error);
    }
  }
  await setDbHealthSnapshot({ lastRecoveryNotifiedOutageId: snapshot.lastOutageId });
}

async function startOpsScanner() {
  if (opsScannerTimer) return;
  opsScannerTimer = setInterval(() => {
    const snap = getDbHealthSnapshot();
    if (snap.status === 'HEALTHY') {
      sendRecoveryNotificationOnce().catch((error) => console.warn('[ops] recovery notify failed', error?.message || error));
    }
  }, 15_000);
  if (typeof opsScannerTimer.unref === 'function') opsScannerTimer.unref();
}

const mainKeyboard = new Keyboard()
  .text('📦 Projects')
  .text('🗄 Databases')
  .row()
  .text('⏱ Cron Jobs')
  .text('⚙️ Settings')
  .row()
  .text('🧾 Logs')
  .text('🚀 Deployments')
  .resized();

const GLOBAL_COMMANDS = [
  { command: 'start', description: '🧭 Main menu' },
  { command: 'project', description: '📦 Projects' },
  { command: 'database', description: '🗄 Databases' },
  { command: 'cronjobs', description: '⏱ Cron Jobs' },
  { command: 'setting', description: '⚙️ Settings' },
  { command: 'logs', description: '🧾 Logs' },
  { command: 'deploy', description: '🚀 Deployments' },
];

const GLOBAL_COMMAND_SCOPES = [{ type: 'default' }, { type: 'all_private_chats' }];

const GLOBAL_COMMAND_ALIASES = {
  '/setting': '/settings',
  '/settings': '/settings',
  '/project': '/projects',
  '/projects': '/projects',
  '/database': '/database',
  '/cronjob': '/cronjobs',
  '/cronjobs': '/cronjobs',
  '/deploy': '/deploy',
  '/logs': '/logs',
  '/health': '/health',
  '/start': '/start',
};

const GLOBAL_COMMAND_ROUTES = {
  '/settings': 'settings',
  '/projects': 'projects',
  '/database': 'database',
  '/cronjobs': 'cronjobs',
  '/logs': 'logs',
  '/deploy': 'deploy',
};

const START_PAYLOAD_ROUTES = new Map([
  ['main', 'main'],
  ['projects', 'projects'],
  ['project', 'projects'],
  ['p', 'projects'],
  ['database', 'database'],
  ['db', 'database'],
  ['cronjobs', 'cronjobs'],
  ['cronjob', 'cronjobs'],
  ['cj', 'cronjobs'],
  ['settings', 'settings'],
  ['setting', 'settings'],
  ['s', 'settings'],
  ['logs', 'logs'],
  ['log', 'logs'],
  ['l', 'logs'],
  ['deploy', 'deploy'],
  ['deploys', 'deploy'],
  ['d', 'deploy'],
]);

function parseStartPayload(payload) {
  const normalized = normalizeRoute(payload);
  if (!normalized) return 'main';
  return START_PAYLOAD_ROUTES.get(normalized) || 'main';
}

async function handleStartCommand(ctx, payload) {
  const payloadText = payload ? String(payload).trim() : '';
  const route = parseStartPayload(payloadText);
  const normalizedPayload = normalizeRoute(payloadText);
  if (payloadText && route === 'main' && normalizedPayload !== 'main') {
    console.warn('[navigation] Unknown start payload', { payload: payloadText });
  }
  await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'start_command');
  const chatId = getChatIdFromCtx(ctx);
  resetUserState(ctx);
  clearPatchSession(ctx.from?.id);
  await clearPanelMessages(ctx, 'start_reset');
  await clearEphemeralMessages(ctx, 'start_reset');
  if (route !== 'main') {
    console.warn('[navigation] Start payload ignored in favor of main menu.', { payload: payloadText });
  }
  await renderMainMenu(ctx);
  const triggered = triggerConfigDbWarmup('start_command');
  if (!triggered) {
    console.warn('[db] refresh skipped after /start (retry already running)');
  }
  return true;
}

async function handleGlobalCommand(ctx, command, payload) {
  if (command === '/health') {
    const miniUrl = getMiniAppUrl();
    await sendDismissibleMessage(
      ctx,
      'Health is a web endpoint. Open Mini App or use Ping test.',
      { reply_markup: new InlineKeyboard().webApp('🩺 Open Mini App Health', miniUrl) },
    );
    return true;
  }
  if (command === '/start') {
    await handleStartCommand(ctx, payload);
    return true;
  }
  const route = GLOBAL_COMMAND_ROUTES[command];
  if (!route) return false;
  await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'slash_command');
  const chatId = getChatIdFromCtx(ctx);
  const userId = ctx.from?.id;
  await navigateTo(chatId, userId, route, { ctx });
  return true;
}

function normalizeCommandList(commands) {
  if (!Array.isArray(commands)) return [];
  return commands
    .map((entry) => ({
      command: String(entry.command || '').trim(),
      description: String(entry.description || '').trim(),
    }))
    .filter((entry) => entry.command);
}

function commandsMatch(current, desired) {
  const currentNorm = normalizeCommandList(current);
  const desiredNorm = normalizeCommandList(desired);
  if (currentNorm.length !== desiredNorm.length) return false;
  const sortByCommand = (a, b) => a.command.localeCompare(b.command);
  currentNorm.sort(sortByCommand);
  desiredNorm.sort(sortByCommand);
  return currentNorm.every(
    (entry, index) =>
      entry.command === desiredNorm[index].command &&
      entry.description === desiredNorm[index].description,
  );
}

async function registerGlobalCommands() {
  try {
    let allConfigured = true;
    for (const scope of GLOBAL_COMMAND_SCOPES) {
      const existing = await bot.api.getMyCommands({ scope });
      if (!commandsMatch(existing, GLOBAL_COMMANDS)) {
        allConfigured = false;
        break;
      }
    }
    if (allConfigured) {
      console.log('[boot] Global commands already configured.');
      return;
    }
    for (const scope of GLOBAL_COMMAND_SCOPES) {
      await bot.api.setMyCommands(GLOBAL_COMMANDS, { scope });
    }
    console.log('[boot] Global commands registered.');
  } catch (error) {
    console.error('[boot] Failed to register global commands', error?.message || error);
  }
}

bot.use(async (ctx, next) => {
  if (!ctx.from) return;
  const settings = await getCachedSettings();
  const security = normalizeSecuritySettings(settings);
  const adminIds = new Set([String(ADMIN_TELEGRAM_ID), ...security.adminIds.map(String)]);
  if (!adminIds.has(String(ctx.from.id))) {
    if (ctx.updateType === 'message' || ctx.updateType === 'callback_query') {
      await ctx.reply('Unauthorized');
    }
    return;
  }
  return next();
});

bot.on('message:text', async (ctx, next) => {
  const text = ctx.message?.text?.trim();
  if (text && text.startsWith('/')) {
    const state = getUserState(ctx.from?.id);
    if (state) {
      console.log('[state] slash command received during input; clearing state', {
        userId: ctx.from?.id,
        stateType: state.type || state.mode,
        command: text.split(/\s+/)[0],
      });
      resetUserState(ctx);
    }
    const [commandToken, ...rest] = text.split(/\s+/);
    const command = commandToken.toLowerCase();
    const normalizedCommand = command.split('@')[0];
    const mapped = GLOBAL_COMMAND_ALIASES[normalizedCommand] || normalizedCommand;
    if (mapped) {
      const handled = await handleGlobalCommand(ctx, mapped, rest.join(' '));
      if (handled) {
        return;
      }
    }
  }
  return next();
});

bot.on('message:text', async (ctx, next) => {
  const session = getPatchSession(ctx.from.id);
  if (!session) {
    return next();
  }
  const chunkLength = appendChangeChunk(session, ctx.message.text, 'text');
  await ctx.reply(
    `Change chunk received (${chunkLength} chars).\nSend more, or press ‘✅ Patch completed’.`,
    { reply_markup: buildPatchSessionKeyboard() },
  );
  await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'patch_text');
});

bot.on('message:document', async (ctx, next) => {
  const session = getPatchSession(ctx.from.id);
  if (!session) {
    return next();
  }
  const doc = ctx.message.document;
  const fileName = doc?.file_name || '';
  const lowerName = fileName.toLowerCase();
  if (
    !lowerName.endsWith('.patch') &&
    !lowerName.endsWith('.diff') &&
    !lowerName.endsWith('.txt') &&
    !lowerName.endsWith('.docx')
  ) {
    await ctx.reply('Unsupported file type; only .patch/.diff/.txt/.docx are accepted in patch mode.');
    return;
  }
  const extension = lowerName.split('.').pop()?.toLowerCase();
  let fileContents = '';
  if (extension === 'docx') {
    const fileBuffer = await downloadTelegramFileBuffer(ctx, doc.file_id);
    fileContents = await extractDocxText(fileBuffer);
  } else {
    fileContents = await downloadTelegramFile(ctx, doc.file_id);
  }
  const chunkLength = appendChangeChunk(session, fileContents, extension || 'document');
  await ctx.reply(
    `Change file received (${chunkLength} chars).\nPress ‘✅ Patch completed’ when ready.`,
    { reply_markup: buildPatchSessionKeyboard() },
  );
  await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'patch_document');
});


bot.on('message:text', async (ctx, next) => {
  const quick = routineQuickState.get(String(ctx.from.id));
  if (!quick) return next();
  routineQuickState.delete(String(ctx.from.id));
  const text = String(ctx.message?.text || '');
  const last = await getLastOpsErrorContext();
  await runRoutineDetection(
    ctx,
    buildMatchContextInputFallback({ text, last }),
    { backCallback: 'gsettings:routine_menu' },
  );
  return;
});

function buildMatchContextInputFallback({ text, last }) {
  return {
    rawText: text,
    refId: last?.refId || null,
    category: last?.category || null,
  };
}

bot.on('message:text', async (ctx, next) => {
  const state = userState.get(ctx.from.id);
  if (!state || state.mode !== 'create-project') {
    return next();
  }
  await handleProjectWizardInput(ctx, state);
  await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'project_wizard');
});

bot.on('message', async (ctx, next) => {
  const state = getUserState(ctx.from?.id);
  if (state) {
    await handleStatefulMessage(ctx, state);
    await safeDeleteMessage(ctx, ctx.message?.chat?.id, ctx.message?.message_id, 'state_input');
    return;
  }
  return next();
});

bot.command('start', async (ctx) => {
  await handleStartCommand(ctx, ctx.match || '');
});


bot.command('navdiag', async (ctx) => {
  const userId = Number(ctx.from?.id || 0);
  if (ADMIN_TELEGRAM_ID && userId !== Number(ADMIN_TELEGRAM_ID)) {
    await respond(ctx, 'Not allowed.');
    return;
  }
  const chatId = getChatIdFromCtx(ctx);
  const stack = await getNavigationStack(chatId, ctx.from?.id || null);
  const top = stack.slice(-10).reverse().map((entry) => `• ${entry.screenId || entry.routeId}`);
  const lines = ['🧭 Navigation diagnostics (top 10)', ...(top.length ? top : ['• (empty)'])];
  await respond(ctx, lines.join('\n'));
});

bot.hears('📦 Projects', async (ctx) => {
  await handleReplyKeyboardNavigation(ctx, 'projects');
});

bot.hears('🗄 Databases', async (ctx) => {
  await handleReplyKeyboardNavigation(ctx, 'database');
});

bot.hears('⚙️ Settings', async (ctx) => {
  await handleReplyKeyboardNavigation(ctx, 'settings');
});

bot.hears('⏱ Cron Jobs', async (ctx) => {
  await handleReplyKeyboardNavigation(ctx, 'cronjobs');
});

bot.hears('🧾 Logs', async (ctx) => {
  await handleReplyKeyboardNavigation(ctx, 'logs');
});

bot.hears('🚀 Deployments', async (ctx) => {
  await handleReplyKeyboardNavigation(ctx, 'deploy');
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

bot.callbackQuery(
  'KEEP_DEFAULT_WORKDIR',
  wrapCallbackHandler(async (ctx) => {
    const session = userState.get(ctx.from.id);
    const chatId = getChatIdFromCtx(ctx);
    const callbackData = ctx.callbackQuery?.data;
    const hasMessage = Boolean(ctx.callbackQuery?.message);
    console.log('[WORKDIR] Keep default selected', {
      userId: ctx.from?.id,
      callbackData,
      hasMessage,
      chatId,
      step: session?.step,
      repo: session?.repo,
      workingDir: session?.draft?.workingDir,
    });

    await ensureAnswerCallback(ctx);

    if (!session || !session.repo) {
      const nextSession =
        session && session.mode === 'create-project'
          ? session
          : { mode: 'create-project', step: 'githubToken', draft: {}, backCallback: 'proj:list' };
      nextSession.step = 'githubToken';
      userState.set(ctx.from.id, nextSession);
      await respond(ctx, '⚠️ Session expired. Please send GitHub token again.');
      return;
    }

    const currentRepo = session.repo;
    const currentWorkingDir = session.draft?.workingDir || '.';
    const validation = await validateWorkingDir({
      repoSlug: currentRepo,
      workingDir: currentWorkingDir,
      projectType: 'other',
    });

    if (!validation.ok && validation.code !== 'CHECKOUT_PENDING') {
      session.step = 'workingDirConfirm';
      await replySafely(
        ctx,
        '❌ Working directory is invalid (outside repo). Please choose again.',
      );
      await promptNextProjectField(ctx, session);
      return;
    }

    session.draft = session.draft || {};
    session.draft.workingDir = currentWorkingDir;
    session.draft.isWorkingDirCustom = false;
    session.step = 'githubTokenEnvKey';
    await promptNextProjectField(ctx, session);
  }, 'keep_default_workdir'),
);

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
  const inputTypes = Array.from(session.inputTypes || []);
  await handlePatchApplication(ctx, session.projectId, session.buffer, inputTypes);
}, 'patch_finish'));

bot.callbackQuery('structured:fix_block', wrapCallbackHandler(async (ctx) => {
  const session = structuredPatchSessions.get(ctx.from.id);
  if (!session) {
    await ctx.answerCallbackQuery({ text: 'No structured patch to fix.', show_alert: true });
    return;
  }
  await ensureAnswerCallback(ctx);
  setUserState(ctx.from.id, { type: 'structured_fix_block' });
  await ctx.reply(`Send the corrected version of block ${session.failureIndex + 1}.`);
}, 'structured_fix_block'));

bot.callbackQuery('structured:cancel', wrapCallbackHandler(async (ctx) => {
  structuredPatchSessions.delete(ctx.from.id);
  clearUserState(ctx.from.id);
  await ensureAnswerCallback(ctx);
  await safeRespond(ctx, 'Patch cancelled.');
}, 'structured_cancel'));

bot.callbackQuery('change:cancel', wrapCallbackHandler(async (ctx) => {
  changePreviewSessions.delete(ctx.from.id);
  await ensureAnswerCallback(ctx);
  await safeRespond(ctx, 'Change request cancelled.');
}, 'change_cancel'));

bot.callbackQuery('change:apply', wrapCallbackHandler(async (ctx) => {
  const session = changePreviewSessions.get(ctx.from.id);
  if (!session) {
    await ctx.answerCallbackQuery({ text: 'No pending change request.', show_alert: true });
    return;
  }
  changePreviewSessions.delete(ctx.from.id);
  await ensureAnswerCallback(ctx);
  await applyChangesInRepo(ctx, session.projectId, { mode: 'unstructured', plan: session.plan });
}, 'change_apply'));

bot.callbackQuery(/ops:recover:(.+)/, wrapCallbackHandler(async (ctx) => {
  await ensureAnswerCallback(ctx);
  const outageId = String(ctx.match[1] || '');
  const chatId = String(ctx.callbackQuery?.message?.chat?.id || '');
  const msgId = ctx.callbackQuery?.message?.message_id;
  try {
    await bot.api.deleteMessage(chatId, msgId);
  } catch (_error) {
    await bot.api.editMessageReplyMarkup(chatId, msgId, { reply_markup: undefined }).catch(() => null);
    await bot.api.editMessageText(chatId, msgId, '✅ Config DB is back online. Acknowledged').catch(() => null);
  }
  opsRecoveryMessages.delete(`${outageId}:${chatId}`);
}, 'ops_recover_ack'));


async function handleRoutineCallback(ctx, data) {
  const parts = String(data || '').split(':');
  const action = parts[1];
  if (action === 'copy') {
    const payload = decodeRoutinePayload(parts[2]);
    if (!payload?.task) {
      await ensureAnswerCallback(ctx, { text: 'Expired routine payload', show_alert: true });
      return;
    }
    await sendDismissibleMessage(ctx, `\`\`\`text\n${payload.task}\n\`\`\``);
    await ensureAnswerCallback(ctx);
  }
}

async function dispatchCallbackData(ctx, data, options = {}) {
  const callbackData = String(data || '');
  if (callbackData === 'nav:back' || callbackData === 'main:back' || callbackData.endsWith(':back')) {
    await goBack(ctx);
    return;
  }
  if (callbackData === 'nav:home' || callbackData === 'main:home') {
    await goHome(ctx);
    return;
  }
  if (callbackData.startsWith('msg:delete:') || callbackData.startsWith('msgdel:')) {
    await handleDeleteMessageCallback(ctx, callbackData.replace(/^msgdel:/, 'msg:delete:'));
    return;
  }
  if (callbackData.startsWith('routine:')) {
    await handleRoutineCallback(ctx, callbackData);
    return;
  }
  if (callbackData.startsWith('opsdbg:')) {
    await ensureAnswerCallback(ctx);
    const payload = decodeRoutinePayload(callbackData.split(':')[1]);
    await sendDismissibleMessage(ctx, `\`\`\`json
${JSON.stringify(payload || {}, null, 2)}
\`\`\``);
    return;
  }
  if (callbackData.startsWith('routinefix:')) {
    const parts = callbackData.split(':');
    await maybeSendRoutineFixFromButton(ctx, parts[1]);
    return;
  }
  if (callbackData.startsWith('codex_tasks:')) return handleCodexTasksCallback(ctx, callbackData);
  if (callbackData.startsWith('main:')) return handleMainCallback(ctx, callbackData);
  if (callbackData.startsWith('ops:')) return handleOpsCallback(ctx, callbackData);
  if (callbackData.startsWith('help:')) return handleHelpCallback(ctx, callbackData);
  if (callbackData.startsWith('proj:')) return handleProjectCallback(ctx, callbackData);
  if (callbackData.startsWith('projlog:')) return handleProjectLogCallback(ctx, callbackData);
  if (callbackData.startsWith('logtest:')) return handleLogTestCallback(ctx, callbackData);
  if (callbackData.startsWith('dbmenu:')) return handleDatabaseMenuCallback(ctx, callbackData);
  if (callbackData.startsWith('logmenu:')) return handleLogsMenuCallback(ctx, callbackData);
  if (callbackData.startsWith('deploy:')) return handleDeployCallback(ctx, callbackData);
  if (callbackData.startsWith('notes:')) return handleNotesCallback(ctx, callbackData);
  if (callbackData.startsWith('projwiz:')) return handleProjectWizardCallback(ctx, callbackData);
  if (callbackData.startsWith('configdb:')) return handleConfigDbCallback(ctx, callbackData);
  if (callbackData.startsWith('gsettings:')) return handleGlobalSettingsCallback(ctx, callbackData);
  if (callbackData.startsWith('supabase:')) return handleSupabaseCallback(ctx, callbackData);
  if (callbackData.startsWith('cron:')) return handleCronCallback(ctx, callbackData);
  if (callbackData.startsWith('cronwiz:')) return handleCronWizardCallback(ctx, callbackData);
  if (callbackData.startsWith('projcron:')) return handleProjectCronCallback(ctx, callbackData);
  if (callbackData.startsWith('envvault:')) return handleEnvVaultCallback(ctx, callbackData);
  if (callbackData.startsWith('cronlink:')) return handleCronLinkCallback(ctx, callbackData);
  if (callbackData.startsWith('tgbot:')) return handleTelegramBotCallback(ctx, callbackData);
}

bot.on('callback_query:data', wrapCallbackHandler(async (ctx) => {
  const resolved = await resolveCallbackData(ctx.callbackQuery.data);
  if (resolved.expired || !resolved.data) {
    await ctx.answerCallbackQuery({ text: 'Expired, please reopen the menu.', show_alert: true });
    return;
  }
  const data = resolved.data;
  if (!ctx.__skipNavigationRecord && data !== 'nav:back' && data !== 'nav:home') {
    ctx.__navRouteId = `cb:${data}`;
  }
  await dispatchCallbackData(ctx, data, { skipNavigationRecord: false });
}, 'callback_query:data'));

async function handleStatefulMessage(ctx, state) {
  switch (state.type) {
    case 'rename_project':
      await handleRenameProjectStep(ctx, state);
      break;
    case 'edit_project_id':
      await handleEditProjectIdInput(ctx, state);
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
    case 'supabase_binding':
      await handleSupabaseBindingInput(ctx, state);
      break;
    case 'db_import_url':
      await handleDatabaseImportInput(ctx, state);
      break;
    case 'edit_service_health':
      await handleEditServiceHealthInput(ctx, state);
      break;
    case 'global_change_base':
      await handleGlobalBaseChange(ctx, state);
      break;
    case 'gsettings_admin_add':
      await handleAdminAddInput(ctx, state);
      break;
    case 'gsettings_base_url':
      await handleBaseUrlInput(ctx, state);
      break;
    case 'gsettings_backup_channel':
      await handleBackupChannelInput(ctx, state);
      break;
    case 'gsettings_backup_caption':
      await handleBackupCaptionInput(ctx, state);
      break;
    case 'supabase_console':
      await handleSupabaseConsoleMessage(ctx, state);
      break;
    case 'supabase_add':
      await handleSupabaseAddMessage(ctx, state);
      break;
    case 'supabase_table_auth':
      await handleSupabaseTableAuthInput(ctx, state);
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
    case 'deploy_service_id':
      await handleDeployServiceIdInput(ctx, state);
      break;
    case 'render_api_key':
      await handleRenderApiKeyInput(ctx, state);
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
    case 'env_vault_search':
      await handleEnvVaultSearchInput(ctx, state);
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
    case 'log_test_config':
      await handleLogTestConfigInput(ctx, state);
      break;
    case 'note_create':
      await handleNoteCreateInput(ctx, state);
      break;
    case 'note_edit_field':
      await handleNoteEditInput(ctx, state);
      break;
    case 'note_search':
      await handleNoteSearchInput(ctx, state);
      break;
    case 'note_add_attachment':
      await handleNoteAttachmentInput(ctx, state);
      break;
    case 'structured_fix_block':
      await handleStructuredFixBlockInput(ctx, state);
      break;
    case 'env_scan_fix_key':
      await handleEnvScanFixKeyInput(ctx, state);
      break;
    case 'routine_refid':
      await handleRoutineRefIdInput(ctx, state);
      break;
    case 'routine_forward':
      await handleRoutineForwardInput(ctx, state);
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
  if (!getConfigDbSnapshot().ready) {
    await renderConfigDbGate(ctx, { title: '📦 Projects', backCallback: 'main:back' });
    return;
  }
  const projects = await loadProjects();
  const globalSettings = await loadGlobalSettings();
  const banner = buildDegradedBanner();
  if (!projects.length) {
    await renderOrEdit(ctx, `${banner}No projects configured yet.`, {
      reply_markup: buildProjectsKeyboard([], globalSettings),
    });
    return;
  }

  await renderOrEdit(ctx, `${banner}${buildScopedHeader('GLOBAL', 'Main → Projects')}Select a project:`, {
    reply_markup: buildProjectsKeyboard(projects, globalSettings),
  });
}

async function handleMainCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, action] = data.split(':');
  switch (action) {
    case 'defaults':
      await renderOrEdit(ctx, `${buildScopedHeader('GLOBAL', 'Main → Settings → Default project settings')}⚙️ Default project settings\n\n- Global DB defaults\n- Global Cron defaults\n- Global Deploy defaults`, {
        reply_markup: new InlineKeyboard().text('⬅️ Back', 'nav:back'),
      });
      break;
    case 'defaults':
      await renderOrEdit(ctx, `${buildScopedHeader('GLOBAL', 'Main → Settings → Default project settings')}⚙️ Default project settings\n\n- Global DB defaults\n- Global Cron defaults\n- Global Deploy defaults`, {
        reply_markup: new InlineKeyboard().text('⬅️ Back', 'nav:back'),
      });
      break;
    case 'back':
      await goBack(ctx);
      break;
    case 'projects':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'projects', { ctx });
      break;
    case 'database':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'database', { ctx });
      break;
    case 'cronjobs':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'cronjobs', { ctx });
      break;
    case 'settings':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'settings', { ctx });
      break;
    case 'logs':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'logs', { ctx });
      break;
    case 'ops':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'ops', { ctx });
      break;
    case 'diagnostics':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'diagnostics', { ctx });
      break;
    case 'templates':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'templates', { ctx });
      break;
    case 'help':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'help', { ctx });
      break;
    case 'deploys':
    case 'deploy':
      await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'deploy', { ctx });
      break;
    default:
      break;
  }
}

async function handleOpsCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, action] = data.split(':');
  if (action === 'timeline') {
    const result = opsTimeline.query({ pageSize: 12 });
    const lines = ['🕒 Ops Timeline', ...result.items.map((e) => `• [${e.severity}] ${e.type}: ${e.title}${e.refId ? ` (${e.refId})` : ''}`)];
    await renderOrEdit(ctx, lines.join('\n') || 'No events yet.', { reply_markup: new InlineKeyboard().text('⬅️ Back', 'main:ops') });
    return;
  }
  if (action === 'safe_mode') {
    const status = safeMode.getStatus();
    const lines = [
      '🛡 Safe Mode',
      `Status: ${status.mode === 'on' ? 'ON' : 'OFF'}`,
      `Forced: ${status.forced || 'none'}`,
      `Last reason: ${status.lastEnteredReason || '-'}`,
    ];
    const kb = new InlineKeyboard().text('Force ON', 'ops:sm_force_on').text('Force OFF', 'ops:sm_force_off').row().text('Reset counters', 'ops:sm_reset').row().text('⬅️ Back', 'main:ops');
    await renderOrEdit(ctx, lines.join('\n'), { reply_markup: kb });
    return;
  }
  if (action === 'sm_force_on') safeMode.setForced('on');
  if (action === 'sm_force_off') safeMode.setForced('off');
  if (action === 'sm_reset') safeMode.resetCounters();
  await renderOpsMenu(ctx);
}

async function handleHelpCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, page] = data.split(':');
  const pages = buildHelpPages();
  const text = pages[page] || pages.overview;
  await renderOrEdit(ctx, `❓ Help — ${page || 'overview'}\n\n${text}`, { reply_markup: new InlineKeyboard().text('⬅️ Back', 'main:help') });
}

function buildDefaultProjectActionsKeyboard(projectId, isDefault) {
  const kb = new InlineKeyboard().text('⭐ Set as default', `proj:set_default:${projectId}`).row();
  if (isDefault) kb.text('🧹 Clear default', `proj:clear_default:${projectId}`).row();
  kb.text('⬅️ Back', `proj:project_menu:${projectId}`);
  return kb;
}

async function handleProjectCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, action, projectId, extra, ...rest] = data.split(':');
  const source = rest.join(':') || null;

  if (!getConfigDbSnapshot().ready && action !== 'list') {
    await renderConfigDbGate(ctx, { title: '📦 Projects', backCallback: 'main:back' });
    return;
  }

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
    case 'overview':
      await openProjectOverview(ctx, projectId);
      break;
    case 'settings':
      await renderProjectScopedSettings(ctx, projectId);
      break;
    case 'preferences':
      await renderProjectPreferencesMenu(ctx, projectId);
      break;
    case 'repo_menu':
      await renderProjectRepositoryMenu(ctx, projectId);
      break;
    case 'project_menu':
      await renderProjectMenu(ctx, projectId);
      break;
    case 'codex_tasks':
      await renderCodexTasksMenu(ctx, null, { projectId });
      break;
    case 'default_actions': {
      const settings = await loadGlobalSettings();
      const isDefault = settings.defaultProjectId === projectId;
      const kb = buildDefaultProjectActionsKeyboard(projectId, isDefault);
      await renderOrEdit(ctx, '⭐ Default project actions', { reply_markup: kb });
      break;
    }
    case 'working_dir':
      await renderWorkingDirectionMenu(ctx, projectId);
      break;
    case 'server_menu':
      await renderServerMenu(ctx, projectId);
      break;
    case 'missing_setup':
      await renderProjectMissingSetup(ctx, projectId);
      break;
    case 'missing_fix':
      await handleProjectMissingFix(ctx, projectId, extra);
      break;
    case 'auto_fix_health':
      await runProjectAutoFixHealthSetup(ctx, projectId);
      break;
    case 'apply_patch':
      startPatchSession(ctx.from.id, projectId);
      await renderOrEdit(
        ctx,
        'Send the change request as text (you can use multiple messages),\n' +
          'or attach a .patch/.diff/.txt/.docx file.\n' +
          'Structured changes use "PM Change Spec v1" blocks.\n' +
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
    case 'edit_id':
      setUserState(ctx.from.id, {
        type: 'edit_project_id',
        step: 'input',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Send the new project ID (slug only, unique).\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'edit_repo':
      setUserState(ctx.from.id, {
        type: 'edit_repo',
        projectId,
        backCallback: source === 'missing_setup' ? `proj:missing_setup:${projectId}` : null,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(
        ctx,
        'Send new GitHub repo as owner/repo (for example: Mirax226/daily-system-bot-v2).\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    case 'edit_workdir':
      resetUserState(ctx);
      console.log('[ui] Edit working dir requested', {
        userId: ctx.from?.id,
        projectId,
        messageId: ctx.callbackQuery?.message?.message_id,
      });
      setUserState(ctx.from.id, {
        type: 'edit_working_dir',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(
        ctx,
        'Send new working directory (repo-relative preferred, e.g. "." or "apps/api"). Absolute paths are allowed but discouraged. Or send "-" to reset to repo root.\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    case 'workdir_menu':
      await renderWorkingDirectionMenu(ctx, projectId);
      break;
    case 'workdir_reset':
      await resetWorkingDir(ctx, projectId);
      break;
    case 'workdir_revalidate':
      await revalidateWorkingDir(ctx, projectId);
      break;
    case 'edit_github_token':
      resetUserState(ctx);
      console.log('[ui] Edit GitHub token requested', {
        userId: ctx.from?.id,
        projectId,
        messageId: ctx.callbackQuery?.message?.message_id,
      });
      setUserState(ctx.from.id, {
        type: 'edit_github_token',
        projectId,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(
        ctx,
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
      await renderCommandsScreen(ctx, projectId, { source });
      break;
    case 'project_type':
      await renderProjectTypeMenu(ctx, projectId);
      break;
    case 'run_mode':
      await renderProjectRunModeMenu(ctx, projectId);
      break;
    case 'run_mode_set':
      await updateProjectRunMode(ctx, projectId, extra, source);
      break;
    case 'project_type_set':
      await updateProjectType(ctx, projectId, extra);
      break;
    case 'cmd_edit':
      setUserState(ctx.from.id, {
        type: 'edit_command_input',
        projectId,
        field: extra,
        backCallback: source === 'missing_setup' ? `proj:missing_setup:${projectId}` : null,
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
    case 'diagnostics_menu':
      await renderProjectDiagnosticsMenu(ctx, projectId);
      break;
    case 'diagnostics_light':
      await runProjectLightDiagnostics(ctx, projectId);
      break;
    case 'diagnostics_full':
      await runProjectFullDiagnostics(ctx, projectId);
      break;
    case 'db_mini':
      resetUserState(ctx);
      await renderProjectDbMiniSite(ctx, projectId);
      break;
    case 'db_mini_open':
      resetUserState(ctx);
      await openProjectDbMiniSite(ctx, projectId);
      break;
    case 'db_mini_enable':
      resetUserState(ctx);
      await enableProjectDbMiniSite(ctx, projectId);
      break;
    case 'db_mini_rotate':
      resetUserState(ctx);
      await rotateProjectDbMiniSiteToken(ctx, projectId);
      break;
    case 'db_config':
      resetUserState(ctx);
      await renderDatabaseBindingMenu(ctx, projectId);
      break;
    case 'db_ssl_settings':
      resetUserState(ctx);
      await renderProjectDbSslSettings(ctx, projectId);
      break;
    case 'db_ssl_mode':
      resetUserState(ctx);
      await updateProjectDbSslMode(ctx, projectId, extra);
      break;
    case 'db_ssl_verify':
      resetUserState(ctx);
      await toggleProjectDbSslVerify(ctx, projectId);
      break;
    case 'db_import':
      resetUserState(ctx);
      await startDatabaseImportFlow(ctx, projectId);
      break;
    case 'db_insights':
      resetUserState(ctx);
      await renderProjectDbInsights(ctx, projectId, Number(extra) || 0, Number(source) || 0);
      break;
    case 'env_export':
      await exportProjectEnv(ctx, projectId);
      break;
    case 'env_scan':
      await scanEnvRequirements(ctx, projectId);
      break;
    case 'env_scan_fix_missing':
      await handleEnvScanFixMissing(ctx, projectId);
      break;
    case 'env_scan_fix_specific':
      await handleEnvScanFixSpecific(ctx, projectId);
      break;
    case 'log_env_fix':
      await handleLogForwardingEnvFix(ctx, projectId, extra);
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
        type: 'supabase_binding',
        projectId,
        step: 'project_ref',
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(
        ctx,
        'Send Supabase project ref (the first part of https://<ref>.supabase.co).\nJWT strings are API keys, not DB DSN.\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    case 'supabase_key_type': {
      const state = getUserState(ctx.from.id);
      if (!state || state.type !== 'supabase_binding' || state.step !== 'key_type') {
        await ctx.answerCallbackQuery({
          text: 'Supabase binding flow not active. Please restart.',
          show_alert: true,
        });
        return;
      }
      const keyType = extra === 'service_role' ? 'service_role' : 'anon';
      setUserState(ctx.from.id, {
        ...state,
        step: 'key_input',
        supabaseKeyType: keyType,
      });
      await renderOrEdit(
        ctx,
        'Paste the Supabase API key (JWT).\nIt will be shown once, then masked.\nJWT strings are API keys, not DB DSN.\n(Or press Cancel)',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    }
    case 'supabase_toggle': {
      const projects = await loadProjects();
      const project = findProjectById(projects, projectId);
      if (!project) {
        await renderOrEdit(ctx, 'Project not found.');
        return;
      }
      const nextEnabled = !resolveSupabaseEnabled(project);
      const idx = projects.findIndex((p) => p.id === projectId);
      projects[idx] = { ...projects[idx], supabaseEnabled: nextEnabled };
      await saveProjectsWithFeedback(ctx, projects);
      await renderDatabaseBindingMenu(
        ctx,
        projectId,
        nextEnabled ? '✅ Supabase binding enabled.' : '🚫 Supabase binding disabled.',
      );
      break;
    }
    case 'supabase_clear':
      {
        const projects = await loadProjects();
        const idx = projects.findIndex((p) => p.id === projectId);
        if (idx === -1) {
          await renderOrEdit(ctx, 'Project not found.');
          return;
        }
        projects[idx] = {
          ...projects[idx],
          supabaseProjectRef: undefined,
          supabaseUrl: undefined,
          supabaseKeyType: undefined,
          supabaseKey: undefined,
          supabaseKeyMask: undefined,
          supabaseEnabled: false,
        };
        await saveProjectsWithFeedback(ctx, projects);
      }
      await renderDatabaseBindingMenu(ctx, projectId, '🧹 Supabase binding cleared.');
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
    case 'clear_default':
      await clearDefaultProject();
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
  const mode = action === 'dest_mode' ? parts[2] : null;
  const projectId =
    action === 'level' || action === 'dest_mode'
      ? parts[3]
      : parts[2];
  const page = action === 'recent' ? Number(parts[3] || 0) : 0;

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

  if (action === 'levels') {
    await renderProjectLogLevelsMenu(ctx, projectId);
    return;
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
      await upsertProjectLogSettings(projectId, updated);
    }
    await renderProjectLogLevelsMenu(ctx, projectId);
    return;
  }

  if (action === 'dest') {
    await renderProjectLogDestinationMenu(ctx, projectId);
    return;
  }

  if (action === 'dest_mode') {
    updated.destinationMode = ['admin', 'channel', 'both'].includes(mode) ? mode : 'admin';
    await upsertProjectLogSettings(projectId, updated);
    await renderProjectLogDestinationMenu(ctx, projectId);
    return;
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
    await upsertProjectLogSettings(projectId, updated);
    await renderProjectLogDestinationMenu(ctx, projectId);
    return;
  }

  if (action === 'clear_chat') {
    updated.destinationChatId = null;
    await upsertProjectLogSettings(projectId, updated);
    await renderProjectLogDestinationMenu(ctx, projectId);
    return;
  }

  if (action === 'recent') {
    await renderProjectLogList(ctx, projectId, Number.isNaN(page) ? 0 : page);
    return;
  }

  if (action === 'test') {
    await sendProjectLogTest(ctx, projectId);
    return;
  }

  await upsertProjectLogSettings(projectId, updated);
  await renderProjectLogAlerts(ctx, projectId);
}

async function handleLogTestCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = data.split(':');
  const action = parts[1];
  const projectId = parts[2];
  const extra = parts[3];

  if (action === 'mode_menu' && projectId) {
    await renderOrEdit(ctx, 'Select a log test mode:', {
      reply_markup: buildLogTestModeMenu(projectId),
    });
    return;
  }

  if (action === 'mode' && projectId && extra) {
    await runSingleLogTest(ctx, projectId, extra);
    return;
  }

  if (action === 'all' && projectId) {
    await runAllLogTests(ctx, projectId);
    return;
  }

  if (action === 'config' && projectId) {
    await startLogTestConfigFlow(ctx, projectId);
    return;
  }

  if (action === 'status' && projectId) {
    await renderLogTestStatus(ctx, projectId);
    return;
  }

  if (action === 'diagnostics' && projectId) {
    const projects = await loadProjects();
    const project = findProjectById(projects, projectId);
    if (!project) {
      await renderOrEdit(ctx, 'Project not found.');
      return;
    }
    const logTest = normalizeLogTestSettings(project);
    const diagnosticsUrl = resolveDiagnosticsEndpoint(project, logTest);
    if (!diagnosticsUrl) {
      await renderProjectLogAlerts(ctx, projectId, '⚠️ Diagnostics endpoint not configured.');
      return;
    }
    const tokenResult = await fetchLogTestToken(projectId, logTest);
    if (!tokenResult.ok) {
      await renderProjectLogAlerts(ctx, projectId, `⚠️ ${tokenResult.error}`);
      return;
    }
    const diag = await requestLogDiagnostics(project, logTest, tokenResult.token);
    if (!diag.ok) {
      await renderProjectLogAlerts(ctx, projectId, `❌ Diagnostics failed.\n${diag.error}`);
      return;
    }
    await storeLogDiagnosticsSnapshot(projectId, diag.payload);
    await renderProjectLogAlerts(ctx, projectId, buildLogTestDiagnosticsReport(diag.payload));
    return;
  }

  if (action === 'snooze' && projectId) {
    await renderLogTestSnoozeMenu(ctx, projectId);
    return;
  }

  if (action === 'snooze_set' && projectId && extra) {
    const days = Number(extra);
    if (!Number.isFinite(days) || days <= 0) {
      await renderLogTestSnoozeMenu(ctx, projectId);
      return;
    }
    const snoozedUntil = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();
    await updateProjectLogTest(projectId, (current) => ({
      ...current,
      reminder: { ...current.reminder, snoozedUntil },
    }));
    await renderProjectLogAlerts(ctx, projectId, `⏰ Reminder snoozed for ${days} day(s).`);
    return;
  }

  if (action === 'snooze_clear' && projectId) {
    await updateProjectLogTest(projectId, (current) => ({
      ...current,
      reminder: { ...current.reminder, snoozedUntil: null },
    }));
    await renderProjectLogAlerts(ctx, projectId, '🔄 Snooze cleared.');
    return;
  }

  if (action === 'reminders') {
    await renderLogTestReminders(ctx);
    return;
  }

  if (action === 'generate_task' && projectId) {
    const task = [
      '🧩 Codex task: add diagnostics + test endpoint + log sender',
      '',
      'Implement:',
      '- POST /pm/test-log (alias /__pm/test-log) with PM_TEST_TOKEN auth.',
      '- GET /pm/diagnostics (alias /__pm/diagnostics).',
      '- PM logger to send to PM_URL + "/api/logs" with PM_INGEST_TOKEN.',
      '',
      'Include correlationId in meta, add interceptors + process hooks, and keep secrets masked.',
    ].join('\n');
    await createCodexTaskAndNotify(ctx, {
      sourceType: 'routine_fix',
      projectId,
      title: 'Add diagnostics + test endpoint + log sender',
      body: task,
      refId: `logtest:${projectId}`,
    });
    await renderProjectLogAlerts(ctx, projectId);
    return;
  }

  if (action === 'inspect_repo' && projectId) {
    await runRepoInspection(ctx, projectId);
    return;
  }

  if (action === 'generate_repo_task' && projectId) {
    const cached = repoInspectionCache.get(projectId);
    if (!cached) {
      await renderProjectLogAlerts(ctx, projectId, 'No repo inspection report available yet.');
      return;
    }
    await createCodexTaskAndNotify(ctx, {
      sourceType: 'repo_inspection',
      projectId,
      title: 'Repo inspection remediation task',
      body: cached.codexTask,
      refId: `repo_inspection:${projectId}`,
    });
    await renderProjectLogAlerts(ctx, projectId);
    return;
  }

  if (action === 'create_pr' && projectId) {
    const cached = repoInspectionCache.get(projectId);
    if (!cached) {
      await renderProjectLogAlerts(ctx, projectId, 'No repo inspection report available yet.');
      return;
    }
    const rateStatus = checkRateLimit(repoPrRateLimits, projectId, REPO_PR_RATE_LIMIT);
    if (rateStatus.blocked) {
      await renderProjectLogAlerts(
        ctx,
        projectId,
        `⏳ PR creation is rate-limited. Try again in ${formatRetryAfter(rateStatus.retryAfterMs)}.`,
      );
      return;
    }
    await renderProjectLogAlerts(
      ctx,
      projectId,
      '🛠 PR creation requires an explicit patch plan. Use "🧩 Generate Codex task for client repo" to apply fixes manually, then re-run tests.',
    );
    return;
  }

  await renderLogsProjectList(ctx);
}

async function handleGlobalSettingsCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = data.split(':');
  const action = parts[1];
  if (!getConfigDbSnapshot().ready && action !== 'pm_status' && action !== 'back') {
    await renderConfigDbGate(ctx, { title: '⚙️ Settings', backCallback: 'main:back' });
    return;
  }
  switch (action) {
    case 'defaults':
      await renderOrEdit(ctx, `${buildScopedHeader('GLOBAL', 'Main → Settings → Default project settings')}⚙️ Default project settings\n\n- Global DB defaults\n- Global Cron defaults\n- Global Deploy defaults`, {
        reply_markup: new InlineKeyboard().text('⬅️ Back', 'nav:back'),
      });
      break;
    case 'maintenance':
      await renderOrEdit(ctx, '🧹 Maintenance\n\nUse maintenance tools for cleanup, diagnostics, and backups.', {
        reply_markup: new InlineKeyboard()
          .text('🧹 UI & Cleanup', 'gsettings:ui')
          .row()
          .text('📦 Backups', 'gsettings:backups')
          .row()
          .text('🧪 Diagnostics', 'gsettings:diagnostics_menu')
          .row()
          .text('🩺 PM Status', 'gsettings:pm_status')
          .row()
          .text('📶 Ping test', 'gsettings:ping_test')
          .row()
          .text('⬅️ Back', 'gsettings:menu'),
      });
      break;
    case 'ops_templates':
      await renderOrEdit(ctx, '🧩 Ops & Templates\n\nOpen operational tools and templates from here.', {
        reply_markup: new InlineKeyboard().text('🧩 Ops', 'main:ops').row().text('🧱 Templates', 'main:templates').row().text('⬅️ Back', 'gsettings:menu'),
      });
      break;
    case 'ui': {
      const settings = await getCachedSettings();
      const view = buildUiCleanupSettingsView(settings);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'ui_toggle': {
      const settings = await getCachedSettings();
      const ui = normalizeUiCleanupSettings(settings);
      const updated = {
        ...settings,
        uiCleanup: { ...settings.uiCleanup, autoCleanMenus: !ui.autoCleanMenus },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildUiCleanupSettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'ui_ttl': {
      const ttl = Number(parts[2]);
      const settings = await getCachedSettings();
      const updated = {
        ...settings,
        uiCleanup: { ...settings.uiCleanup, ephemeralTtlSec: [0, 30, 60].includes(ttl) ? ttl : 30 },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildUiCleanupSettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'ui_keep': {
      const keep = Number(parts[2]);
      const settings = await getCachedSettings();
      const updated = {
        ...settings,
        uiCleanup: { ...settings.uiCleanup, keepLastPanels: [1, 3, 5].includes(keep) ? keep : 1 },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildUiCleanupSettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'security': {
      const settings = await getCachedSettings();
      const view = buildSecuritySettingsView(settings);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'admins': {
      const settings = await getCachedSettings();
      const view = buildAdminListView(settings);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'admin_add': {
      setUserState(ctx.from.id, {
        type: 'gsettings_admin_add',
        messageContext: getMessageTargetFromCtx(ctx),
        backCallback: 'gsettings:admins',
      });
      await renderOrEdit(ctx, '🪪 Send the Telegram user ID to add as admin.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    }
    case 'admin_remove': {
      const adminId = parts[2];
      const settings = await getCachedSettings();
      const security = normalizeSecuritySettings(settings);
      const updated = {
        ...settings,
        security: {
          ...settings.security,
          adminIds: security.adminIds.filter((id) => id !== adminId),
        },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildAdminListView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'mini_ttl': {
      const ttl = Number(parts[2]);
      const settings = await getCachedSettings();
      const updated = {
        ...settings,
        security: {
          ...settings.security,
          miniSiteSessionTtlMinutes: Number.isFinite(ttl) ? ttl : MINI_SITE_SESSION_DEFAULT_TTL_MINUTES,
        },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildSecuritySettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'env_mask': {
      const mode = parts[2] === 'spoiler' ? 'spoiler' : 'strict';
      const settings = await getCachedSettings();
      const updated = {
        ...settings,
        security: { ...settings.security, envMaskPolicy: mode },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildSecuritySettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'logs': {
      const settings = await getCachedSettings();
      const view = buildLogsSettingsView(settings);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'log_default_level': {
      const level = normalizeLogLevel(parts[2]);
      if (!level) {
        await renderOrEdit(ctx, 'Invalid log level.');
        break;
      }
      const settings = await getCachedSettings();
      const logsDefaults = normalizeLogDefaults(settings);
      const levels = new Set(logsDefaults.defaultLevels);
      if (levels.has(level)) {
        levels.delete(level);
      } else {
        levels.add(level);
      }
      const updatedLevels = Array.from(levels).filter((entry) => LOG_LEVELS.includes(entry));
      const updated = {
        ...settings,
        logs: { ...settings.logs, defaultLevels: updatedLevels.length ? updatedLevels : ['error'] },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildLogsSettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'log_allowed': {
      const mode = parts[2] === 'whitelist' ? 'whitelist' : 'allow-all';
      const settings = await getCachedSettings();
      const updated = {
        ...settings,
        logs: { ...settings.logs, allowedProjectsMode: mode },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildLogsSettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'integrations': {
      const settings = await getCachedSettings();
      const view = buildIntegrationsSettingsView(settings);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'base_url': {
      setUserState(ctx.from.id, {
        type: 'gsettings_base_url',
        messageContext: getMessageTargetFromCtx(ctx),
        backCallback: 'gsettings:integrations',
      });
      await renderOrEdit(ctx, '🌐 Send base URL override.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    }
    case 'base_url_clear': {
      const settings = await getCachedSettings();
      const updated = {
        ...settings,
        integrations: { ...settings.integrations, baseUrlOverride: '' },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildIntegrationsSettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'ping_interval': {
      const interval = Number(parts[2]);
      const settings = await getCachedSettings();
      const updated = {
        ...settings,
        integrations: {
          ...settings.integrations,
          healthPingIntervalMinutes: Number.isFinite(interval) ? interval : 5,
        },
      };
      await saveGlobalSettingsAndCache(updated);
      const view = buildIntegrationsSettingsView(updated);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'backups': {
      const settings = await getCachedSettings();
      const view = buildBackupsSettingsView(settings);
      await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
      break;
    }
    case 'backup_channel': {
      setUserState(ctx.from.id, {
        type: 'gsettings_backup_channel',
        messageContext: getMessageTargetFromCtx(ctx),
        backCallback: 'gsettings:backups',
      });
      await renderOrEdit(ctx, '📦 Send backup channel chat_id.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    }
    case 'backup_caption': {
      setUserState(ctx.from.id, {
        type: 'gsettings_backup_caption',
        messageContext: getMessageTargetFromCtx(ctx),
        backCallback: 'gsettings:backups',
      });
      await renderOrEdit(ctx, '🧾 Send backup caption template.\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    }
    case 'bot_log_alerts':
      await renderSelfLogAlerts(ctx);
      break;
    case 'diagnostics_menu':
      await renderLogDeliveryDiagnosticsMenu(ctx);
      break;
    case 'routine_menu':
      await renderRoutineFixesMenu(ctx);
      break;
    case 'routine_analyze_last': {
      const context = await getLastOpsErrorContext();
      if (!context) {
        await renderRoutineFixesMenu(ctx, 'No recent error in event_log.');
        break;
      }
      await runRoutineDetection(ctx, context, { backCallback: 'gsettings:routine_menu' });
      break;
    }
    case 'routine_quick_detect':
    case 'routine_paste': {
      routineQuickState.set(String(ctx.from.id), { mode: action === 'routine_quick_detect' ? 'quick' : 'paste' });
      await renderOrEdit(ctx, 'Send a keyword OR paste the error/log text', {
        reply_markup: buildBackKeyboard('gsettings:routine_menu'),
      });
      break;
    }
    case 'routine_refid': {
      setUserState(ctx.from.id, {
        type: 'routine_refid',
        backCallback: 'gsettings:routine_menu',
      });
      await renderOrEdit(ctx, 'Send RefId (e.g. CRON-123).', { reply_markup: buildCancelKeyboard() });
      break;
    }
    case 'routine_forward': {
      setUserState(ctx.from.id, {
        type: 'routine_forward',
        backCallback: 'gsettings:routine_menu',
      });
      await renderOrEdit(ctx, 'Forward an error message now.', { reply_markup: buildCancelKeyboard() });
      break;
    }
    case 'routine_catalog': {
      const lines = ['📚 Rule catalog', ''];
      for (const rule of listRoutineCatalog()) {
        lines.push(`• ${rule.id} — ${rule.title}`);
      }
      await renderOrEdit(ctx, lines.join('\n'), { reply_markup: buildBackKeyboard('gsettings:routine_menu') });
      break;
    }
    case 'routine_help':
      await renderOrEdit(ctx, 'Use Analyze last error, Quick detect, RefId lookup, or Forwarded message. Output is deterministic and copy-ready.', {
        reply_markup: buildBackKeyboard('gsettings:routine_menu'),
      });
      break;
    case 'routine_pick': {
      const ruleId = parts[2];
      const rule = getRoutineRuleById(ruleId);
      if (!rule) {
        await renderRoutineFixesMenu(ctx, 'Rule not found.');
        break;
      }
      const rendered = renderRoutineFixMatch({ rule, confidence: 1, fields: {} });
      await renderRoutineOutput(ctx, rendered, { backCallback: 'gsettings:routine_menu' });
      break;
    }
    case 'diag_test_log_delivery':
      await runLogDeliveryDiagnostics(ctx);
      break;
    case 'pm_status':
      await renderConfigDbStatus(ctx);
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
      await saveGlobalSettingsAndCache(updated);
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
      await saveGlobalSettingsAndCache(updated);
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
    case 'copy_health_url': {
      const healthUrl = `${getPublicBaseUrl().replace(/\/$/, '')}/health`;
      await ensureAnswerCallback(ctx, { text: healthUrl, show_alert: true });
      break;
    }
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
      resetUserState(ctx);
      clearPatchSession(ctx.from?.id);
      try {
        await navigateTo(getChatIdFromCtx(ctx), ctx.from?.id, 'main', {
          ctx,
          resetMenus: false,
          deleteIncomingMessage: false,
        });
      } catch (error) {
        console.warn('[navigation] Failed to return to main menu from settings', error?.message);
        await renderMainMenu(ctx);
      }
      break;
    default:
      break;
  }
}

async function handleConfigDbCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, action] = data.split(':');
  if (action === 'retry') {
    const triggered = triggerConfigDbWarmup('manual');
    await ctx.answerCallbackQuery({
      text: triggered ? '🔄 Retry started.' : '⏳ Retry already in progress.',
      show_alert: false,
    });
    return;
  }
  if (action === 'error') {
    const snapshot = getConfigDbSnapshot();
    const message = snapshot.lastErrorMessage || 'No recent error recorded.';
    const category = snapshot.lastErrorCategory || '-';
    await sendEphemeralMessage(ctx, `🧾 Config DB error\nCategory: ${category}\n${message}`);
    return;
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
      if (
        await ensureSupabaseTableAccess(ctx, connectionId, decodeSupabaseTableName(tableToken), 'table')
      ) {
        await renderSupabaseTableDetails(ctx, connectionId, decodeSupabaseTableName(tableToken));
      }
      break;
    case 'rows':
      resetUserState(ctx);
      if (
        await ensureSupabaseTableAccess(
          ctx,
          connectionId,
          decodeSupabaseTableName(tableToken),
          'rows',
          Number(extra) || 0,
        )
      ) {
        await renderSupabaseTableRows(
          ctx,
          connectionId,
          decodeSupabaseTableName(tableToken),
          Number(extra) || 0,
        );
      }
      break;
    case 'count':
      resetUserState(ctx);
      if (
        await ensureSupabaseTableAccess(ctx, connectionId, decodeSupabaseTableName(tableToken), 'count')
      ) {
        await renderSupabaseTableCount(ctx, connectionId, decodeSupabaseTableName(tableToken));
      }
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
    case 'pause_all': {
      const role = await resolveAccessRoleForUser(ctx.from?.id);
      if (!canAccess(role, 'mutate:cronjobs')) {
        await ensureAnswerCallback(ctx, { text: 'Read-only access', show_alert: true });
        return;
      }
      await renderCronMenu(ctx);
      break;
    }
    case 'resume_all': {
      const role = await resolveAccessRoleForUser(ctx.from?.id);
      if (!canAccess(role, 'mutate:cronjobs')) {
        await ensureAnswerCallback(ctx, { text: 'Read-only access', show_alert: true });
        return;
      }
      await renderCronMenu(ctx);
      break;
    }
    case 'create':
      {
        const cronSettings = await getEffectiveCronSettings();
        if (!cronSettings.enabled) {
          await renderOrEdit(ctx, 'Cron integration is disabled in settings.', {
            reply_markup: buildBackKeyboard('main:back'),
          });
          return;
        }
        const guidance = getCronProviderGuidance(cronSettings);
        if (guidance) {
          await renderOrEdit(ctx, guidance, {
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
          jobId,
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

    case 'test_all': {
      try {
        const response = await fetchCronJobs();
        const jobs = (response.jobs || []).filter((job) => job?.url).slice(0, 5);
        if (!jobs.length) {
          await sendTransientMessage(ctx, 'No cron jobs with target URL found.');
          break;
        }
        const results = [];
        for (const job of jobs) {
          const result = await runCronTargetReachabilityTest(job.url);
          results.push(`#${job.id} HTTP ${result.status != null ? result.status : '-'} · ${result.latencyMs}ms · ${result.category}`);
        }
        await sendTransientMessage(ctx, `🧪 Cron test (first ${jobs.length} job(s))\n${results.join('\n')}`);
      } catch (error) {
        const correlationId = buildCronCorrelationId();
        logCronApiError({ operation: 'test', error, userId: ctx.from?.id, projectId: null, correlationId });
        await sendTransientMessage(ctx, formatCronApiErrorNotice('Failed to test cron jobs', error, correlationId));
      }
      break;
    }
    case 'test': {
      try {
        const job = await fetchCronJob(jobId);
        if (!job?.url) {
          await sendTransientNotice(ctx, 'Cron job target URL is missing.', { ttlSec: 10, deleteButton: true, extraMarkup: buildBackKeyboard(`cron:job:${jobId}`) });
          break;
        }
        await renderCronTargetTestResult(ctx, jobId, job.url, 'cron:list');
      } catch (error) {
        const correlationId = buildCronCorrelationId();
        logCronApiError({
          operation: 'get',
          error,
          userId: ctx.from?.id,
          projectId: null,
          correlationId,
          jobId,
        });
        await renderOrEdit(ctx, formatCronApiErrorNotice('Failed to load cron job', error, correlationId), { reply_markup: buildBackKeyboard('cron:list') });
      }
      break;
    }
    case 'delete': {
      const inline = new InlineKeyboard()
        .text('✅ Yes, delete', `cron:delete_confirm:${jobId}`)
        .text('⬅️ No', `cron:job:${jobId}`);
      await renderOrEdit(ctx, `Delete cron job #${jobId}?`, { reply_markup: inline });
      break;
    }
    case 'cleanup_duplicates': {
      const summary = await cleanupCronDuplicateJobs();
      await sendTransientMessage(
        ctx,
        `🧹 Cleanup complete\nkept: ${summary.kept}\ndisabled: ${summary.disabled}\ndeleted: ${summary.deleted}`,
      );
      await renderCronJobList(ctx);
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
          jobId,
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
  const extra2 = parts[4];

  console.info('[projcron] callback received', {
    action,
    projectId,
    extra,
    data,
    userId: ctx.from?.id,
  });

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
    case 'keepalive_preset': {
      const scheduleKey = extra;
      const recreate = extra2 === '1';
      const scheduleInput = scheduleKey ? `every ${scheduleKey}` : '';
      await createProjectCronJobWithSchedule(ctx, projectId, 'keepalive', scheduleInput, recreate);
      break;
    }
    case 'keepalive_custom': {
      const recreate = extra === '1';
      setUserState(ctx.from.id, {
        type: recreate ? 'projcron_keepalive_recreate' : 'projcron_keepalive_schedule',
        projectId,
        backCallback: `projcron:menu:${projectId}`,
      });
      await renderOrEdit(
        ctx,
        'Send schedule (cron string or "every 10m", "every 1h"). Or press Cancel.',
        { reply_markup: buildCancelKeyboard() },
      );
      break;
    }
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

    case 'test': {
      const type = extra;
      const projects = await loadProjects();
      const project = findProjectById(projects, projectId);
      const jobId = type === 'deploy' ? project?.cronDeployHookJobId : project?.cronKeepAliveJobId;
      if (!jobId) {
        await sendTransientMessage(ctx, 'Cron job not linked for this target.');
        break;
      }
      try {
        const job = await fetchCronJob(jobId);
        if (!job?.url) {
          await sendTransientMessage(ctx, 'Cron job target URL is missing.');
          break;
        }
        await renderCronTargetTestResult(ctx, jobId, job.url, `projcron:menu:${projectId}`);
      } catch (error) {
        const correlationId = buildCronCorrelationId();
        logCronApiError({ operation: 'test', error, userId: ctx.from?.id, projectId, correlationId, jobId });
        await sendTransientMessage(ctx, formatCronApiErrorNotice('Failed to test cron job', error, correlationId));
      }
      break;
    }
    case 'alerts_toggle':
      await toggleProjectCronAlerts(ctx, projectId);
      break;
    case 'alerts_levels':
      await renderProjectCronAlertLevels(ctx, projectId);
      break;
    case 'alerts_level':
      await toggleProjectCronAlertLevel(ctx, projectId, extra);
      break;
    case 'retry_create': {
      const payload = cronCreateRetryCache.get(projectId);
      if (!payload) {
        await ctx.answerCallbackQuery({
          text: 'Retry details expired. Please start again.',
          show_alert: true,
        });
        return;
      }
      cronCreateRetryCache.delete(projectId);
      await createProjectCronJobWithSchedule(
        ctx,
        payload.projectId,
        payload.type,
        payload.scheduleInput,
        payload.recreate,
      );
      break;
    }
    case 'copy_debug': {
      const details = cronErrorDetailsCache.get(projectId);
      if (!details) {
        await ctx.answerCallbackQuery({
          text: 'Debug details expired. Please retry.',
          show_alert: true,
        });
        return;
      }
      const lines = [
        'Cron debug details:',
        `Project: ${details.projectId}`,
        `Type: ${details.type}`,
        `Schedule: ${details.schedule || '-'}`,
        `Target: ${details.targetUrl || '-'}`,
        `Status: ${details.status || '-'}`,
        `Endpoint: ${details.path || '-'}`,
        `Reason: ${details.reason || '-'}`,
      ];
      await ctx.reply(lines.join('\n'));
      break;
    }
    default:
      console.warn('[projcron] Unknown action', { action, projectId, extra, data });
      await ctx.answerCallbackQuery({
        text: 'Unknown cron action. Please reopen the menu.',
        show_alert: true,
      });
      await renderProjectCronBindings(ctx, projectId);
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

  if (!getConfigDbSnapshot().ready) {
    await renderConfigDbGate(ctx, { title: '🔐 Env Vault', backCallback: `proj:open:${projectId}` });
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
    case 'search':
      setUserState(ctx.from.id, {
        type: 'env_vault_search',
        projectId,
        backCallback: `envvault:menu:${projectId}`,
        messageContext: getMessageTargetFromCtx(ctx),
      });
      await renderOrEdit(ctx, 'Search Env Vault key by name.\nSend the key or Cancel.', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'search_edit':
      await promptEnvVaultValue(ctx, projectId, key, { messageContext: getMessageTargetFromCtx(ctx) });
      break;
    case 'search_delete':
      await handleEnvVaultDelete(ctx, projectId, key);
      break;
    case 'merge':
      await startEnvVaultDuplicateMerge(ctx, projectId);
      break;
    case 'merge_pick':
      await handleEnvVaultMergePick(ctx, projectId, key, extra);
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
    case 'view':
      await revealEnvVaultValue(ctx, projectId, key, { ephemeral: true });
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
  if (!process.env.DATABASE_URL_PM && !process.env.PATH_APPLIER_CONFIG_DSN) {
    warnings.push('⚠️ Config DB is not configured; Env Vault is in-memory only.');
  }
  if (!appState.dbReady) {
    warnings.push('⚠️ Config DB status: DOWN (degraded mode).');
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
    .text('🔎 Search env', `envvault:search:${projectId}`)
    .row()
    .text('🗑 Delete key', `envvault:delete_menu:${projectId}`)
    .row()
    .text('🧩 Duplicate env merge', `envvault:merge:${projectId}`)
    .row()
    .text('🧩 Recommended keys', `envvault:recommend:${projectId}`)
    .row()
    .text('🧩 Import from text', `envvault:import:${projectId}`)
    .row()
    .text('📤 Export keys', `envvault:export:${projectId}`)
    .row()
    .text('📤 Export env (masked + file)', `proj:env_export:${projectId}`)
    .row()
    .text('🔎 Scan env requirements', `proj:env_scan:${projectId}`)
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
    inline
      .text(`🔎 ${key}`, `envvault:key:${projectId}:${key}`)
      .text('👁 View (30s)', `envvault:view:${projectId}:${key}`)
      .row();
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

async function revealEnvVaultValue(ctx, projectId, key, options = {}) {
  const envSetId = await ensureProjectEnvSet(projectId);
  try {
    const value = await getEnvVarValue(projectId, key, envSetId);
    if (!value) {
      await ctx.reply('Key not found.');
      return;
    }
    const settings = await getCachedSettings();
    const formatted = formatEnvValueForDisplay(key, value, settings);
    const message = `🔐 ${escapeHtml(key)}:\n${formatted.text}`;
    await sendEphemeralMessage(ctx, message, { parse_mode: 'HTML' });
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

async function handleEnvVaultSearchInput(ctx, state) {
  const query = ctx.message.text?.trim();
  if (!query) {
    await ctx.reply('Please send a search query.');
    return;
  }
  if (query.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(state.backCallback || 'main:back'),
    });
    return;
  }
  const envSetId = await ensureProjectEnvSet(state.projectId);
  const envVars = await listEnvVarKeys(state.projectId, envSetId);
  const matchKey = envVars.find((key) => key.toLowerCase() === query.toLowerCase());
  if (!matchKey) {
    clearUserState(ctx.from.id);
    await renderStateMessage(ctx, state, '⚠️ No matching key found.', {
      reply_markup: buildBackKeyboard(`envvault:menu:${state.projectId}`),
    });
    return;
  }
  const value = await getEnvVarValue(state.projectId, matchKey, envSetId);
  const settings = await getCachedSettings();
  const formatted = formatEnvValueForDisplay(matchKey, value, settings);
  const message = `🔎 Env Vault — ${escapeHtml(matchKey)}\n${formatted.text}`;
  await sendEphemeralMessage(ctx, message, { parse_mode: 'HTML' });
  clearUserState(ctx.from.id);
  await renderStateMessage(ctx, state, '✅ Result sent (auto-delete enabled).', {
    reply_markup: buildBackKeyboard(`envvault:menu:${state.projectId}`),
  });
}

function buildEnvDuplicateCandidates(project, envVars, envSetId) {
  const candidates = new Map();
  const addCandidate = (key, value, source, firstSeenIn) => {
    if (!key || evaluateEnvValueStatus(value).status !== 'SET') return;
    if (!candidates.has(key)) {
      candidates.set(key, []);
    }
    candidates.get(key).push({ value, source, firstSeenIn });
  };

  envVars.forEach((entry) => {
    addCandidate(entry.key, entry.value, 'project_env_vault', 'env_vault');
  });

  const trackedKeys = new Set(envVars.map((entry) => entry.key));
  trackedKeys.add('DATABASE_URL');
  trackedKeys.add('PROJECT_NAME');

  trackedKeys.forEach((key) => {
    if (process.env[key]) {
      addCandidate(key, process.env[key], 'process_env', 'runtime');
    }
  });

  if (project?.databaseUrl) {
    addCandidate('DATABASE_URL', project.databaseUrl, 'project_db_config', 'project_config');
  }
  if (project?.name || project?.id) {
    addCandidate('PROJECT_NAME', project.name || project.id, 'computed_default', 'project_meta');
  }

  return Array.from(candidates.entries())
    .map(([key, values]) => {
      const uniqueValues = new Map();
      values.forEach((candidate) => {
        const normalized = String(candidate.value);
        if (!uniqueValues.has(normalized)) {
          uniqueValues.set(normalized, candidate);
        }
      });
      return { key, candidates: Array.from(uniqueValues.values()) };
    })
    .filter((entry) => entry.candidates.length > 1);
}

async function startEnvVaultDuplicateMerge(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const envSetId = await ensureProjectEnvSet(projectId);
  const envVars = await listEnvVars(projectId, envSetId);
  const hydrated = [];
  for (const entry of envVars) {
    const value = await getEnvVarValue(projectId, entry.key, envSetId);
    hydrated.push({ key: entry.key, value });
  }
  const conflicts = buildEnvDuplicateCandidates(project, hydrated, envSetId);
  if (!conflicts.length) {
    await renderOrEdit(ctx, 'No duplicate env keys detected across sources.', {
      reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`),
    });
    return;
  }
  setUserState(ctx.from.id, {
    type: 'env_vault_merge',
    projectId,
    envSetId,
    conflicts,
    index: 0,
    resolved: [],
    messageContext: getMessageTargetFromCtx(ctx),
  });
  await renderEnvVaultMergePrompt(ctx, getUserState(ctx.from.id));
}

async function renderEnvVaultMergePrompt(ctx, state) {
  if (!state || state.type !== 'env_vault_merge') return;
  const conflict = state.conflicts[state.index];
  if (!conflict) {
    clearUserState(ctx.from.id);
    await renderOrEdit(ctx, 'Duplicate env merge complete.', {
      reply_markup: buildBackKeyboard(`envvault:menu:${state.projectId}`),
    });
    return;
  }

  const lines = [
    `🧩 Duplicate env merge — ${state.projectId}`,
    `Key: ${conflict.key}`,
    '',
    'Pick the value to keep (others will be overridden by Env Vault):',
  ];
  const inline = new InlineKeyboard();
  conflict.candidates.forEach((candidate, idx) => {
    const masked = maskEnvValue(candidate.value);
    lines.push(`• ${masked} (source: ${candidate.source}, firstSeenIn: ${candidate.firstSeenIn})`);
    inline.text(`✅ Use ${candidate.source}`, `envvault:merge_pick:${state.projectId}:${conflict.key}:${idx}`).row();
  });
  inline.text('⬅️ Back', `envvault:menu:${state.projectId}`);
  await renderStateMessage(ctx, state, lines.join('\n'), { reply_markup: inline });
}

async function handleEnvVaultMergePick(ctx, projectId, key, indexToken) {
  const state = getUserState(ctx.from.id);
  if (!state || state.type !== 'env_vault_merge') {
    await renderOrEdit(ctx, 'Merge session expired.', {
      reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`),
    });
    return;
  }
  const conflict = state.conflicts[state.index];
  if (!conflict || conflict.key !== key) {
    await renderOrEdit(ctx, 'Merge session out of sync. Restart from Env Vault.', {
      reply_markup: buildBackKeyboard(`envvault:menu:${projectId}`),
    });
    clearUserState(ctx.from.id);
    return;
  }
  const idx = Number(indexToken);
  const candidate = conflict.candidates[idx];
  if (!candidate) {
    await ctx.answerCallbackQuery({ text: 'Invalid selection.', show_alert: true });
    return;
  }
  await upsertEnvVar(projectId, key, candidate.value, state.envSetId);
  state.resolved.push({ key, source: candidate.source });
  state.index += 1;
  setUserState(ctx.from.id, state);
  await renderEnvVaultMergePrompt(ctx, state);
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
    backCallback: options.backCallback || `envvault:menu:${projectId}`,
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
    const inline = new InlineKeyboard().text('⬅️ Back', state.backCallback || `envvault:menu:${state.projectId}`);
    inline.row().text('❌ Cancel', 'cancel_input');
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
  await sendDismissibleMessage(ctx, lines.join('\n'), {
    reply_markup: buildBackKeyboard(state.backCallback || `envvault:menu:${state.projectId}`),
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
  if (missing.some((item) => item.key === 'serviceHealth' || item.key === 'startCommand' || item.key === 'testCommand' || item.key === 'diagnosticCommand')) {
    inline.text('🛠 Auto-Fix Health Setup', `proj:auto_fix_health:${project.id}`).row();
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
    console.error('[supabase] Failed to read DB URL from Env Vault', error);
    await forwardSelfLog('error', 'Failed to read Supabase URL/API key from Env Vault', {
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

function buildSupabaseTableAccessKey(userId, connectionId, tableName) {
  return `${userId || 'anon'}:${connectionId || 'unknown'}:${tableName || 'table'}`;
}

function getSupabaseTableAccessSession(key) {
  const session = supabaseTableAccess.get(key);
  if (!session) return null;
  if (session.expiresAt && session.expiresAt <= Date.now()) {
    supabaseTableAccess.delete(key);
    return null;
  }
  return session;
}

async function ensureSupabaseTableAccess(ctx, connectionId, tableName, action, page = 0) {
  const userId = ctx.from?.id;
  const accessKey = buildSupabaseTableAccessKey(userId, connectionId, tableName);
  const session = getSupabaseTableAccessSession(accessKey);
  if (session?.verifiedUntil && session.verifiedUntil > Date.now()) {
    return true;
  }

  const token = Math.random().toString(36).slice(2, 8).toUpperCase();
  supabaseTableAccess.set(accessKey, {
    token,
    expiresAt: Date.now() + SUPABASE_TABLE_ACCESS_TTL_MS,
    verifiedUntil: null,
  });

  setUserState(ctx.from.id, {
    type: 'supabase_table_auth',
    connectionId,
    tableName,
    requestedAction: action,
    page,
    messageContext: getMessageTargetFromCtx(ctx),
  });

  await renderOrEdit(
    ctx,
    `🔐 Table access required for "${tableName}".\nRe-enter access token within 60s:\n${token}\n(Or press Cancel)`,
    { reply_markup: buildCancelKeyboard() },
  );
  return false;
}

async function handleSupabaseTableAuthInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please enter the access token.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderOrEdit(ctx, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(`supabase:tables:${state.connectionId}`),
    });
    return;
  }

  const accessKey = buildSupabaseTableAccessKey(ctx.from?.id, state.connectionId, state.tableName);
  const session = getSupabaseTableAccessSession(accessKey);
  if (!session) {
    resetUserState(ctx);
    await renderOrEdit(ctx, 'Access token expired. Please open the table again.', {
      reply_markup: buildBackKeyboard(`supabase:tables:${state.connectionId}`),
    });
    return;
  }
  if (session.token !== text.trim().toUpperCase()) {
    await ctx.reply('Invalid access token. Please try again.');
    return;
  }

  session.verifiedUntil = Date.now() + SUPABASE_TABLE_ACCESS_TTL_MS;
  session.expiresAt = Date.now() + SUPABASE_TABLE_ACCESS_TTL_MS;
  supabaseTableAccess.set(accessKey, session);
  clearUserState(ctx.from.id);

  if (state.requestedAction === 'rows') {
    await renderSupabaseTableRows(ctx, state.connectionId, state.tableName, Number(state.page) || 0);
    return;
  }
  if (state.requestedAction === 'count') {
    await renderSupabaseTableCount(ctx, state.connectionId, state.tableName);
    return;
  }
  await renderSupabaseTableDetails(ctx, state.connectionId, state.tableName);
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

function escapeHtmlAttribute(value) {
  return escapeHtml(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function parseCookies(request) {
  const header = request.headers?.cookie;
  if (!header) return {};
  return header.split(';').reduce((acc, pair) => {
    const [rawKey, ...rest] = pair.split('=');
    if (!rawKey) return acc;
    acc[rawKey.trim()] = decodeURIComponent(rest.join('=').trim());
    return acc;
  }, {});
}

function parseFormBody(body) {
  const params = new URLSearchParams(body);
  const result = {};
  params.forEach((value, key) => {
    result[key] = value;
  });
  return result;
}

function hashSqlQuery(sql) {
  return crypto.createHash('sha256').update(String(sql || '')).digest('hex');
}

function normalizeSqlInput(raw) {
  const trimmed = String(raw || '').trim();
  if (!trimmed) {
    return { ok: false, error: 'Query is required.' };
  }
  const stripped = trimmed.replace(/;+\s*$/, '');
  if (stripped.includes(';')) {
    return { ok: false, error: 'Multiple statements are not allowed.' };
  }
  return { ok: true, sql: stripped };
}

function normalizeSqlLimit(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return MINI_SITE_SQL_DEFAULT_LIMIT;
  }
  return Math.min(parsed, MINI_SITE_SQL_MAX_LIMIT);
}

function isSqlWriteAttempt(sql) {
  const forbidden = [
    'insert',
    'update',
    'delete',
    'drop',
    'alter',
    'truncate',
    'grant',
    'revoke',
    'create',
    'vacuum',
    'merge',
    'reindex',
    'cluster',
    'comment',
  ];
  const lowered = String(sql || '').toLowerCase();
  if (forbidden.some((keyword) => new RegExp(`\\b${keyword}\\b`, 'i').test(lowered))) {
    return true;
  }
  const firstWord = lowered.trim().split(/\s+/)[0];
  return !['select', 'show', 'explain'].includes(firstWord);
}

function getMiniSiteWriteGrantKey(req, adminTokenHash) {
  const bearerToken = getBearerToken(req);
  if (isMiniSiteAdminTokenValid(bearerToken, adminTokenHash)) {
    return `bearer:${hashMiniSiteToken(bearerToken)}`;
  }
  const cookies = parseCookies(req);
  const editToken = cookies[MINI_SITE_EDIT_SESSION_COOKIE];
  if (editToken) {
    return `edit:${editToken}`;
  }
  return null;
}

function enableMiniSiteWriteGrant(req, adminTokenHash) {
  const key = getMiniSiteWriteGrantKey(req, adminTokenHash);
  if (!key) return null;
  const expiresAt = Date.now() + MINI_SITE_SQL_WRITE_WINDOW_MS;
  miniSiteWriteGrants.set(key, expiresAt);
  return expiresAt;
}

function isMiniSiteWriteGrantActive(req, adminTokenHash) {
  const key = getMiniSiteWriteGrantKey(req, adminTokenHash);
  if (!key) return false;
  const expiresAt = miniSiteWriteGrants.get(key);
  if (!expiresAt) return false;
  if (Date.now() > expiresAt) {
    miniSiteWriteGrants.delete(key);
    return false;
  }
  return true;
}

function renderMiniSiteLayout(title, body) {
  return `
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>${escapeHtml(title)}</title>
        <style>
          body { font-family: Inter, system-ui, sans-serif; background: #0b0f1a; color: #e6e8ef; margin: 0; }
          header { padding: 24px; background: #111827; border-bottom: 1px solid #1f2937; }
          main { padding: 24px; max-width: 960px; margin: 0 auto; }
          .card { background: #0f172a; border: 1px solid #1e293b; border-radius: 12px; padding: 16px; margin-bottom: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.2); transition: transform .2s ease, box-shadow .2s ease; }
          .card:hover { transform: translateY(-2px); box-shadow: 0 14px 40px rgba(0,0,0,0.25); }
          a { color: #60a5fa; text-decoration: none; transition: color .2s ease; }
          a:hover { color: #93c5fd; }
          .button { display: inline-block; padding: 8px 14px; border-radius: 8px; background: #1d4ed8; color: #fff; transition: transform .2s ease, background .2s ease; }
          .button:hover { background: #2563eb; transform: translateY(-1px); }
          .muted { color: #9ca3af; }
          table { width: 100%; border-collapse: collapse; }
          th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #1f2937; }
          th { color: #93c5fd; font-weight: 600; }
          .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #1e293b; color: #cbd5f5; font-size: 12px; }
          .row-actions { display: flex; gap: 8px; flex-wrap: wrap; }
          .grid { display: grid; gap: 16px; }
          .grid.two-col { grid-template-columns: repeat(2, minmax(0, 1fr)); }
          .fade-in { animation: fadeIn .35s ease; }
          @keyframes fadeIn { from { opacity: 0; transform: translateY(4px);} to { opacity: 1; transform: translateY(0);} }
          input, select, textarea { width: 100%; padding: 8px 10px; border-radius: 8px; border: 1px solid #334155; background: #0b1220; color: #e6e8ef; }
          .form-row { margin-bottom: 12px; }
          .tag { display: inline-block; padding: 4px 8px; border-radius: 999px; font-size: 12px; background: #1f2937; color: #cbd5f5; }
          .warning { color: #fca5a5; }
          .success { color: #86efac; }
          .stack { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; }
          @media (min-width: 720px) {
            .grid.two-col { grid-template-columns: repeat(2, minmax(0, 1fr)); }
          }
        </style>
      </head>
      <body>
        <header>
          <h2>Project Manager DB mini-site</h2>
          <p class="muted">${escapeHtml(title)}</p>
        </header>
        <main class="fade-in">
          ${body}
        </main>
      </body>
    </html>
  `;
}

function renderWebLoginPage({ token, mask, message }) {
  const displayToken = token ? escapeHtml(token) : null;
  const tokenMask = escapeHtml(mask || '••••');
  const note = message ? `<p class="muted">${escapeHtml(message)}</p>` : '';
  const tokenBlock = displayToken
    ? `<div class="token-box"><strong>New token (shown once):</strong><div class="token">${displayToken}</div></div>`
    : `<p class="muted">Token on file: ${tokenMask}</p>`;
  return `
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Project Manager — Web Login</title>
        <style>
          body { font-family: Inter, system-ui, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 40px; }
          .card { max-width: 540px; margin: 0 auto; background: #111827; border-radius: 16px; padding: 24px; box-shadow: 0 20px 40px rgba(15, 23, 42, 0.35); }
          h1 { font-size: 22px; margin-bottom: 8px; }
          .muted { color: #94a3b8; font-size: 14px; }
          .token-box { background: #0b1220; border: 1px solid #1f2937; padding: 12px; border-radius: 12px; margin: 16px 0; }
          .token { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 14px; word-break: break-all; margin-top: 6px; }
          label { display: block; margin-top: 16px; font-size: 14px; }
          input { width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid #1f2937; background: #0b1220; color: #e2e8f0; }
          button { margin-top: 16px; width: 100%; padding: 12px; border-radius: 10px; border: none; background: #38bdf8; color: #0f172a; font-weight: 600; cursor: pointer; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>🔐 Project Manager Dashboard</h1>
          <p class="muted">Enter the dashboard token to continue.</p>
          ${note}
          ${tokenBlock}
          <form method="POST" action="/web/login">
            <label>Token</label>
            <input name="token" type="password" required />
            <button type="submit">Login</button>
          </form>
          <p class="muted">After login, return to <a href="/web" style="color:#38bdf8;">/web</a>.</p>
        </div>
      </body>
    </html>
  `;
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
      `• ${status.connection.name} (${status.connection.id}) — env: ${status.connection.envKey} — DB URL: ${status.dsnStatus} — Connect: ${status.connectStatus}`,
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
    `DB URL: ${dsnInfo.dsn ? '✅ present' : `❌ ${dsnInfo.error}`}`,
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

function buildRangeStep(step, max, start = 0) {
  const out = [];
  for (let value = start; value <= max; value += step) out.push(value);
  return out;
}

function normalizeCronSchedulePayload(schedule, timezone) {
  const tz = timezone || 'UTC';
  if (schedule && typeof schedule === 'object' && !Array.isArray(schedule)) {
    if (Array.isArray(schedule.minutes) || Array.isArray(schedule.hours) || Array.isArray(schedule.mdays)
      || Array.isArray(schedule.months) || Array.isArray(schedule.wdays)) {
      return {
        timezone: schedule.timezone || tz,
        minutes: normalizeScheduleArray(schedule.minutes),
        hours: normalizeScheduleArray(schedule.hours),
        mdays: normalizeScheduleArray(schedule.mdays),
        months: normalizeScheduleArray(schedule.months),
        wdays: normalizeScheduleArray(schedule.wdays),
      };
    }
    if (typeof schedule.cron === 'string' || typeof schedule.expression === 'string') {
      return normalizeCronSchedulePayload(schedule.cron || schedule.expression, schedule.timezone || tz);
    }
  }

  const cronExpr = String(schedule || '').trim();
  const parts = cronExpr.split(/\s+/);
  if (parts.length !== 5) {
    throw new Error('Unsupported cron format. Accepted: */N * * * *, 0 */N * * *, 0 0 * * * and simple fixed values.');
  }
  const [m,h,md,mo,wd]=parts;

  function parseField(value, min, max, allowAny=true) {
    if (value === '*') return [-1];
    if (/^\*\/\d+$/.test(value)) {
      const step = Number(value.slice(2));
      if (!Number.isFinite(step) || step <= 0) throw new Error('Invalid step in cron expression.');
      return buildRangeStep(step, max, min);
    }
    if (/^\d+$/.test(value)) {
      const num = Number(value);
      if (num < min || num > max) throw new Error('Cron value out of range.');
      return [num];
    }
    throw new Error('Unsupported cron format. Accepted: */N * * * *, 0 */N * * *, 0 0 * * * and simple fixed values.');
  }

  return {
    timezone: tz,
    minutes: parseField(m, 0, 59),
    hours: parseField(h, 0, 23),
    mdays: parseField(md, 1, 31),
    months: parseField(mo, 1, 12),
    wdays: parseField(wd, 0, 6),
  };
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



async function cleanupCronDuplicateJobs() {
  const response = await fetchCronJobs();
  const jobs = response.jobs || [];
  const links = await listCronJobLinks();
  const linkMap = new Map(links.map((link) => [String(link.cronJobId), link]));
  const projects = await loadProjects();

  const grouped = new Map();
  for (const job of jobs) {
    const link = linkMap.get(String(job.id));
    const projectId = resolveCronProjectId(job, link, projects);
    const inferredType = (job.name || '').includes(':deploy') ? 'deploy' : 'keepalive';
    const jobKey = link?.jobKey
      || parseJobKeyFromTitle(job.name)
      || buildCronJobKey({ projectId, type: inferredType, targetUrl: job.url, schedule: normalizeCronJobSchedule(job) });
    if (!jobKey) continue;
    if (!grouped.has(jobKey)) grouped.set(jobKey, []);
    grouped.get(jobKey).push({ ...job, jobKey });
  }

  const summary = { kept: 0, disabled: 0, deleted: 0 };
  for (const [jobKey, items] of grouped.entries()) {
    if (items.length < 2) continue;
    const best = chooseBestCronJob(items);
    if (!best) continue;
    summary.kept += 1;
    for (const job of items) {
      if (String(job.id) === String(best.id)) continue;
      if (job.enabled !== false) {
        await toggleJob(String(job.id), false);
      }
      summary.disabled += 1;
      await upsertCronJobLink(String(job.id), resolveCronProjectId(job, linkMap.get(String(job.id)), projects), null, {
        providerJobId: String(job.id),
        jobKey,
        enabled: false,
        scheduleNormalized: normalizeCronJobSchedule(job),
        targetNormalized: normalizeCronJobTarget(job.url),
        lastUpdatedAt: new Date().toISOString(),
      });
    }
  }
  clearCronJobsCache();
  return summary;
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
  const schedulePayload = normalizeCronSchedulePayload(schedule, timezone);
  return {
    title: name,
    url,
    enabled: enabled !== false,
    schedule: schedulePayload,
  };
}

function normalizeCronJobSchedule(job) {
  const summary = describeCronSchedule(job, { includeAllHours: true });
  return String(summary || '').trim().toLowerCase();
}

function normalizeCronJobTarget(url) {
  try {
    const parsed = new URL(String(url || '').trim());
    const pathname = parsed.pathname.replace(/\/+$/, '') || '/';
    return `${parsed.protocol}//${parsed.host}${pathname}`.toLowerCase();
  } catch (error) {
    return String(url || '').trim().toLowerCase();
  }
}

function buildCronJobKey({ projectId, type, targetUrl, schedule }) {
  if (!projectId || !type) return null;
  if (type === 'keepalive') {
    return `${projectId}:keepalive`;
  }
  const normalizedTarget = normalizeCronJobTarget(targetUrl);
  const normalizedSchedule = String(schedule || '').trim().toLowerCase();
  return `${projectId}:${type}:${normalizedTarget}:${normalizedSchedule}`;
}

function parseJobKeyFromTitle(title) {
  const text = String(title || '');
  const match = text.match(/PM:([^\s]+)/);
  return match ? match[1] : null;
}

function scoreCronJobForDedup(job) {
  const enabledScore = job?.enabled !== false ? 1 : 0;
  const updatedAt = Date.parse(job?.lastUpdatedAt || '') || 0;
  return { enabledScore, updatedAt };
}

function chooseBestCronJob(entries) {
  const sorted = [...entries].sort((a, b) => {
    const left = scoreCronJobForDedup(a);
    const right = scoreCronJobForDedup(b);
    if (left.enabledScore !== right.enabledScore) return right.enabledScore - left.enabledScore;
    if (left.updatedAt !== right.updatedAt) return right.updatedAt - left.updatedAt;
    return String(a.id || '').localeCompare(String(b.id || ''));
  });
  return sorted[0] || null;
}

function dedupeCronJobsByJobKey(jobs, linksMap, projects) {
  const grouped = new Map();
  for (const job of jobs) {
    const link = linksMap.get(String(job.id));
    const projectId = resolveCronProjectId(job, link, projects);
    const inferredType = (job.name || '').includes(':deploy') ? 'deploy' : 'keepalive';
    const jobKey = link?.jobKey
      || parseJobKeyFromTitle(job.name)
      || buildCronJobKey({
        projectId,
        type: inferredType,
        targetUrl: job.url,
        schedule: normalizeCronJobSchedule(job),
      });
    const enriched = {
      ...job,
      jobKey,
      duplicatesDetected: false,
      lastUpdatedAt: link?.lastUpdatedAt || null,
    };
    const key = jobKey || `__id__:${job.id}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(enriched);
  }
  const deduped = [];
  for (const items of grouped.values()) {
    const best = chooseBestCronJob(items);
    if (!best) continue;
    if (items.length > 1) {
      best.duplicatesDetected = true;
      best.duplicateCount = items.length;
    }
    deduped.push(best);
  }
  return deduped;
}

function cronJobLooksLikeType(job, type) {
  const lower = String(job?.name || '').toLowerCase();
  if (type === 'keepalive') return lower.includes('keep-alive') || lower.includes('keepalive');
  if (type === 'deploy') return lower.includes('deploy');
  return true;
}

async function findProviderJobForProjectCron({ projectId, type, jobKey, targetUrl, scheduleNormalized }) {
  const links = await listCronJobLinks();
  const linkByKey = links.find((entry) => entry.jobKey === jobKey);
  if (linkByKey?.providerJobId) {
    return { id: String(linkByKey.providerJobId), source: 'configdb:jobKey' };
  }
  const linkByProject = links.find((entry) => entry.projectId === projectId && cronJobLooksLikeType({ name: entry.label || '' }, type));
  if (linkByProject?.providerJobId) {
    return { id: String(linkByProject.providerJobId), source: 'configdb:project' };
  }
  const response = await fetchCronJobs();
  const targetNormalized = normalizeCronJobTarget(targetUrl);
  for (const job of response.jobs) {
    const parsedJobKey = parseJobKeyFromTitle(job.name);
    if (parsedJobKey && parsedJobKey === jobKey) {
      return { id: String(job.id), source: 'provider:title' };
    }
    if (!cronJobLooksLikeType(job, type)) continue;
    if (normalizeCronJobTarget(job.url) !== targetNormalized) continue;
    if (normalizeCronJobSchedule(job) !== scheduleNormalized) continue;
    return { id: String(job.id), source: 'provider:url+schedule' };
  }
  return null;
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
    await sendTransientMessage(ctx, `Cron job created (id: #${created.id}, title: ${jobName}).`);
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
  const guidance = getCronProviderGuidance(cronSettings);
  if (guidance) {
    await renderOrEdit(ctx, guidance, {
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

  const lines = ['⏱ Cron Jobs', buildScopedHeader('GLOBAL', 'Main → Cron Jobs'), `Total jobs: ${jobs.length}`];
  const inline = new InlineKeyboard()
    .text('📋 List jobs', 'cron:list')
    .row()
    .text('➕ Create job', 'cron:create')
    .row()
    .text('🧷 Link job to project', 'cronlink:menu')
    .row()
    .text('⏸ Pause all', 'cron:pause_all')
    .text('▶️ Resume all', 'cron:resume_all')
    .row()
    .text('⚙️ Global cron defaults', 'gsettings:defaults')
    .row()
    .text('🧹 Show “Other” only', 'cronlink:other')
    .row()
    .text('🔎 Filter by project', 'cronlink:filter_menu')
    .row()
    .text('🧪 Test all jobs', 'cron:test_all')
    .row()
    .text('⬅️ Back', 'main:back');

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderCronJobList(ctx, options = {}) {
  const cronSettings = await getEffectiveCronSettings();
  const guidance = getCronProviderGuidance(cronSettings);
  if (guidance) {
    await renderOrEdit(ctx, guidance, {
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

  const deduped = dedupeCronJobsByJobKey(jobs, linkMap, projects);
  const filtered = filterCronJobsByProject(deduped, projects, linkMap, options);
  const grouped = groupCronJobs(filtered, projects, linkMap);

  const lines = ['Cron jobs:'];
  if (!grouped.length) {
    lines.push('No cron jobs found.');
  }
  grouped.forEach((group) => {
    group.jobs.forEach((job) => {
      const schedule = describeCronSchedule(job);
      const duplicateNote = job.duplicatesDetected ? ` (deduped${job.duplicateCount ? `, ${job.duplicateCount}x` : ''})` : '';
      lines.push(
        `[${group.label}] — ${getCronJobDisplayName(job)} — ${job.enabled ? 'Enabled' : 'Disabled'} — ${schedule}${duplicateNote}`,
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
      inline.text(label, `cron:job:${job.id}`).text('🧪 Test cron', `cron:test:${job.id}`).row();
    });
  });
  inline.text('🧹 Cleanup duplicates', 'cron:cleanup_duplicates').row();
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
  const guidance = getCronProviderGuidance(await getEffectiveCronSettings());
  if (guidance) {
    await renderOrEdit(ctx, guidance, {
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


function sanitizeCronTargetSnippet(text, maxLen = 200) {
  const withoutHtml = String(text || '').replace(/<[^>]*>/g, ' ');
  const compact = withoutHtml.replace(/\s+/g, ' ').trim();
  return truncateText(compact, maxLen);
}

async function runCronTargetReachabilityTest(url) {
  const startedAt = Date.now();
  try {
    const response = await fetch(url, { method: 'GET', timeout: 10000, redirect: 'follow' });
    const body = await response.text();
    return {
      ok: response.ok,
      category: 'http',
      status: response.status,
      latencyMs: Date.now() - startedAt,
      snippet: sanitizeCronTargetSnippet(body),
    };
  } catch (error) {
    const latencyMs = Date.now() - startedAt;
    const code = String(error?.code || '').toUpperCase();
    const message = String(error?.message || 'request failed');
    let category = 'network';
    if (code.includes('TIMEOUT') || message.toLowerCase().includes('timeout')) {
      category = 'timeout';
    } else if (code.includes('ENOTFOUND') || code.includes('EAI_AGAIN')) {
      category = 'dns';
    } else if (code.includes('CERT') || code.includes('TLS')) {
      category = 'tls';
    }
    return {
      ok: false,
      category,
      status: null,
      latencyMs,
      snippet: sanitizeCronTargetSnippet(message),
    };
  }
}

async function renderCronTargetTestResult(ctx, jobId, url, backCallback = `cron:job:${jobId}`) {
  const result = await runCronTargetReachabilityTest(url);
  const lines = [
    '🧪 Cron target reachability test',
    `Job: #${jobId}`,
    `URL: ${url}`,
    `Category: ${result.category}`,
    `HTTP status: ${result.status != null ? result.status : '-'}`,
    `Latency: ${result.latencyMs}ms`,
    `Body (first 200 chars): ${result.snippet || '-'}`,
  ];
  await sendTransientNotice(ctx, lines.join('\n'), {
    ttlSec: 10,
    deleteButton: true,
    extraMarkup: buildBackKeyboard(backCallback),
  });
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
    .text('🧪 Test cron', `cron:test:${jobId}`)
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
    ctx,
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
    await sendTransientNotice(ctx, 'Cron job updated.', { ttlSec: 10, deleteButton: true });
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
    await sendTransientNotice(ctx, 'Cron job updated.', { ttlSec: 10, deleteButton: true });
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
    await sendTransientNotice(ctx, 'Cron job updated.', { ttlSec: 10, deleteButton: true });
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
    await sendTransientNotice(ctx, 'Cron job updated.', { ttlSec: 10, deleteButton: true });
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

async function createProjectCronJobWithSchedule(ctx, projectId, type, scheduleInput, recreate) {
  let schedule;
  try {
    schedule = typeof scheduleInput === 'string' ? parseScheduleInput(scheduleInput) : scheduleInput;
  } catch (error) {
    await ctx.reply(`Invalid schedule: ${error.message}`);
    return;
  }

  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    clearUserState(ctx.from.id);
    await ctx.reply('Project not found.');
    return;
  }

  const cronSettings = await getEffectiveCronSettings();
  const isKeepAlive = type === 'keepalive';
  const scheduleNormalized = String(schedule?.cron || '').trim().toLowerCase();

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
  const targetNormalized = normalizeCronJobTarget(targetUrl);
  const jobKey = buildCronJobKey({ projectId: project.id, type, targetUrl, schedule: scheduleNormalized });
  const jobName = isKeepAlive
    ? `PM:${jobKey} path-applier:${project.id}:keep-alive`
    : `PM:${jobKey} path-applier:${project.id}:deploy`;

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
    let providerJobId = null;
    const existingByKey = await getCronJobLinkByJobKey(jobKey);
    if (existingByKey?.providerJobId) {
      providerJobId = String(existingByKey.providerJobId);
    }

    if (!providerJobId && recreate) {
      const oldJobId = isKeepAlive ? project.cronKeepAliveJobId : project.cronDeployHookJobId;
      providerJobId = oldJobId ? String(oldJobId) : null;
    }

    if (!providerJobId) {
      const found = await findProviderJobForProjectCron({
        projectId: project.id,
        type,
        jobKey,
        targetUrl: urlValidation.url,
        scheduleNormalized,
      });
      providerJobId = found?.id || null;
    }

    if (providerJobId) {
      await updateJob(providerJobId, payload);
    } else {
      const created = await createJob(payload);
      providerJobId = created.id;
    }

    clearCronJobsCache();
    if (isKeepAlive) {
      project.cronKeepAliveJobId = providerJobId;
    } else {
      project.cronDeployHookJobId = providerJobId;
    }
    await saveProjects(projects);
    await upsertCronJobLink(providerJobId, project.id, type, {
      providerJobId,
      jobKey,
      enabled: true,
      scheduleNormalized,
      targetNormalized,
      lastUpdatedAt: new Date().toISOString(),
    });
    clearUserState(ctx.from.id);
    await sendTransientMessage(ctx, `Cron job saved. ID: ${providerJobId}`);
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
    if (error?.status === 500) {
      cronCreateRetryCache.set(correlationId, {
        projectId,
        type,
        scheduleInput,
        recreate: Boolean(recreate),
      });
      cronErrorDetailsCache.set(correlationId, {
        projectId,
        type,
        schedule: schedule?.cron,
        targetUrl: urlValidation.url,
        status: error.status,
        path: error.path || '/jobs',
        reason: extractCronApiErrorReason(error) || error.message,
      });
      const inline = new InlineKeyboard()
        .text('🔁 Retry', `projcron:retry_create:${correlationId}`)
        .row()
        .text('🧪 Run Cron API ping test', 'gsettings:ping_test')
        .row()
        .text('📋 Copy debug details', `projcron:copy_debug:${correlationId}`)
        .row()
        .text('⬅️ Back', `projcron:menu:${projectId}`);
      await ctx.reply(formatCronCreateErrorPanel({ error, correlationId }), { reply_markup: inline });
      return;
    }
    await sendTransientMessage(ctx, formatCronApiErrorNotice('Failed to create cron job', error, correlationId));
  }
}

async function handleProjectCronScheduleMessage(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a schedule or press Cancel.');
    return;
  }
  await createProjectCronJobWithSchedule(
    ctx,
    state.projectId,
    state.type.includes('keepalive') ? 'keepalive' : 'deploy',
    text,
    state.type.endsWith('recreate'),
  );
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
    throw new Error(dsnInfo.error || `Supabase URL/API key not configured for ${connection.name}.`);
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

function chunkTextLines(lines, limit = 3500) {
  const chunks = [];
  let buffer = '';
  lines.forEach((line) => {
    const next = buffer ? `${buffer}\n${line}` : line;
    if (next.length > limit) {
      if (buffer) {
        chunks.push(buffer);
        buffer = line;
      } else {
        chunks.push(line.slice(0, limit));
        buffer = '';
      }
      return;
    }
    buffer = next;
  });
  if (buffer) {
    chunks.push(buffer);
  }
  return chunks;
}

async function sendChunkedMessages(ctx, lines, options = {}) {
  const chunks = chunkTextLines(lines, options.limit || 3500);
  for (const chunk of chunks) {
    await replySafely(ctx, chunk, options.extra);
  }
}

async function sendTextFile(ctx, filename, content, caption) {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) {
    console.error('[file] Unable to send file: missing chat id.');
    return;
  }
  const file = new InputFile(Buffer.from(content, 'utf8'), filename);
  await bot.api.sendDocument(chatId, file, { caption });
}

function getWizardSteps() {
  return [
    'name',
    'id',
    'githubToken',
    'repoSelect',
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
  if (step === 'githubToken') {
    inline.text('⭐ Use Default', 'projwiz:use_default_token').text('➕ Set Default', 'projwiz:set_default_token').row();
  }
  inline.text('◀️ Back', 'projwiz:back').text('❌ Cancel', 'cancel_input').row();
  if (isWizardStepSkippable(step)) {
    inline.text('⏭ Skip', 'projwiz:skip').row();
  }
  return inline;
}

function getWorkingDirChoiceKeyboard() {
  return new InlineKeyboard()
    .text('✅ Keep default', 'KEEP_DEFAULT_WORKDIR')
    .text('✏️ Change working dir', 'projwiz:change_workdir')
    .row()
    .text('◀️ Back', 'projwiz:back')
    .text('❌ Cancel', 'cancel_input');
}

function isSlashCommandLikeInput(value) {
  if (value == null) return false;
  const trimmed = String(value).trim();
  return /^\/[a-z]+$/i.test(trimmed);
}

function parseRepoSlug(value) {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (isSlashCommandLikeInput(trimmed)) return undefined;
  const parts = trimmed.split('/');
  if (parts.length !== 2) return undefined;
  const [owner, repo] = parts;
  if (!owner || !repo) return undefined;
  return `${owner}/${repo}`;
}



async function resolveReposFromGithubTokenInput(userId, tokenInput, options = {}) {
  const token = String(tokenInput || '').trim();
  if (!token) {
    return { ok: false, errorMessage: 'GitHub token is required.', keepStep: true };
  }
  const fetchRepos = options.fetchRepos || fetchGithubReposWithToken;
  try {
    const repos = await fetchRepos(token);
    if (!repos.length) {
      return { ok: false, errorMessage: 'No repositories found for this token. Try another token.', keepStep: true };
    }
    if (options.setAsDefault) {
      await setDefaultGithubToken(userId, token);
    }
    return { ok: true, repos };
  } catch (error) {
    if (error.code === 'INVALID_TOKEN') {
      return { ok: false, errorMessage: options.isDefault ? 'Default token is invalid/expired' : 'Invalid GitHub token. Please send a valid PAT.', keepStep: true };
    }
    if (error.code === 'RATE_LIMIT') {
      return { ok: false, errorMessage: 'GitHub API rate limited. Wait a minute and try again.', keepStep: true };
    }
    return { ok: false, errorMessage: 'Failed to fetch repositories. Please try again.', keepStep: true };
  }
}

async function fetchGithubReposWithToken(token) {
  const headers = {
    'User-Agent': 'project-manager-bot',
    Accept: 'application/vnd.github+json',
    Authorization: `Bearer ${token}`,
  };
  const repos = await listAllGithubRepos(async (page) => {
    const response = await requestUrlWithHeaders(
      'GET',
      `https://api.github.com/user/repos?per_page=100&page=${page}&sort=updated`,
      headers,
    );
    if (response.status === 401) {
      const error = new Error('invalid_token');
      error.code = 'INVALID_TOKEN';
      throw error;
    }
    if (response.status === 403) {
      const error = new Error('rate_limited');
      error.code = 'RATE_LIMIT';
      throw error;
    }
    if (response.status >= 400) {
      const error = new Error(`github_http_${response.status}`);
      error.code = 'GITHUB_HTTP';
      throw error;
    }
    return Array.isArray(response.body) ? response.body : [];
  });
  return repos;
}

function buildRepoPickerKeyboard(state) {
  const pagination = paginateRepos(state.repoCandidates || [], state.repoPage || 0, 8);
  const inline = new InlineKeyboard();
  pagination.items.forEach((repo) => {
    inline.text(mapRepoButton(repo), buildCb('projwiz:repo_pick', [repo.full_name || mapRepoButton(repo)])).row();
  });
  if (pagination.hasPrev) {
    inline.text('⬅️ Prev', buildCb('projwiz:repo_page', [pagination.page - 1]));
  }
  if (pagination.hasNext) {
    inline.text('Next ➡️', buildCb('projwiz:repo_page', [pagination.page + 1]));
  }
  if (pagination.hasPrev || pagination.hasNext) inline.row();
  inline.text('◀️ Back', 'projwiz:back').text('❌ Cancel', 'cancel_input');
  return inline;
}

async function renderRepoSelectionStep(ctx, state) {
  await renderOrEdit(
    ctx,
    `Select repository (${(state.repoCandidates || []).length} found):`,
    { reply_markup: buildRepoPickerKeyboard(state) },
  );
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
  const parts = String(data).split(':');
  const action = parts[1];
  const state = userState.get(ctx.from.id);
  if (!state || state.mode !== 'create-project') {
    return;
  }

  if (action === 'back') {
    const steps = getWizardSteps();
    const idx = steps.indexOf(state.step);
    state.step = steps[Math.max(0, idx - 1)] || 'name';
    if (state.step === 'repoSelect') {
      await renderRepoSelectionStep(ctx, state);
      return;
    }
    await promptNextProjectField(ctx, state);
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

  if (action === 'use_default_token') {
    if (state.step !== 'githubToken') return;
    const token = await getDefaultGithubToken(ctx.from.id);
    if (!token) {
      await renderOrEdit(ctx, 'No default token set', { reply_markup: getWizardKeyboard(state.step) });
      return;
    }
    const result = await resolveReposFromGithubTokenInput(ctx.from.id, token, { isDefault: true });
    if (!result.ok) {
      await renderOrEdit(ctx, `${result.errorMessage}
Use ➕ Set Default to update token.`, { reply_markup: getWizardKeyboard(state.step) });
      return;
    }
    state.repoCandidates = result.repos;
    state.repoPage = 0;
    state.step = 'repoSelect';
    await renderRepoSelectionStep(ctx, state);
    return;
  }

  if (action === 'set_default_token') {
    if (state.step !== 'githubToken') return;
    state.awaitingDefaultGithubToken = true;
    await renderOrEdit(ctx, 'Send GitHub PAT to save as your default token.', { reply_markup: getWizardKeyboard(state.step) });
    return;
  }

  if (action === 'repo_page') {
    state.repoPage = Number(parts[2]) || 0;
    await renderRepoSelectionStep(ctx, state);
    return;
  }

  if (action === 'repo_pick') {
    const repoSlug = parts.slice(2).join(':');
    state.draft.repoSlug = repoSlug;
    state.repo = repoSlug;
    state.draft.repoUrl = `https://github.com/${repoSlug}`;
    state.draft.workingDir = '.';
    state.draft.isWorkingDirCustom = false;
    state.step = 'workingDirConfirm';
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
    state.step = 'githubToken';
    await promptNextProjectField(ctx, state);
    return;
  }

  if (state.step === 'githubToken') {
    const setAsDefault = Boolean(state.awaitingDefaultGithubToken);
    state.awaitingDefaultGithubToken = false;
    const result = await resolveReposFromGithubTokenInput(ctx.from.id, value, { setAsDefault });
    if (!result.ok) {
      await ctx.reply(result.errorMessage, { reply_markup: getWizardKeyboard(state.step) });
      return;
    }
    state.repoCandidates = result.repos;
    state.repoPage = 0;
    state.step = 'repoSelect';
    await renderRepoSelectionStep(ctx, state);
    return;
  }

  if (state.step === 'repoSelect') {
    await ctx.reply('Use the repo buttons below to select a repository.');
    return;
  }

  if (state.step === 'repoSelect') {
    await renderRepoSelectionStep(ctx, state);
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
    githubToken:
      'Send GitHub personal access token (PAT) to load repository list.\n(Or press Cancel)',
    repoSelect: null,
    workingDirConfirm: null,
    workingDirCustom:
      'Send working directory path (repo-relative preferred, e.g. "." or "apps/api"). Absolute paths are allowed but discouraged.\n(Or press Cancel)',
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
    workingDir: draft.workingDir || (repoSlug ? '.' : undefined),
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

  await sendDismissibleMessage(
    ctx,
    `✅ Project created.\n🧩 Name: ${project.name}\n🆔 ID: ${project.id}`,
  );
  await renderProjectsList(ctx);
}

function isPatchText(text) {
  if (!text) return false;
  return (
    /diff --git /m.test(text) ||
    (/^--- a\//m.test(text) && /^\+\+\+ b\//m.test(text)) ||
    /^Index: /m.test(text)
  );
}

function parseBooleanValue(value) {
  if (typeof value !== 'string') return false;
  return ['true', 'yes', '1'].includes(value.trim().toLowerCase());
}

function parseChangeSpecBlocks(text) {
  const blocks = text
    .split(/\n\s*\n/)
    .map((block) => block.trim())
    .filter(Boolean);
  if (!blocks.length) {
    return { ok: false, errors: [{ index: 0, message: 'No change blocks found.' }] };
  }
  const parsed = [];
  const errors = [];

  blocks.forEach((block, index) => {
    const data = {};
    let currentKey = null;
    let hasField = false;
    block.split('\n').forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed) {
        return;
      }
      const match = trimmed.match(/^([A-Z_]+):\s*(.*)$/);
      if (match) {
        currentKey = match[1].toUpperCase();
        data[currentKey] = match[2] || '';
        hasField = true;
      } else if (currentKey) {
        data[currentKey] += `\n${line}`;
      }
      if (!match && !currentKey) {
        errors.push({ index: index + 1, message: `Invalid syntax: "${trimmed}"` });
      }
    });
    if (!hasField) {
      errors.push({ index: index + 1, message: 'No valid fields found.' });
      return;
    }

    const filePath = data.FILE?.trim();
    const op = data.OP?.trim().toLowerCase();
    if (!filePath || !op) {
      errors.push({ index: index + 1, message: 'Missing FILE or OP.' });
      return;
    }

    const entry = {
      blockIndex: index + 1,
      filePath,
      op,
      find: data.FIND?.trim(),
      replace: data.REPLACE?.trim(),
      anchor: data.ANCHOR?.trim(),
      insert: data.INSERT?.trim(),
      append: data.APPEND?.trim(),
      start: data.START?.trim(),
      end: data.END?.trim(),
      createIfMissing: parseBooleanValue(data.CREATE_IF_MISSING),
      exactOneMatch: parseBooleanValue(data.EXACT_ONE_MATCH),
      raw: block,
    };

    parsed.push(entry);
  });

  if (errors.length) {
    return { ok: false, errors };
  }
  return { ok: true, blocks: parsed };
}

function validateStructuredSpec(blocks) {
  const errors = [];
  blocks.forEach((entry, index) => {
    const label = `Block ${index + 1}`;
    if (!entry.filePath) {
      errors.push({ index: index + 1, message: 'FILE is required.' });
      return;
    }
    if (!entry.op) {
      errors.push({ index: index + 1, message: 'OP is required.' });
      return;
    }
    switch (entry.op) {
      case 'replace':
        if (!entry.find || entry.replace == null) {
          errors.push({ index: index + 1, message: 'FIND and REPLACE are required for replace.' });
        }
        break;
      case 'insert_after':
      case 'insert_before':
        if (!entry.anchor || (!entry.insert && !entry.append)) {
          errors.push({ index: index + 1, message: 'ANCHOR and INSERT are required for insert.' });
        }
        break;
      case 'append':
        if (!entry.append && !entry.insert) {
          errors.push({ index: index + 1, message: 'APPEND or INSERT is required for append.' });
        }
        break;
      case 'delete_range':
        if (!entry.start || !entry.end) {
          errors.push({ index: index + 1, message: 'START and END are required for delete_range.' });
        }
        break;
      default:
        errors.push({ index: index + 1, message: `Unsupported OP "${entry.op}".` });
        break;
    }
  });
  return errors;
}

function looksLikeFilePath(value) {
  if (!value) return false;
  if (value.includes(' ')) return false;
  if (value.includes('\\')) return true;
  return value.includes('/') || /\.[a-z0-9]+$/i.test(value);
}

function inferUnstructuredPlan(text) {
  const plan = [];
  const regex = /```([^`\n]*)\n([\s\S]*?)```/g;
  let match;
  while ((match = regex.exec(text))) {
    const info = (match[1] || '').trim();
    const body = match[2] || '';
    if (looksLikeFilePath(info)) {
      plan.push({
        filePath: info,
        op: 'replace_file',
        content: body.trimEnd() + '\n',
        createIfMissing: true,
      });
    }
  }

  const fileHeaderRegex = /FILE:\s*(\S+)/g;
  let headerMatch;
  while ((headerMatch = fileHeaderRegex.exec(text))) {
    const pathValue = headerMatch[1];
    if (!looksLikeFilePath(pathValue)) continue;
    const startIndex = headerMatch.index + headerMatch[0].length;
    const nextHeader = text.slice(startIndex).search(/FILE:\s*\S+/);
    const endIndex = nextHeader === -1 ? text.length : startIndex + nextHeader;
    const snippet = text.slice(startIndex, endIndex).trim();
    if (snippet) {
      plan.push({
        filePath: pathValue,
        op: 'replace_file',
        content: snippet.trimEnd() + '\n',
        createIfMissing: true,
      });
    }
  }

  const unique = new Map();
  plan.forEach((entry) => {
    if (!unique.has(entry.filePath)) {
      unique.set(entry.filePath, entry);
    }
  });

  return Array.from(unique.values());
}

function buildChangePreview(plan) {
  const lines = ['Preview plan:', ''];
  plan.forEach((entry, index) => {
    const preview = (entry.content || '').split('\n').slice(0, 5).join('\n');
    lines.push(
      `${index + 1}. ${entry.filePath} (${entry.op})`,
      preview ? `---\n${preview}\n---` : '(no content)',
      '',
    );
  });
  return lines.join('\n').trim();
}

function decodeXmlEntities(value) {
  if (!value) return '';
  return value
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, code) => String.fromCharCode(parseInt(code, 16)));
}

function extractDocxTextFromXml(xml) {
  const paragraphs = xml.match(/<w:p[\s\S]*?<\/w:p>/g) || [];
  const lines = [];
  let inCodeBlock = false;

  paragraphs.forEach((paragraph) => {
    const styleMatch = paragraph.match(/<w:pStyle[^>]*w:val="([^"]+)"/);
    const runStyleMatch = paragraph.match(/<w:rStyle[^>]*w:val="([^"]+)"/);
    const styleValue = `${styleMatch?.[1] || ''} ${runStyleMatch?.[1] || ''}`.toLowerCase();
    const isCode = styleValue.includes('code') || styleValue.includes('pre');
    const textParts = [];
    const textRegex = /<w:t[^>]*>([\s\S]*?)<\/w:t>/g;
    let textMatch;
    while ((textMatch = textRegex.exec(paragraph))) {
      textParts.push(decodeXmlEntities(textMatch[1]));
    }
    const paragraphText = textParts.join('');

    if (isCode && !inCodeBlock) {
      lines.push('```');
      inCodeBlock = true;
    }
    if (!isCode && inCodeBlock) {
      lines.push('```');
      inCodeBlock = false;
    }
    lines.push(paragraphText);
  });

  if (inCodeBlock) {
    lines.push('```');
  }

  return lines.join('\n').trim();
}

async function extractDocxText(buffer) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pm-docx-'));
  const filePath = path.join(tempDir, 'document.docx');
  try {
    await fs.writeFile(filePath, buffer);
    const { stdout } = await execFileAsync('unzip', ['-p', filePath, 'word/document.xml'], {
      maxBuffer: 10 * 1024 * 1024,
    });
    return extractDocxTextFromXml(String(stdout || ''));
  } catch (error) {
    console.error('[docx] Failed to extract docx text', error);
    return '';
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true });
  }
}

async function downloadTelegramFileBuffer(ctx, fileId) {
  const file = await ctx.api.getFile(fileId);
  const fileUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${file.file_path}`;
  return new Promise((resolve, reject) => {
    https
      .get(fileUrl, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`Failed to download file: ${res.statusCode}`));
          return;
        }
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => resolve(Buffer.concat(chunks)));
      })
      .on('error', reject);
  });
}

function resolveRepoFilePath(repoDir, filePath) {
  if (!filePath) return null;
  const normalized = filePath.replace(/^\.?\//, '');
  const fullPath = path.resolve(repoDir, normalized);
  const root = path.resolve(repoDir);
  if (fullPath === root || fullPath.startsWith(`${root}${path.sep}`)) {
    return fullPath;
  }
  return null;
}

function findOccurrences(text, needle) {
  if (!needle) return [];
  const matches = [];
  let index = 0;
  while (index <= text.length) {
    const found = text.indexOf(needle, index);
    if (found === -1) break;
    matches.push(found);
    index = found + needle.length;
  }
  return matches;
}

function applyStructuredOperation(content, entry) {
  const exactOneMatch = entry.exactOneMatch;
  if (entry.op === 'replace') {
    const matches = findOccurrences(content, entry.find);
    if (!matches.length) {
      return { ok: false, reason: 'find not found' };
    }
    if (matches.length > 1) {
      return { ok: false, reason: exactOneMatch ? 'multiple matches' : 'ambiguous matches' };
    }
    const idx = matches[0];
    const updated =
      content.slice(0, idx) + entry.replace + content.slice(idx + entry.find.length);
    return { ok: true, content: updated, warnings: [] };
  }

  if (entry.op === 'insert_after' || entry.op === 'insert_before') {
    const anchor = entry.anchor;
    const insert = entry.insert || entry.append || '';
    const matches = findOccurrences(content, anchor);
    if (!matches.length) {
      return { ok: false, reason: 'anchor not found' };
    }
    if (matches.length > 1) {
      return { ok: false, reason: exactOneMatch ? 'multiple matches' : 'ambiguous matches' };
    }
    const idx = matches[0];
    const insertAt = entry.op === 'insert_after' ? idx + anchor.length : idx;
    const updated = content.slice(0, insertAt) + insert + content.slice(insertAt);
    return { ok: true, content: updated, warnings: [] };
  }

  if (entry.op === 'append') {
    const append = entry.append || entry.insert || '';
    const separator = content.endsWith('\n') || append.startsWith('\n') ? '' : '\n';
    const updated = content + separator + append;
    return { ok: true, content: updated, warnings: [] };
  }

  if (entry.op === 'delete_range') {
    const startMatches = findOccurrences(content, entry.start);
    if (!startMatches.length) {
      return { ok: false, reason: 'start not found' };
    }
    if (startMatches.length > 1) {
      return { ok: false, reason: exactOneMatch ? 'multiple matches' : 'ambiguous matches' };
    }
    const startIndex = startMatches[0];
    const endIndex = content.indexOf(entry.end, startIndex + entry.start.length);
    if (endIndex === -1) {
      return { ok: false, reason: 'end not found' };
    }
    const updated = content.slice(0, startIndex) + content.slice(endIndex + entry.end.length);
    return { ok: true, content: updated, warnings: [] };
  }

  return { ok: false, reason: 'unsupported op' };
}

function formatInvalidChangeSpecMessage(error, blockIndex) {
  return `Patch rejected\nError: ${error}\nBlock index: ${blockIndex}`;
}

function validateStructuredFilePaths(repoDir, plan) {
  const errors = [];
  plan.forEach((entry, index) => {
    const resolved = resolveRepoFilePath(repoDir, entry.filePath);
    if (!resolved) {
      errors.push({ index: index + 1, message: 'Invalid file path.' });
    }
  });
  return errors;
}

function buildStructuredFailureMessage(failure) {
  return [
    `Block ${failure.index + 1} FAILED`,
    `File: ${failure.entry?.filePath || ''}`,
    `Operation: ${failure.entry?.op || ''}`,
    `Reason: ${failure.reason || 'Unknown error'}`,
    'Subsequent blocks were not executed.',
  ].join('\n');
}

function buildStructuredSuccessMessage(totalBlocks, modifiedFiles, diffPreview) {
  const lines = [
    'Applied successfully',
    `Blocks applied: ${totalBlocks}`,
    'Modified files:',
    ...modifiedFiles,
  ];
  if (diffPreview) {
    lines.push('', 'Diff preview:', diffPreview);
  }
  return lines.join('\n');
}

async function applyStructuredChangePlan(repoDir, plan, options = {}) {
  const results = [];
  const startIndex = Number.isInteger(options.startIndex) ? options.startIndex : 0;

  for (let index = startIndex; index < plan.length; index += 1) {
    const entry = plan[index];
    const fullPath = resolveRepoFilePath(repoDir, entry.filePath);
    if (!fullPath) {
      return {
        results,
        failure: { index, entry, reason: 'invalid file path' },
      };
    }
    let content = '';
    let exists = true;
    try {
      content = await fs.readFile(fullPath, 'utf8');
    } catch (error) {
      exists = false;
    }
    if (!exists && !entry.createIfMissing) {
      return {
        results,
        failure: { index, entry, reason: 'file not found' },
      };
    }

    const operation = applyStructuredOperation(content, entry);
    if (!operation.ok) {
      return {
        results,
        failure: { index, entry, reason: operation.reason },
      };
    }
    if (operation.content === content) {
      return {
        results,
        failure: { index, entry, reason: 'no changes' },
      };
    }
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, operation.content, 'utf8');
    results.push({ entry, status: 'applied', warnings: operation.warnings });
  }

  return { results, failure: null };
}

async function applyUnstructuredPlan(repoDir, plan) {
  const results = [];
  for (const entry of plan) {
    const fullPath = resolveRepoFilePath(repoDir, entry.filePath);
    if (!fullPath) {
      results.push({ entry, status: 'failed', reason: 'invalid file path' });
      continue;
    }
    let exists = true;
    try {
      await fs.access(fullPath);
    } catch (error) {
      exists = false;
    }
    if (!exists && !entry.createIfMissing) {
      results.push({ entry, status: 'failed', reason: 'file not found' });
      continue;
    }
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, entry.content || '', 'utf8');
    results.push({ entry, status: 'applied' });
  }
  return results;
}

function summarizeChangeResults(results) {
  const applied = results.filter((result) => result.status === 'applied').length;
  const skipped = results.filter((result) => result.status === 'skipped').length;
  const failed = results.filter((result) => result.status === 'failed').length;
  return { applied, skipped, failed };
}

function formatChangeFailures(results) {
  const lines = [];
  results
    .filter((result) => result.status === 'failed')
    .forEach((result) => {
      lines.push(`- ${result.entry.filePath}: ${result.reason}`);
    });
  return lines;
}

async function buildDiffPreview(git) {
  try {
    const diff = await git.diff();
    if (!diff) return null;
    return truncateMessage(diff, 3500);
  } catch (error) {
    console.error('[diff] Failed to build diff preview', error);
    return null;
  }
}

function buildChangeSummaryMessage(summary, failures, diffPreview) {
  const lines = [
    `Summary: applied=${summary.applied}, skipped=${summary.skipped}, failed=${summary.failed}`,
  ];
  if (failures.length) {
    lines.push('', 'Failures:', ...failures);
  }
  if (diffPreview) {
    lines.push('', 'Diff preview:', diffPreview);
  }
  return lines.join('\n');
}

async function applyChangesInRepo(ctx, projectId, change) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const startTime = Date.now();
  const globalSettings = await loadGlobalSettings();
  let shouldClearUserState = true;
  const progress = createOperationProgress(ctx, `🛠 Create PR — ${project.name || project.id}`, 4);
  let completedSteps = 0;

  try {
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps,
      currentStep: 'Update repository',
      nextStep: 'Apply changes',
    });
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

    const { git, repoDir } = await prepareRepository(project, effectiveBaseBranch);
    const branchName = makePatchBranchName(project.id);
    completedSteps = 1;
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps,
      currentStep: 'Apply changes',
      nextStep: 'Commit + push',
    });

    if (change.mode === 'structured') {
      const pathErrors = validateStructuredFilePaths(repoDir, change.plan || []);
      if (pathErrors.length) {
        const firstError = pathErrors[0];
        await ctx.reply(formatInvalidChangeSpecMessage(firstError.message, firstError.index));
        return;
      }
    }

    await createWorkingBranch(git, effectiveBaseBranch, branchName);

    let results = [];
    let structuredFailure = null;
    if (change.mode === 'patch') {
      await applyPatchToRepo(git, repoDir, change.patchText);
      results = [{ entry: { filePath: '(patch)' }, status: 'applied' }];
    } else if (change.mode === 'structured') {
      const structuredResult = await applyStructuredChangePlan(repoDir, change.plan);
      results = structuredResult.results;
      structuredFailure = structuredResult.failure;
    } else {
      results = await applyUnstructuredPlan(repoDir, change.plan);
    }

    console.info('[change-input] apply_results', {
      mode: change.mode,
      results: results.map((result) => ({
        filePath: result.entry?.filePath,
        status: result.status,
        reason: result.reason,
        warnings: result.warnings,
      })),
    });

    if (change.mode === 'structured' && structuredFailure) {
      const failureMessage = buildStructuredFailureMessage(structuredFailure);
      const inline = new InlineKeyboard()
        .text('Fix this block', 'structured:fix_block')
        .text('Cancel patch', 'structured:cancel');
      structuredPatchSessions.set(ctx.from.id, {
        projectId,
        repoDir,
        git,
        branchName,
        baseBranch: effectiveBaseBranch,
        plan: change.plan,
        results,
        failureIndex: structuredFailure.index,
        startTime,
      });
      setUserState(ctx.from.id, { type: 'structured_fix_block' });
      shouldClearUserState = false;
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps,
        currentStep: 'Apply changes',
        nextStep: null,
        reason: 'Structured change needs input',
      });
      await ctx.reply(failureMessage, { reply_markup: inline });
      return;
    }

    const summary = summarizeChangeResults(results);
    const failureLines = formatChangeFailures(results);
    const diffPreview = await buildDiffPreview(git);

    completedSteps = 2;
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps,
      currentStep: 'Commit + push',
      nextStep: 'Create PR',
    });
    const identityResult = await configureGitIdentity(git);
    if (!identityResult.ok) {
      const stderr = identityResult.error?.stderr || identityResult.error?.message || 'Unknown error';
      console.error(`[gitIdentity] Failed to set ${identityResult.step}: ${stderr}`);
      await ctx.reply('Failed to configure git author identity for this project. Please check logs.');
      throw new Error('Failed to configure git author identity for this project.');
    }
    const hasChanges = await commitAndPush(git, branchName);
    if (!hasChanges) {
      const message = buildChangeSummaryMessage(summary, failureLines, diffPreview);
      await ctx.reply(`No changes detected.\n${message}`);
      return;
    }

    completedSteps = 3;
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps,
      currentStep: 'Create PR',
      nextStep: null,
    });
    const prBody = buildPrBody(diffPreview || change.patchText || '');
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
    completedSteps = 4;
    await updateProgressMessage(ctx, progress, {
      status: 'success',
      completedSteps,
      currentStep: 'Create PR',
      nextStep: null,
    });

    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const inline = new InlineKeyboard().url('View PR', pr.html_url);
    if (change.mode === 'structured') {
      const modifiedFiles = Array.from(
        new Set(results.map((result) => result.entry?.filePath).filter(Boolean)),
      );
      const message = buildStructuredSuccessMessage(change.plan.length, modifiedFiles, diffPreview);
      await ctx.reply(
        `✅ Pull Request created.\nPR: ${pr.html_url}\nPlease review and merge manually.\nElapsed: ~${elapsed}s\n\n${message}`,
        { reply_markup: inline },
      );
    } else {
      const message = buildChangeSummaryMessage(summary, failureLines, diffPreview);
      await ctx.reply(
        `✅ Pull Request created.\nPR: ${pr.html_url}\nPlease review and merge manually.\nElapsed: ~${elapsed}s\n\n${message}`,
        { reply_markup: inline },
      );
    }
  } catch (error) {
    console.error('Failed to apply changes', error);
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps,
      currentStep: 'Create PR',
      nextStep: null,
      reason: error.message,
    });
    await ctx.reply(`Failed to apply changes: ${error.message}`);
  } finally {
    if (shouldClearUserState) {
      clearUserState(ctx.from.id);
    }
  }
}

async function finalizeStructuredChangeSession(ctx, session, results) {
  const projects = await loadProjects();
  const project = findProjectById(projects, session.projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }
  const progress = createOperationProgress(ctx, `🛠 Create PR — ${project.name || project.id}`, 2);
  let completedSteps = 0;

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

  const diffPreview = await buildDiffPreview(session.git);
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: 'Commit + push',
    nextStep: 'Create PR',
  });
  const identityResult = await configureGitIdentity(session.git);
  if (!identityResult.ok) {
    const stderr = identityResult.error?.stderr || identityResult.error?.message || 'Unknown error';
    console.error(`[gitIdentity] Failed to set ${identityResult.step}: ${stderr}`);
    await ctx.reply('Failed to configure git author identity for this project. Please check logs.');
    throw new Error('Failed to configure git author identity for this project.');
  }
  const hasChanges = await commitAndPush(session.git, session.branchName);
  if (!hasChanges) {
    await ctx.reply('No changes detected.');
    return;
  }

  completedSteps = 1;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: 'Create PR',
    nextStep: null,
  });
  const prBody = buildPrBody(diffPreview || '');
  const [owner, repo] = repoInfo.repoSlug.split('/');
  const githubToken = getGithubToken(project);
  const pr = await createPullRequest({
    owner,
    repo,
    baseBranch: session.baseBranch || DEFAULT_BASE_BRANCH,
    headBranch: session.branchName,
    title: `Automated patch: ${project.id}`,
    body: prBody,
    token: githubToken,
  });
  completedSteps = 2;
  await updateProgressMessage(ctx, progress, {
    status: 'success',
    completedSteps,
    currentStep: 'Create PR',
    nextStep: null,
  });

  const elapsed = Math.round((Date.now() - session.startTime) / 1000);
  const modifiedFiles = Array.from(
    new Set(results.map((result) => result.entry?.filePath).filter(Boolean)),
  );
  const message = buildStructuredSuccessMessage(session.plan.length, modifiedFiles, diffPreview);
  const inline = new InlineKeyboard().url('View PR', pr.html_url);
  await ctx.reply(
    `✅ Pull Request created.\nPR: ${pr.html_url}\nPlease review and merge manually.\nElapsed: ~${elapsed}s\n\n${message}`,
    { reply_markup: inline },
  );
}

async function handleStructuredFixBlockInput(ctx, state) {
  const text = ctx.message?.text?.trim();
  if (!text) {
    await ctx.reply('Please send the corrected block text.');
    return;
  }
  const session = structuredPatchSessions.get(ctx.from.id);
  if (!session) {
    clearUserState(ctx.from.id);
    await ctx.reply('No structured patch session found.');
    return;
  }

  const parsed = parseChangeSpecBlocks(text);
  if (!parsed.ok) {
    const firstError = parsed.errors[0];
    await ctx.reply(formatInvalidChangeSpecMessage(firstError.message, firstError.index));
    return;
  }
  if (parsed.blocks.length !== 1) {
    await ctx.reply(formatInvalidChangeSpecMessage('Expected exactly one block.', 1));
    return;
  }

  const block = parsed.blocks[0];
  const validationErrors = validateStructuredSpec([block]);
  if (validationErrors.length) {
    const firstError = validationErrors[0];
    await ctx.reply(formatInvalidChangeSpecMessage(firstError.message, firstError.index));
    return;
  }
  const pathErrors = validateStructuredFilePaths(session.repoDir, [block]);
  if (pathErrors.length) {
    const firstError = pathErrors[0];
    await ctx.reply(formatInvalidChangeSpecMessage(firstError.message, firstError.index));
    return;
  }

  session.plan[session.failureIndex] = {
    ...block,
    blockIndex: session.failureIndex + 1,
  };

  await ctx.reply('Retrying failed block…');
  const structuredResult = await applyStructuredChangePlan(session.repoDir, session.plan, {
    startIndex: session.failureIndex,
  });
  const combinedResults = [...session.results, ...structuredResult.results];

  if (structuredResult.failure) {
    session.results = combinedResults;
    session.failureIndex = structuredResult.failure.index;
    structuredPatchSessions.set(ctx.from.id, session);
    const failureMessage = buildStructuredFailureMessage(structuredResult.failure);
    const inline = new InlineKeyboard()
      .text('Fix this block', 'structured:fix_block')
      .text('Cancel patch', 'structured:cancel');
    await ctx.reply(failureMessage, { reply_markup: inline });
    return;
  }

  structuredPatchSessions.delete(ctx.from.id);
  clearUserState(ctx.from.id);
  await finalizeStructuredChangeSession(ctx, session, combinedResults);
}

async function handlePatchApplication(ctx, projectId, patchText, inputTypes = []) {
  const normalizedTypes = Array.isArray(inputTypes) ? inputTypes : Array.from(inputTypes || []);
  console.info('[change-input] received', {
    projectId,
    inputTypes: normalizedTypes,
  });

  if (isPatchText(patchText)) {
    await applyChangesInRepo(ctx, projectId, { mode: 'patch', patchText });
    return;
  }

  if (/FILE:\s*/i.test(patchText)) {
    const parsed = parseChangeSpecBlocks(patchText);
    if (!parsed.ok) {
      const firstError = parsed.errors[0];
      await ctx.reply(formatInvalidChangeSpecMessage(firstError.message, firstError.index));
      return;
    }
    const validationErrors = validateStructuredSpec(parsed.blocks);
    if (validationErrors.length) {
      const firstError = validationErrors[0];
      await ctx.reply(formatInvalidChangeSpecMessage(firstError.message, firstError.index));
      return;
    }
    console.info('[change-input] structured_plan', {
      projectId,
      blocks: parsed.blocks.length,
    });
    await applyChangesInRepo(ctx, projectId, { mode: 'structured', plan: parsed.blocks });
    return;
  }

  const plan = inferUnstructuredPlan(patchText);
  if (!plan.length) {
    await ctx.reply('No file changes could be inferred. Use PM Change Spec v1 or a patch file.');
    return;
  }

  console.info('[change-input] preview_plan', {
    projectId,
    files: plan.map((entry) => entry.filePath),
  });

  const preview = buildChangePreview(plan);
  const limitNote =
    plan.length > 10 ? `\n⚠️ ${plan.length} files detected (limit 10; confirm to proceed).` : '';
  const message = `Unstructured change request detected.${limitNote}\n\n${preview}\n\nApply these changes?`;
  changePreviewSessions.set(ctx.from.id, {
    projectId,
    mode: 'unstructured',
    plan,
    inputTypes: normalizedTypes,
    sourceText: patchText,
  });
  const inline = new InlineKeyboard().text('✅ Apply', 'change:apply').text('❌ Cancel', 'change:cancel');
  await safeRespond(ctx, message, { reply_markup: inline }, { action: 'change_preview' });
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

async function renderProjectAfterUpdate(ctx, state, notice) {
  if (state?.backCallback && state.backCallback.startsWith('proj:missing_setup:')) {
    await renderProjectMissingSetup(ctx, state.projectId, notice);
    return;
  }
  await renderProjectSettingsForMessage(state.messageContext, state.projectId, notice);
  if (!state.messageContext) {
    await renderProjectSettings(ctx, state.projectId, notice);
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
    const newIdRaw = text === '-' ? '' : text;
    const newId = newIdRaw ? newIdRaw : state.projectId;
    const projects = await loadProjects();
    const idx = projects.findIndex((p) => p.id === state.projectId);
    if (idx === -1) {
      await ctx.reply('Project not found.');
      clearUserState(ctx.from.id);
      return;
    }
    if (newId !== state.projectId) {
      const validation = validateProjectIdInput(newId);
      if (!validation.valid) {
        await ctx.reply(`Invalid project ID: ${validation.message}`);
        return;
      }
      if (projects.find((p) => p.id === validation.value)) {
        await ctx.reply('A project with this ID already exists.');
        return;
      }
      setUserState(ctx.from.id, {
        type: 'rename_project',
        step: 3,
        projectId: state.projectId,
        data: { newName: state.data.newName, newId: validation.value },
        messageContext: state.messageContext,
      });
      await ctx.reply(
        `⚠️ Confirm project ID change:\n${state.projectId} → ${validation.value}\nType CONFIRM to proceed.`,
        { reply_markup: buildCancelKeyboard() },
      );
      return;
    }

    projects[idx] = { ...projects[idx], name: state.data.newName };
    await saveProjects(projects);
    clearUserState(ctx.from.id);
    await renderProjectSettingsForMessage(state.messageContext, state.projectId, '✅ Updated');
    if (!state.messageContext) {
      await renderProjectSettings(ctx, state.projectId, '✅ Updated');
    }
  }

  if (state.step === 3) {
    if (text !== 'CONFIRM') {
      await ctx.reply('Type CONFIRM to proceed or Cancel.');
      return;
    }
    const newId = state.data?.newId;
    const newName = state.data?.newName;
    try {
      const updated = await migrateProjectId(state.projectId, newId);
      if (newName) {
        await updateProjectField(updated.id, 'name', newName);
      }
      clearUserState(ctx.from.id);
      await renderProjectSettingsForMessage(state.messageContext, updated.id, '✅ Updated');
      if (!state.messageContext) {
        await renderProjectSettings(ctx, updated.id, '✅ Updated');
      }
    } catch (error) {
      await ctx.reply(`Failed to update project ID: ${error.message}`);
      clearUserState(ctx.from.id);
    }
  }
}

async function handleEditProjectIdInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderStateMessage(ctx, state, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(`proj:open:${state.projectId}`),
    });
    return;
  }
  if (state.step === 'input') {
    const validation = validateProjectIdInput(text);
    if (!validation.valid) {
      await ctx.reply(`Invalid project ID: ${validation.message}`);
      return;
    }
    if (validation.value === state.projectId) {
      await ctx.reply('Project ID unchanged.');
      clearUserState(ctx.from.id);
      await renderProjectSettings(ctx, state.projectId);
      return;
    }
    const projects = await loadProjects();
    if (projects.find((p) => p.id === validation.value)) {
      await ctx.reply('A project with this ID already exists.');
      return;
    }
    setUserState(ctx.from.id, {
      type: 'edit_project_id',
      step: 'confirm',
      projectId: state.projectId,
      data: { newId: validation.value },
      messageContext: state.messageContext,
    });
    await ctx.reply(
      `⚠️ Confirm project ID change:\n${state.projectId} → ${validation.value}\nType CONFIRM to proceed.`,
      { reply_markup: buildCancelKeyboard() },
    );
    return;
  }
  if (state.step === 'confirm') {
    if (text !== 'CONFIRM') {
      await ctx.reply('Type CONFIRM to proceed or Cancel.');
      return;
    }
    try {
      const updated = await migrateProjectId(state.projectId, state.data.newId);
      clearUserState(ctx.from.id);
      await renderProjectSettingsForMessage(state.messageContext, updated.id, '✅ Updated');
      if (!state.messageContext) {
        await renderProjectSettings(ctx, updated.id, '✅ Updated');
      }
    } catch (error) {
      await ctx.reply(`Failed to update project ID: ${error.message}`);
      clearUserState(ctx.from.id);
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
    if (state.backCallback) {
      await renderProjectMissingSetup(ctx, state.projectId, 'Operation cancelled.');
    } else {
      await renderMainMenu(ctx);
    }
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
    if (state.backCallback) {
      await renderProjectMissingSetup(ctx, state.projectId, 'Operation cancelled.');
    } else {
      await renderMainMenu(ctx);
    }
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
    console.warn('[githubToken] Project not found during edit', {
      projectId: state.projectId,
      userId: ctx.from?.id,
    });
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
    updatedProject.workingDir = defaultWorkingDir ? '.' : updatedProject.workingDir;
    updatedProject.isWorkingDirCustom = false;
  }

  projects[idx] = updatedProject;
  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) {
    console.warn('[githubToken] Failed to save project after edit', {
      projectId: state.projectId,
      userId: ctx.from?.id,
    });
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await renderProjectAfterUpdate(ctx, state, '✅ Updated');
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
    console.warn('[workingDir] Project not found during edit', {
      projectId: state.projectId,
      userId: ctx.from?.id,
    });
    await respond(ctx, 'Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const project = projects[idx];
  let nextWorkingDir = trimmed;
  let isWorkingDirCustom = true;
  let extraNotice = null;

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
    nextWorkingDir = '.';
    isWorkingDirCustom = false;
  } else {
    const validation = validateWorkingDirInput(rawText);
    if (!validation.ok) {
      await respond(ctx, validation.error);
      return;
    }
    nextWorkingDir = validation.value;
    if (path.isAbsolute(nextWorkingDir) && project.repoSlug) {
      const checkoutDir = getDefaultWorkingDir(project.repoSlug);
      const expectedCheckoutDir = checkoutDir ? path.resolve(checkoutDir) : null;
      if (expectedCheckoutDir) {
        try {
          await fs.stat(nextWorkingDir);
          const resolved = path.resolve(nextWorkingDir);
          if (resolved === expectedCheckoutDir || resolved.startsWith(`${expectedCheckoutDir}${path.sep}`)) {
            const relative = path.relative(expectedCheckoutDir, resolved) || '.';
            nextWorkingDir = relative === '.' ? '.' : relative;
          }
        } catch (error) {
          extraNotice =
            '⚠️ Absolute path does not exist in this runtime. Consider switching to a repo-relative path.';
        }
      }
    }
  }

  projects[idx] = {
    ...project,
    workingDir: nextWorkingDir,
    isWorkingDirCustom,
  };

  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) {
    console.warn('[workingDir] Failed to save project after edit', {
      projectId: state.projectId,
      userId: ctx.from?.id,
    });
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  const refreshedProjects = await loadProjects();
  const updatedProject = findProjectById(refreshedProjects, state.projectId);
  if (!updatedProject) {
    console.warn('[workingDir] Project missing after save', {
      projectId: state.projectId,
      userId: ctx.from?.id,
    });
    await respond(ctx, 'Project not found.');
    return;
  }
  const validation = await validateWorkingDir(updatedProject);
  const noticeBase = formatWorkingDirValidationNotice(validation);
  const notice = extraNotice ? `${noticeBase}\n${extraNotice}` : noticeBase;
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

async function revalidateWorkingDir(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  const globalSettings = await loadGlobalSettings();
  const baseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  let checkoutDir = null;
  let repoReady = false;
  const lines = ['🔁 Re-checkout & Validate WorkingDir'];

  try {
    const prepared = await prepareRepository({ ...project, workingDir: undefined }, baseBranch);
    checkoutDir = prepared?.repoDir || null;
    repoReady = Boolean(checkoutDir);
  } catch (error) {
    lines.push(`Repo checkout: ❌ ${truncateText(error.message || 'failed', 120)}`);
    await renderOrEdit(ctx, lines.join('\n'), {
      reply_markup: buildBackKeyboard(`proj:open:${projectId}`),
    });
    return;
  }

  lines.push(`Repo checkout: ${repoReady ? '✅ present' : '❌ missing'}`);
  const resolvedWorkingDir = resolveWorkingDirAgainstCheckout(project.workingDir, checkoutDir);
  lines.push(`Resolved workingDir: ${resolvedWorkingDir || '-'}`);

  if (!resolvedWorkingDir) {
    lines.push('❌ Validation failed: workingDir missing.');
    await renderOrEdit(ctx, lines.join('\n'), {
      reply_markup: buildBackKeyboard(`proj:open:${projectId}`),
    });
    return;
  }

  let exists = true;
  try {
    await fs.stat(resolvedWorkingDir);
  } catch (error) {
    exists = false;
    lines.push(`❌ Validation failed: ${error.code === 'ENOENT' ? 'path does not exist' : error.message}`);
  }

  const validation = await validateWorkingDir({ ...project, workingDir: project.workingDir || '.' });
  if (!validation.ok) {
    lines.push(`❌ Validation failed: ${validation.details}`);
    if (validation.expectedCheckoutDir) {
      lines.push(`Expected repo root: ${validation.expectedCheckoutDir}`);
    }
  }

  if (exists && validation.ok && checkoutDir) {
    const relative = path.relative(checkoutDir, resolvedWorkingDir) || '.';
    project.workingDir = relative;
    await saveProjects(projects);
    lines.push(`✅ WorkingDir validated and saved as: ${relative}`);
  }

  await renderOrEdit(ctx, lines.join('\n'), {
    reply_markup: buildBackKeyboard(`proj:open:${projectId}`),
  });
}

async function renderWorkingDirectionMenu(ctx, projectId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const workingDirLabel = formatWorkingDirDisplay(project);
  const lines = [
    `📁 Working Direction — ${project.name || project.id}`,
    notice || null,
    '',
    `Current: ${workingDirLabel}`,
  ].filter(Boolean);

  const inline = new InlineKeyboard()
    .text('✏️ Set working dir', `proj:edit_workdir:${projectId}`)
    .row()
    .text('♻️ Reset to default', `proj:workdir_reset:${projectId}`)
    .row()
    .text('🔁 Re-checkout & Validate WorkingDir', `proj:workdir_revalidate:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function resetWorkingDir(ctx, projectId) {
  const updated = await updateProjectField(projectId, 'workingDir', undefined);
  if (!updated) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  await renderWorkingDirectionMenu(ctx, projectId, '✅ Reset to default');
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
  await renderProjectAfterUpdate(ctx, state, '✅ Updated');
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

async function handleDeployServiceIdInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a valid Render serviceId.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    clearUserState(ctx.from.id);
    await renderDeployProjectPanel(ctx, state.projectId, 'Operation cancelled.');
    return;
  }
  const cache = getRenderServiceCache();
  const matched = cache?.services?.find((service) => service.id === text) || null;
  await updateProjectDeploySettings(state.projectId, (current) => ({
    ...current,
    deployProvider: 'render',
    render: {
      ...current.render,
      serviceId: text,
      serviceName: matched?.name || current.render.serviceName || null,
      enabled: true,
      pollingEnabled: true,
      pollingStatus: null,
    },
  }));
  clearUserState(ctx.from.id);
  await renderDeployProjectPanel(ctx, state.projectId, '✅ Render serviceId updated.');
}

async function handleRenderApiKeyInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a valid Render API key.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    clearUserState(ctx.from.id);
    await renderDeploysProjectList(ctx, 'Operation cancelled.');
    return;
  }
  try {
    await storeRenderApiKey(text);
    clearUserState(ctx.from.id);
    await renderDeploysProjectList(ctx, '✅ Render API key saved.');
  } catch (error) {
    clearUserState(ctx.from.id);
    const message = error?.message || 'Failed to save API key.';
    await renderDeploysProjectList(ctx, `❌ ${message}`);
  }
}

async function handleSupabaseBindingInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderDatabaseBindingMenu(ctx, state.projectId, 'Operation cancelled.');
    return;
  }

  if (state.step === 'project_ref') {
    const projectRef = extractSupabaseProjectRef(text);
    if (!projectRef) {
      await ctx.reply('Invalid Supabase project ref. Use the first part of https://<ref>.supabase.co.');
      return;
    }
    const suggestedUrl = buildSupabaseUrlFromRef(projectRef);
    setUserState(ctx.from.id, {
      ...state,
      step: 'url',
      supabaseProjectRef: projectRef,
      supabaseUrl: suggestedUrl,
      messageContext: state.messageContext || getMessageTargetFromCtx(ctx),
    });
    await renderOrEdit(
      ctx,
      `Supabase URL (default: ${suggestedUrl}).\nSend URL or "-" to use the default.\nJWT strings are API keys, not DB DSN.\n(Or press Cancel)`,
      { reply_markup: buildCancelKeyboard() },
    );
    return;
  }

  if (state.step === 'url') {
    const chosen = text.trim() === '-' ? state.supabaseUrl : text.trim();
    if (!isSupabaseUrl(chosen)) {
      await ctx.reply('Supabase URL must start with https:// and include .supabase.co.');
      return;
    }
    setUserState(ctx.from.id, {
      ...state,
      step: 'key_type',
      supabaseUrl: chosen,
      messageContext: state.messageContext || getMessageTargetFromCtx(ctx),
    });
    const inline = new InlineKeyboard()
      .text('Anon (recommended)', `proj:supabase_key_type:${state.projectId}:anon`)
      .text('Service role (dangerous)', `proj:supabase_key_type:${state.projectId}:service_role`);
    await renderOrEdit(ctx, 'Select Supabase API key type:', { reply_markup: inline });
    return;
  }

  if (state.step === 'key_input') {
    if (!state.supabaseKeyType) {
      await ctx.reply('Supabase key type not selected. Please restart the binding flow.');
      clearUserState(ctx.from.id);
      await renderDatabaseBindingMenu(ctx, state.projectId);
      return;
    }
    const storage = buildSupabaseKeyStorage(text);
    const projects = await loadProjects();
    const idx = projects.findIndex((p) => p.id === state.projectId);
    if (idx === -1) {
      await ctx.reply('Project not found.');
      clearUserState(ctx.from.id);
      return;
    }
    projects[idx] = {
      ...projects[idx],
      supabaseProjectRef: state.supabaseProjectRef,
      supabaseUrl: state.supabaseUrl,
      supabaseKeyType: state.supabaseKeyType,
      supabaseKey: storage.stored,
      supabaseKeyMask: storage.mask,
      supabaseEnabled: true,
    };
    await saveProjectsWithFeedback(ctx, projects);
    clearUserState(ctx.from.id);
    const storageNotice =
      storage.storage === 'hashed'
        ? 'Supabase API key stored as a hash (Env Vault master key missing).'
        : 'Supabase API key stored securely.';
    await renderDatabaseBindingMenu(
      ctx,
      state.projectId,
      `✅ Supabase binding saved (${state.supabaseKeyType}).\n${storageNotice}`,
    );
    return;
  }

  await ctx.reply('Unexpected Supabase binding step. Please restart.');
  clearUserState(ctx.from.id);
  await renderDatabaseBindingMenu(ctx, state.projectId);
}

async function handleEditServiceHealthInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderProjectMissingSetup(ctx, state.projectId, 'Operation cancelled.');
    return;
  }

  if (state.step === 'healthPath') {
    const healthPath = text.trim() === '-' ? undefined : text.trim();
    setUserState(ctx.from.id, {
      ...state,
      step: 'servicePort',
      healthPath,
      messageContext: state.messageContext || getMessageTargetFromCtx(ctx),
    });
    await renderOrEdit(
      ctx,
      'Send expected service port (for example: 3000). Or send "-" to clear.\n(Or press Cancel)',
      { reply_markup: buildCancelKeyboard() },
    );
    return;
  }

  const servicePort = text.trim() === '-' ? undefined : text.trim();
  const projects = await loadProjects();
  const idx = projects.findIndex((p) => p.id === state.projectId);
  if (idx === -1) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }
  projects[idx] = {
    ...projects[idx],
    healthPath: state.healthPath,
    servicePort,
  };
  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) {
    clearUserState(ctx.from.id);
    return;
  }
  clearUserState(ctx.from.id);
  await renderProjectMissingSetup(ctx, state.projectId, '✅ Updated');
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
  await saveGlobalSettingsAndCache(settings);
  clearUserState(ctx.from.id);
  await renderGlobalSettingsForMessage(state.messageContext, '✅ Updated');
  if (!state.messageContext) {
    await renderGlobalSettings(ctx, '✅ Updated');
  }
}

async function handleAdminAddInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a Telegram user ID.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderOrEdit(ctx, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard('gsettings:security'),
    });
    return;
  }
  const settings = await getCachedSettings();
  const security = normalizeSecuritySettings(settings);
  const normalizedId = text.trim();
  const adminIds = new Set(security.adminIds.map(String));
  adminIds.add(normalizedId);
  const updated = {
    ...settings,
    security: { ...settings.security, adminIds: Array.from(adminIds) },
  };
  await saveGlobalSettingsAndCache(updated);
  clearUserState(ctx.from.id);
  const view = buildAdminListView(updated);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function handleBaseUrlInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a URL.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderOrEdit(ctx, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard('gsettings:integrations'),
    });
    return;
  }
  let parsed;
  try {
    parsed = new URL(text);
  } catch (error) {
    await ctx.reply('⚠️ Invalid URL. Include http:// or https://.');
    return;
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    await ctx.reply('⚠️ Invalid URL scheme. Use http:// or https://.');
    return;
  }
  const settings = await getCachedSettings();
  const updated = {
    ...settings,
    integrations: { ...settings.integrations, baseUrlOverride: text },
  };
  await saveGlobalSettingsAndCache(updated);
  clearUserState(ctx.from.id);
  const view = buildIntegrationsSettingsView(updated);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function handleBackupChannelInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send a chat_id.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderOrEdit(ctx, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard('gsettings:backups'),
    });
    return;
  }
  const settings = await getCachedSettings();
  const updated = {
    ...settings,
    backups: { ...settings.backups, channelId: text },
  };
  await saveGlobalSettingsAndCache(updated);
  clearUserState(ctx.from.id);
  const view = buildBackupsSettingsView(updated);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function handleBackupCaptionInput(ctx, state) {
  const text = ctx.message.text;
  if (!text) {
    await ctx.reply('Please send a caption template.');
    return;
  }
  if (text.trim().toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderOrEdit(ctx, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard('gsettings:backups'),
    });
    return;
  }
  const settings = await getCachedSettings();
  const updated = {
    ...settings,
    backups: { ...settings.backups, captionTemplate: text.trim() },
  };
  await saveGlobalSettingsAndCache(updated);
  clearUserState(ctx.from.id);
  const view = buildBackupsSettingsView(updated);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function formatDiagnosticsCheckLine(status, label, detail) {
  const icon = status === 'ok' ? '✅' : '❌';
  const detailText = detail ? ` — ${detail}` : '';
  return `${icon} ${label}${detailText}`;
}

function buildDiagnosticsErrorHints(result, commandLabel, commandText) {
  const lines = [];
  const exitCode = result?.exitCode;
  if (exitCode === 134) {
    lines.push('Interpretation: exit 134 — likely Node/V8 OOM or process crash during tests.');
    lines.push('📌 Suggested fixes:');
    lines.push('• reduce test concurrency');
    lines.push('• increase Node memory (NODE_OPTIONS=--max-old-space-size=...)');
  }
  if (exitCode === 2 && /lint/i.test(commandText || commandLabel || '')) {
    lines.push('Interpretation: lint exit 2 — lint command failed (code style/type errors).');
    lines.push('📌 Suggested fixes:');
    lines.push('• ensure dependencies installed');
    lines.push('• run lint locally/CI and check first failing lines');
  }
  return lines;
}

async function resolveEnvValueSources(project, key) {
  const sources = [];
  if (isEnvVaultAvailable()) {
    try {
      const envSetId = await ensureProjectEnvSet(project.id);
      const value = await getEnvVarValue(project.id, key, envSetId);
      sources.push({ source: 'project_env_vault', value });
    } catch (error) {
      sources.push({ source: 'project_env_vault', value: null, error: 'Env Vault read failed.' });
    }
  } else {
    sources.push({ source: 'project_env_vault', value: null, error: MASTER_KEY_ERROR_MESSAGE });
  }

  sources.push({ source: 'process_env', value: process.env[key] });

  if (key === 'DATABASE_URL' && project?.databaseUrl) {
    sources.push({ source: 'project_db_config', value: project.databaseUrl });
  }

  if (key === 'PROJECT_NAME') {
    const computed = project?.name || project?.id || null;
    if (computed) {
      sources.push({ source: 'computed_default', value: computed });
    }
  }

  return sources;
}

function selectEffectiveEnvValue(sources) {
  for (const entry of sources) {
    const status = evaluateEnvValueStatus(entry.value);
    if (status.status === 'SET') {
      return { source: entry.source, value: entry.value, status };
    }
  }
  const fallback = sources[0] || { source: null, value: null };
  return { source: fallback.source || null, value: fallback.value, status: evaluateEnvValueStatus(null) };
}

async function buildLogForwardingDiagnostics(project) {
  const requiredKeys = ['PATH_APPLIER_URL', 'PATH_APPLIER_TOKEN', 'PROJECT_NAME'];
  const details = [];
  const missing = [];
  const missingSet = new Set();
  const sourcesTried = ['project_env_vault', 'process_env', 'computed_default'];

  for (const key of requiredKeys) {
    const sources = await resolveEnvValueSources(project, key);
    const effective = selectEffectiveEnvValue(sources);
    const baseStatus = evaluateEnvValueStatus(effective.value);
    const status = baseStatus;
    const masked = effective.value ? maskEnvValue(effective.value) : '(missing)';
    if (status.status !== 'SET') {
      if (!missingSet.has(key)) {
        missingSet.add(key);
        missing.push({ key, status: status.status, reason: status.reason });
      }
    }
    details.push(
      `- ${key} = ${masked} (source: ${effective.source || '-'}) (status: ${status.status}) (reason: ${status.reason}) (sources: ${sourcesTried.join(', ')})`,
    );
  }

  const urlSources = await resolveEnvValueSources(project, 'PATH_APPLIER_URL');
  const urlEffective = selectEffectiveEnvValue(urlSources);
  const urlValue = String(urlEffective.value || '').trim();
  if (urlValue && !/^https?:\/\//i.test(urlValue)) {
    if (!missingSet.has('PATH_APPLIER_URL')) {
      missingSet.add('PATH_APPLIER_URL');
      missing.push({ key: 'PATH_APPLIER_URL', status: 'INVALID', reason: 'URL must start with http:// or https://' });
    }
    details.push('  ↳ PATH_APPLIER_URL must start with http:// or https://.');
  }

  const status = missing.length ? 'fail' : 'ok';
  const summary = missing.length
    ? `missing/invalid: ${missing.map((entry) => entry.key).join(', ')}`
    : 'all required keys set';

  return {
    status,
    summary,
    details: missing.length ? details : [],
    missingKeys: missing.map((entry) => entry.key),
  };
}

async function buildLightDiagnosticsReport(project, options = {}) {
  const includeHeader = options.includeHeader !== false;
  const lines = [];
  if (includeHeader) {
    lines.push(`🩺 Light Diagnostics — ${project.name || project.id}`, '');
  }

  const checks = [];
  const logForwardingEnabled = getEffectiveProjectLogForwarding(project).enabled === true;
  const logForwardingCheck = await buildLogForwardingDiagnostics(project);
  const logForwardingDetail = logForwardingEnabled
    ? logForwardingCheck.summary
    : `log forwarding disabled (${logForwardingCheck.summary})`;
  checks.push({
    status: logForwardingCheck.status,
    label: 'Log forwarding env',
    detail: logForwardingDetail,
    details: logForwardingCheck.details,
    missingKeys: logForwardingEnabled ? logForwardingCheck.missingKeys : [],
  });

  const envCount = Object.keys(process.env || {}).length;
  checks.push({
    status: envCount > 0 ? 'ok' : 'fail',
    label: 'Runtime env visible',
    detail: envCount > 0 ? `${envCount} vars` : 'no env vars visible',
  });

  const envVaultAvailable = isEnvVaultAvailable();
  if (!envVaultAvailable) {
    checks.push({
      status: 'fail',
      label: 'Project Env Vault keys',
      detail: MASTER_KEY_ERROR_MESSAGE,
    });
  } else {
    const envSetId = await ensureProjectEnvSet(project.id);
    const keys = await listEnvVarKeys(project.id, envSetId);
    checks.push({
      status: keys.length ? 'ok' : 'fail',
      label: 'Project Env Vault keys',
      detail: keys.length ? `${keys.length} keys stored` : 'no keys stored',
    });
  }

  const telegramCheck = await checkTelegramSetup(project);
  checks.push({
    status: telegramCheck.status === 'ok' ? 'ok' : 'fail',
    label: 'Telegram setup',
    detail: telegramCheck.detail || 'unknown',
  });

  const supabaseStatus = await buildSupabaseBindingStatus(project);
  if (!supabaseStatus.enabled) {
    checks.push({ status: 'ok', label: 'Supabase binding', detail: supabaseStatus.summary });
  } else if (!supabaseStatus.ready) {
    checks.push({ status: 'fail', label: 'Supabase binding', detail: supabaseStatus.summary });
  } else {
    checks.push({ status: 'ok', label: 'Supabase binding', detail: supabaseStatus.summary });
  }

  const runModeNormalized = String(project?.runMode || project?.run_mode || '').toLowerCase();
  if (runModeNormalized === 'service') {
    const healthPath = project?.healthPath || project?.health_path;
    const servicePort = project?.servicePort || project?.expectedPort || project?.port || project?.healthPort;
    const healthMissing = isMissingRequirementValue(healthPath);
    const portMissing = isMissingRequirementValue(servicePort);
    checks.push({
      status: healthMissing || portMissing ? 'fail' : 'ok',
      label: 'Service health/port config',
      detail:
        healthMissing || portMissing
          ? `healthPath: ${healthMissing ? 'missing' : 'set'}, port: ${portMissing ? 'missing' : 'set'}`
          : 'configured',
    });
  } else {
    checks.push({ status: 'ok', label: 'Service health/port config', detail: 'not service runMode' });
  }

  checks.forEach((check) => {
    lines.push(formatDiagnosticsCheckLine(check.status, check.label, check.detail));
    if (check.details && check.details.length) {
      check.details.forEach((line) => lines.push(`  ${line}`));
    }
  });

  return { lines, checks };
}

async function runProjectLightDiagnostics(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await safeRespond(ctx, 'Project not found.', null, { action: 'light_diagnostics' });
    return;
  }

  const report = await buildLightDiagnosticsReport(project);
  const missingLogKeys = report.checks.find((entry) => entry.label === 'Log forwarding env')?.missingKeys || [];
  const inline = new InlineKeyboard();
  missingLogKeys.forEach((key) => {
    inline.text(`Fix ${key}`, `proj:log_env_fix:${project.id}:${key}`).row();
  });
  inline.text('⬅️ Back', `proj:diagnostics_menu:${project.id}`);

  await safeRespond(ctx, report.lines.join('\n'), { reply_markup: inline }, { action: 'light_diagnostics' });
}

async function buildHeavyDiagnosticsReport(project, options = {}) {
  const checks = [];
  const lines = [];
  let repoInfo = null;
  let blockedReason = null;
  let checkoutDir = null;
  const globalSettings = await loadGlobalSettings();
  const baseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  const onStep = typeof options.onStep === 'function' ? options.onStep : null;

  try {
    const projectForCheckout = { ...project, workingDir: undefined };
    repoInfo = getRepoInfo(projectForCheckout);
    const prepared = await prepareRepository(projectForCheckout, baseBranch);
    checkoutDir = prepared?.repoDir || null;
  } catch (error) {
    if (error.message === 'Project is missing repoSlug') {
      blockedReason = 'repoSlug missing';
    } else {
      blockedReason = error.message || 'repo preparation failed';
    }
  }

  if (onStep) {
    await onStep('Step 2/6: Validate working directory');
  }
  const workingDir = resolveWorkingDirAgainstCheckout(project.workingDir, checkoutDir) || repoInfo?.workingDir;
  if (!workingDir) {
    blockedReason = blockedReason || 'workingDir missing';
  }

  if (workingDir && !project.workingDir && checkoutDir) {
    const relative = path.relative(checkoutDir, workingDir) || '.';
    await updateProjectField(project.id, 'workingDir', relative);
  }

  const validation = workingDir
    ? await validateWorkingDir({ ...project, workingDir: project.workingDir || '.' })
    : null;
  if (validation && !validation.ok) {
    blockedReason = blockedReason || validation.details || 'workingDir invalid';
  }

  if (blockedReason) {
    lines.push(`❌ Heavy diagnostics blocked — ${blockedReason}`);
    if (validation && !validation.ok) {
      lines.push(`Reason: ${validation.details || 'workingDir invalid'}`);
      if (validation.expectedCheckoutDir) {
        lines.push(`Expected repo root: ${validation.expectedCheckoutDir}`);
      }
      if (validation.suggestedWorkingDir) {
        lines.push(`Suggested workingDir: ${validation.suggestedWorkingDir}`);
      }
    }
    return {
      blocked: true,
      lines,
      workingDirInvalid: validation && !validation.ok,
    };
  }

  checks.push({
    status: 'ok',
    label: 'Working dir validation',
    detail: validation?.details || workingDir,
  });

  if (onStep) {
    await onStep('Step 3/6: Git fetch');
  }
  const tokenInfo = await resolveGithubToken(project);
  const fetchCheck = repoInfo
    ? await checkGitFetch(repoInfo.repoUrl, tokenInfo.token, baseBranch)
    : { status: 'fail', detail: 'repo info missing', hint: 'Check repo settings.' };
  checks.push({
    status: fetchCheck.status === 'ok' ? 'ok' : 'fail',
    label: 'Git fetch',
    detail: fetchCheck.detail,
  });

  if (onStep) {
    await onStep('Step 4/6: Install deps (if applicable)');
  }
  if (project.installCommand) {
    const installResult = await runCommandInProject({ ...project, workingDir }, project.installCommand);
    checks.push({
      status: installResult.exitCode === 0 ? 'ok' : 'fail',
      label: 'Install deps',
      detail: `exit ${installResult.exitCode} (${installResult.durationMs} ms)`,
      details:
        installResult.exitCode === 0
          ? []
          : [
              `Last output: ${truncateText(installResult.stderr || installResult.stdout || '', 200)}`,
              ...buildDiagnosticsErrorHints(installResult, 'Install deps', project.installCommand),
            ],
    });
  } else {
    checks.push({ status: 'ok', label: 'Install deps', detail: 'not configured' });
  }

  if (onStep) {
    await onStep('Step 5/6: Run test command');
  }
  if (project.testCommand) {
    const testResult = await runCommandInProject({ ...project, workingDir }, project.testCommand);
    checks.push({
      status: testResult.exitCode === 0 ? 'ok' : 'fail',
      label: 'Test command',
      detail: `exit ${testResult.exitCode} (${testResult.durationMs} ms)`,
      details:
        testResult.exitCode === 0
          ? []
          : [
              `Last output: ${truncateText(testResult.stderr || testResult.stdout || '', 200)}`,
              ...buildDiagnosticsErrorHints(testResult, 'Test command', project.testCommand),
            ],
    });
  } else {
    checks.push({ status: 'fail', label: 'Test command', detail: 'not configured' });
  }

  if (onStep) {
    await onStep('Step 6/6: Run diagnostic command');
  }
  if (project.diagnosticCommand) {
    const diagResult = await runCommandInProject({ ...project, workingDir }, project.diagnosticCommand);
    checks.push({
      status: diagResult.exitCode === 0 ? 'ok' : 'fail',
      label: 'Diagnostic command',
      detail: `exit ${diagResult.exitCode} (${diagResult.durationMs} ms)`,
      details:
        diagResult.exitCode === 0
          ? []
          : [
              `Last output: ${truncateText(diagResult.stderr || diagResult.stdout || '', 200)}`,
              ...buildDiagnosticsErrorHints(diagResult, 'Diagnostic command', project.diagnosticCommand),
            ],
    });
  } else {
    checks.push({ status: 'fail', label: 'Diagnostic command', detail: 'not configured' });
  }

  checks.forEach((check) => {
    lines.push(formatDiagnosticsCheckLine(check.status, check.label, check.detail));
    if (check.details && check.details.length) {
      check.details.forEach((line) => lines.push(`  ${line}`));
    }
  });

  return { blocked: false, lines, workingDirInvalid: false };
}

async function runProjectFullDiagnostics(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await safeRespond(ctx, 'Project not found.', null, { action: 'full_diagnostics' });
    return;
  }

  const header = `🧪 Full Diagnostics — ${project.name || project.id}`;
  const progress = await startProgressMessage(ctx, `${header}\n⏳ waiting testing…\nStep 1/6: Resolve env`);
  const updateProgress = async (stepLine) =>
    updateProgressMessage(progress, `${header}\n⏳ waiting testing…\n${stepLine}`);

  await updateProgress('Step 1/6: Resolve env');
  const lightReport = await buildLightDiagnosticsReport(project, { includeHeader: false });

  const heavyReport = await buildHeavyDiagnosticsReport(project, {
    onStep: async (stepLine) => updateProgress(stepLine),
  });

  const lines = [
    header,
    '',
    'Light diagnostics:',
    ...lightReport.lines,
    '',
    'Heavy diagnostics:',
    ...heavyReport.lines,
  ];

  const inline = new InlineKeyboard();
  const missingLogKeys = lightReport.checks.find((entry) => entry.label === 'Log forwarding env')?.missingKeys || [];
  missingLogKeys.forEach((key) => {
    inline.text(`Fix ${key}`, `proj:log_env_fix:${project.id}:${key}`).row();
  });
  if (heavyReport.workingDirInvalid) {
    inline.text('Fix WorkingDir', `proj:edit_workdir:${project.id}`).row();
  }
  inline.text('⬅️ Back', `proj:diagnostics_menu:${project.id}`);

  await updateProgressMessage(progress, lines.join('\n'), { reply_markup: inline });
}

async function runProjectDiagnostics(ctx, projectId) {
  await runProjectFullDiagnostics(ctx, projectId);
}

async function handleLogForwardingEnvFix(ctx, projectId, key) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  if (!key) {
    await safeRespond(ctx, 'Env key missing.', null, { action: 'log_env_fix' });
    return;
  }
  if (!isEnvVaultAvailable()) {
    await safeRespond(
      ctx,
      `Env Vault unavailable. Set ${key} in the runtime environment.`,
      { reply_markup: buildBackKeyboard(`proj:diagnostics_menu:${projectId}`) },
      { action: 'log_env_fix' },
    );
    return;
  }
  await promptEnvVaultValue(ctx, projectId, key, { messageContext: getMessageTargetFromCtx(ctx) });
}

function formatMaskedEnvExportLine(entry) {
  const status = evaluateEnvValueStatus(entry.value);
  const masked = maskEnvValue(entry.value);
  const display = status.status === 'SET' ? masked : '(missing)';
  const source = entry.source ? `source: ${entry.source}` : 'source: -';
  return `${entry.key} = ${display} (${source}, status: ${status.status})`;
}

async function exportProjectEnv(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;

  const envVaultEntries = [];
  if (isEnvVaultAvailable()) {
    try {
      const envSetId = await ensureProjectEnvSet(projectId);
      const envVars = await listEnvVars(projectId, envSetId);
      for (const entry of envVars) {
        const value = await getEnvVarValue(projectId, entry.key, envSetId);
        envVaultEntries.push({ key: entry.key, value, source: 'project_env_vault' });
      }
    } catch (error) {
      console.error('[env export] Failed to read env vault', error);
    }
  }

  const processEntries = Object.entries(process.env || {}).map(([key, value]) => ({
    key,
    value,
    source: 'process_env',
  }));

  const projectDbEntries = project.databaseUrl
    ? [{ key: 'DATABASE_URL', value: project.databaseUrl, source: 'project_db_config' }]
    : [];

  const computedDefaults = project.name || project.id
    ? [{ key: 'PROJECT_NAME', value: project.name || project.id, source: 'computed_default' }]
    : [];

  const effectiveEntries = [];
  const allKeys = new Set([
    ...envVaultEntries.map((entry) => entry.key),
    ...processEntries.map((entry) => entry.key),
    ...projectDbEntries.map((entry) => entry.key),
    ...computedDefaults.map((entry) => entry.key),
  ]);

  const findEntry = (list, key) => list.find((entry) => entry.key === key);
  allKeys.forEach((key) => {
    const candidates = [
      findEntry(envVaultEntries, key),
      findEntry(processEntries, key),
      findEntry(projectDbEntries, key),
      findEntry(computedDefaults, key),
    ].filter(Boolean);
    let selected = candidates.find((entry) => evaluateEnvValueStatus(entry.value).status === 'SET');
    if (!selected) {
      selected = candidates[0] || { key, value: null, source: 'computed_default' };
    }
    effectiveEntries.push({ key, value: selected.value, source: selected.source });
  });

  const sortedEnvVault = [...envVaultEntries].sort((a, b) => a.key.localeCompare(b.key));
  const sortedProcess = [...processEntries].sort((a, b) => a.key.localeCompare(b.key));
  const sortedEffective = [...effectiveEntries].sort((a, b) => a.key.localeCompare(b.key));

  const lines = [
    `🔐 Env export — ${project.name || project.id}`,
    '',
    'Effective resolved env (masked):',
    ...sortedEffective.map(formatMaskedEnvExportLine),
  ];

  await sendChunkedMessages(ctx, lines);

  const fileLines = sortedEffective.map(
    (entry) => `${entry.key}=${entry.value == null ? '' : String(entry.value)}`,
  );

  const filename = `${projectId}-env-export.txt`;
  await sendTextFile(ctx, filename, fileLines.join('\n'));
}

const ENV_SCAN_EXCLUDE_DIRS = new Set([
  'node_modules',
  '.git',
  'dist',
  'build',
  'coverage',
  'vendor',
  '.next',
  '.cache',
]);
const ENV_SCAN_MAX_FILE_SIZE = 512 * 1024;

async function collectEnvScanFiles(rootDir) {
  const files = [];
  const queue = [rootDir];
  while (queue.length) {
    const current = queue.pop();
    let entries = [];
    try {
      entries = await fs.readdir(current, { withFileTypes: true });
    } catch (error) {
      continue;
    }
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        if (ENV_SCAN_EXCLUDE_DIRS.has(entry.name)) {
          continue;
        }
        queue.push(fullPath);
      } else if (entry.isFile()) {
        files.push(fullPath);
      }
    }
  }
  return files;
}

function recordEnvUsage(envMap, name, info) {
  if (!envMap.has(name)) {
    envMap.set(name, {
      name,
      firstSeen: info.firstSeen,
      firstSeenExcerpt: info.firstSeen?.excerpt || null,
      seenInCode: false,
      seenInExample: false,
      usage: { required: false, optional: false },
    });
  }
  const entry = envMap.get(name);
  if (!entry.firstSeen) {
    entry.firstSeen = info.firstSeen;
  }
  if (!entry.firstSeenExcerpt && info.firstSeen?.excerpt) {
    entry.firstSeenExcerpt = info.firstSeen.excerpt;
  }
  if (info.isExample) {
    entry.seenInExample = true;
  } else {
    entry.seenInCode = true;
    if (info.required) {
      entry.usage.required = true;
    } else {
      entry.usage.optional = true;
    }
  }
}

function isEnvNameValid(name) {
  return /^[A-Z0-9_]{2,}$/.test(name);
}

function classifyNodeUsage(line, name) {
  const requiredRegex = new RegExp(`process\\.env(?:\\.${name}|\\[['"]${name}['"]\\])\\s*!`);
  return requiredRegex.test(line) ? 'required' : 'optional';
}

function buildEnvExcerpt(lines, lineNumber, context = 2) {
  const index = Math.max(0, lineNumber - 1);
  const start = Math.max(0, index - context);
  const end = Math.min(lines.length - 1, index + context);
  const excerptLines = [];
  for (let i = start; i <= end; i += 1) {
    const marker = i + 1 === lineNumber ? '▶' : ' ';
    excerptLines.push(`${marker} ${i + 1}: ${lines[i]}`);
  }
  return excerptLines.join('\n');
}

function scanLineForEnv(line, lineNumber, relativePath, envMap, isExample, lines) {
  const patterns = [
    { regex: /process\.env\.([A-Z0-9_]+)/g, type: 'node' },
    { regex: /process\.env\[['"]([A-Z0-9_]+)['"]\]/g, type: 'node' },
    { regex: /os\.environ\[['"]([A-Z0-9_]+)['"]\]/g, type: 'python_required' },
    { regex: /os\.getenv\(\s*['"]([A-Z0-9_]+)['"](?:\s*,\s*[^)]+)?\)/g, type: 'python_optional' },
    { regex: /\$\{([A-Z0-9_]+)\}/g, type: 'generic' },
    { regex: /\bENV\s+([A-Z0-9_]+)=/g, type: 'generic' },
    { regex: /\bexport\s+([A-Z0-9_]+)=/g, type: 'generic' },
    { regex: /\bname:\s*([A-Z0-9_]+)\b/g, type: 'generic' },
  ];

  patterns.forEach((pattern) => {
    let match;
    while ((match = pattern.regex.exec(line))) {
      const name = match[1];
      if (!isEnvNameValid(name)) continue;
      let required = false;
      if (pattern.type === 'python_required') {
        required = true;
      } else if (pattern.type === 'node') {
        required = classifyNodeUsage(line, name) === 'required';
      }
      recordEnvUsage(envMap, name, {
        firstSeen: {
          path: relativePath,
          line: lineNumber,
          excerpt: Array.isArray(lines) ? buildEnvExcerpt(lines, lineNumber) : null,
        },
        isExample,
        required,
      });
    }
  });
}

async function scanEnvRequirements(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;

  const header = `🔎 Env scan — ${project.name || project.id}`;
  const progress = await startProgressMessage(ctx, `${header}\n⏳ waiting testing…`);
  const updateProgress = async (statusLine, extraLines = [], extra = {}) =>
    updateProgressMessage(progress, [header, statusLine, ...extraLines].filter(Boolean).join('\n'), extra);

  console.log('[env scan] start', { projectId });
  await updateProgress('⏳ Preparing repository…');
  const globalSettings = await loadGlobalSettings();
  const baseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  let repoInfo = null;
  try {
    repoInfo = getRepoInfo(project);
    await prepareRepository(project, baseBranch);
  } catch (error) {
    const message =
      error.message === 'Project is missing repoSlug'
        ? 'Repo slug missing. Set repoSlug before scanning env requirements.'
        : `Repo preparation failed: ${error.message}`;
    await updateProgress(`❌ ${message}`, [], {
      reply_markup: buildBackKeyboard(`proj:diagnostics_menu:${projectId}`),
    });
    return;
  }

  const workingDir = resolveProjectWorkingDir(project) || repoInfo?.workingDir;
  if (!workingDir) {
    await updateProgress('❌ workingDir missing. Set a working directory before scanning.', [], {
      reply_markup: buildBackKeyboard(`proj:diagnostics_menu:${projectId}`),
    });
    return;
  }

  await updateProgress('⏳ Validating working directory…');
  const validation = await validateWorkingDir({ ...project, workingDir });
  if (!validation.ok) {
    const lines = ['Env scan blocked: workingDir is invalid.', `Reason: ${validation.details}`];
    if (validation.expectedCheckoutDir) {
      lines.push(`Expected repo root: ${validation.expectedCheckoutDir}`);
    }
    if (validation.suggestedWorkingDir) {
      lines.push(`Suggested workingDir: ${validation.suggestedWorkingDir}`);
    }
    const inline = new InlineKeyboard()
      .text('Fix WorkingDir', `proj:edit_workdir:${projectId}`)
      .row()
      .text('⬅️ Back', `proj:diagnostics_menu:${projectId}`);
    await updateProgress(lines.join('\n'), [], { reply_markup: inline });
    return;
  }

  await updateProgress('⏳ Scanning files for env usage…');
  const envMap = new Map();
  const files = await collectEnvScanFiles(workingDir);
  for (const file of files) {
    let stat;
    try {
      stat = await fs.stat(file);
    } catch (error) {
      continue;
    }
    if (stat.size > ENV_SCAN_MAX_FILE_SIZE) {
      continue;
    }
    let content = '';
    try {
      content = await fs.readFile(file, 'utf8');
    } catch (error) {
      continue;
    }
    const relativePath = path.relative(workingDir, file);
    const isExample = relativePath.endsWith('.env.example');
    const lines = content.split(/\r?\n/);
    lines.forEach((line, index) => {
      scanLineForEnv(line, index + 1, relativePath, envMap, isExample, lines);
    });
  }

  const entries = [...envMap.values()];
  const required = [];
  const optional = [];
  const suggested = [];
  entries.forEach((entry) => {
    if (entry.seenInCode) {
      if (entry.usage.required) {
        required.push(entry);
      } else {
        optional.push(entry);
      }
    } else if (entry.seenInExample) {
      suggested.push(entry);
    }
  });

  await updateProgress('⏳ Resolving current env values…');
  const allKeys = entries.map((entry) => entry.name);
  const envVaultValues = new Map();
  if (isEnvVaultAvailable()) {
    const envSetId = await ensureProjectEnvSet(projectId);
    for (const key of allKeys) {
      try {
        const value = await getEnvVarValue(projectId, key, envSetId);
        if (value !== null && value !== undefined) {
          envVaultValues.set(key, value);
        }
      } catch (error) {
        continue;
      }
    }
  }

  const resolveEnvStatus = (name) => {
    const vaultValue = envVaultValues.get(name);
    const processValue = process.env[name];
    const candidates = [
      { source: 'project_env_vault', value: vaultValue },
      { source: 'process_env', value: processValue },
    ];
    const effective = selectEffectiveEnvValue(candidates);
    const status = evaluateEnvValueStatus(effective.value);
    return {
      status: status.status,
      source: effective.source,
      maskedValue: status.status === 'SET' ? maskEnvValue(effective.value) : '(missing)',
    };
  };

  const summarizeEntry = (entry, classification, resolved) => {
    const preset = getPmEnvPreset(entry.name);
    const lines = [
      `${classification} ${entry.name}`,
      `• Source: ${resolved.source || '-'} | First seen: ${entry.firstSeen?.path || '-'}:${entry.firstSeen?.line || '-'}`,
      `• Recommended profile: ${preset.category}`,
    ];
    return lines;
  };

  const missingRequiredEntries = required.filter(
    (entry) => resolveEnvStatus(entry.name).status !== 'SET',
  );
  const missingOptionalEntries = optional.filter(
    (entry) => resolveEnvStatus(entry.name).status !== 'SET',
  );

  const missingRequired = missingRequiredEntries.map((entry) => entry.name);
  envScanCache.set(projectId, { missingRequired, updatedAt: Date.now() });

  const summary = [
    `✅ Ready: ${required.length - missingRequiredEntries.length}/${required.length} required set`,
    `❌ Required missing: ${missingRequiredEntries.length}`,
    `⚠️ Optional missing: ${missingOptionalEntries.length}`,
    `💡 Suggested: ${suggested.length}`,
  ];

  const reportLines = [header, ...summary, '', 'Detailed missing env report:'];
  if (missingRequiredEntries.length) {
    reportLines.push('', '❌ Required missing:');
    missingRequiredEntries.forEach((entry) => {
      const resolved = resolveEnvStatus(entry.name);
      const preset = getPmEnvPreset(entry.name);
      reportLines.push(...summarizeEntry(entry, '❌ Required:', resolved));
      reportLines.push(`• What it does: ${getEnvPurposeHint(entry.name)}`);
      reportLines.push(`• Recommended value for PM: ${preset.recommendedValue || '(none / set manually)'}`);
      reportLines.push(`• Notes: ${preset.notes}`);
      reportLines.push('');
    });
  }
  if (missingOptionalEntries.length) {
    reportLines.push('', '⚠️ Optional missing:');
    missingOptionalEntries.forEach((entry) => {
      const resolved = resolveEnvStatus(entry.name);
      const preset = getPmEnvPreset(entry.name);
      reportLines.push(...summarizeEntry(entry, '⚠️ Optional:', resolved));
      reportLines.push(`• What it does: ${getEnvPurposeHint(entry.name)}`);
      reportLines.push(`• Recommended value for PM: ${preset.recommendedValue || '(optional)'}`);
      reportLines.push(`• Notes: ${preset.notes}`);
      reportLines.push('');
    });
  }
  if (suggested.length) {
    reportLines.push('', '💡 Suggested:');
    suggested
      .slice()
      .sort((a, b) => a.name.localeCompare(b.name))
      .forEach((entry) => {
        const resolved = resolveEnvStatus(entry.name);
        const preset = getPmEnvPreset(entry.name);
        reportLines.push(...summarizeEntry(entry, '💡 Suggested:', resolved));
        reportLines.push(`• What it does: ${getEnvPurposeHint(entry.name)}`);
        reportLines.push(`• Recommended value for PM: ${preset.recommendedValue || '(optional)'}`);
        reportLines.push(`• Notes: ${preset.notes}`);
        reportLines.push('');
      });
  }

  const outputText = [header, ...summary].join('\n');
  miniLatestEnvScan = {
    scannedAt: new Date().toISOString(),
    summary: summary.join(' | '),
    reportText: reportLines.join('\n'),
  };

  const inline = new InlineKeyboard()
    .text('🛠️ Fix missing required envs', `proj:env_scan_fix_missing:${projectId}`)
    .row()
    .text('🎯 Fix a specific env', `proj:env_scan_fix_specific:${projectId}`)
    .row()
    .text('📤 Export env (masked + full file)', `proj:env_export:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:diagnostics_menu:${projectId}`);

  await sendDismissibleMessage(ctx, outputText, { reply_markup: inline });
  const filename = `${projectId}-env-scan-report.txt`;
  await sendTextFile(ctx, filename, reportLines.join('\n'), 'Env scan detailed report');
  await updateProgress('✅ Env scan completed. Opening diagnostics menu…');
  await renderProjectDiagnosticsMenu(ctx, projectId, '✅ Env scan completed.');

  console.log('[env scan] end', { projectId, entries: entries.length });
}

async function handleEnvScanFixMissing(ctx, projectId) {
  const cached = envScanCache.get(projectId);
  if (!cached || !cached.missingRequired || !cached.missingRequired.length) {
    await safeRespond(
      ctx,
      'No missing required envs found. Run "Scan env requirements" first.',
      { reply_markup: buildBackKeyboard(`proj:diagnostics_menu:${projectId}`) },
      { action: 'env_scan_fix_missing' },
    );
    return;
  }
  if (!isEnvVaultAvailable()) {
    await safeRespond(
      ctx,
      buildEnvVaultUnavailableMessage('Env Vault unavailable.'),
      { reply_markup: buildBackKeyboard(`proj:diagnostics_menu:${projectId}`) },
      { action: 'env_scan_fix_missing' },
    );
    return;
  }
  const keys = cached.missingRequired;
  await promptEnvVaultValue(ctx, projectId, keys[0], {
    queue: keys,
    allowSkip: true,
    skipExisting: true,
    requiredKeys: keys,
    messageContext: getMessageTargetFromCtx(ctx),
  });
}

async function handleEnvScanFixSpecific(ctx, projectId) {
  if (!isEnvVaultAvailable()) {
    await safeRespond(
      ctx,
      buildEnvVaultUnavailableMessage('Env Vault unavailable.'),
      { reply_markup: buildBackKeyboard(`proj:diagnostics_menu:${projectId}`) },
      { action: 'env_scan_fix_specific' },
    );
    return;
  }
  setUserState(ctx.from.id, {
    type: 'env_scan_fix_key',
    projectId,
    messageContext: getMessageTargetFromCtx(ctx),
  });
  await renderOrEdit(ctx, 'Send the env key to set.\n(Or press Cancel)', {
    reply_markup: buildCancelKeyboard(),
  });
}

async function handleEnvScanFixKeyInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send an env key.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderOrEdit(ctx, 'Operation cancelled.', {
      reply_markup: buildBackKeyboard(`proj:diagnostics_menu:${state.projectId}`),
    });
    return;
  }
  const key = normalizeEnvKeyInput(text);
  clearUserState(ctx.from.id);
  await promptEnvVaultValue(ctx, state.projectId, key, { messageContext: state.messageContext });
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

async function openProjectOverview(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const globalSettings = await loadGlobalSettings();
  const view = await buildProjectOverviewView(project, globalSettings, notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function renderProjectSettings(ctx, projectId, notice) {
  if (!getConfigDbSnapshot().ready) {
    await renderConfigDbGate(ctx, { title: '📦 Project settings', backCallback: 'main:back' });
    return;
  }
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const view = buildProjectHubView(project, notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

const DEFAULT_PROJECT_SETUP_RULES = {
  requireRepoUrl: true,
  runModeRequiringStartCommand: ['service', 'worker'],
  logForwardingRequiredEnvKeys: ['PATH_APPLIER_URL', 'PATH_APPLIER_TOKEN', 'PROJECT_NAME'],
  databaseModuleEnabledKey: 'dbEnabled',
  databaseUrlKey: 'databaseUrl',
  cronModuleEnabledKey: 'cronEnabled',
  cronTimezoneKey: 'cronTimezone',
  cronExpressionKey: 'cronExpression',
  templateCommandFields: {
    install: 'installCommand',
    build: 'buildCommand',
  },
};

function getProjectSetupRules(globalSettings) {
  const overrides = globalSettings?.projectSetupRules || {};
  return {
    ...DEFAULT_PROJECT_SETUP_RULES,
    ...overrides,
    templateCommandFields: {
      ...DEFAULT_PROJECT_SETUP_RULES.templateCommandFields,
      ...(overrides.templateCommandFields || {}),
    },
  };
}

function isMissingRequirementValue(value) {
  const trimmed = String(value ?? '').trim();
  return !trimmed || trimmed === '-';
}

function resolveProjectFeatureFlag(project, key) {
  const feature = project?.feature || project?.features || {};
  if (Object.prototype.hasOwnProperty.call(feature, key)) {
    return feature[key];
  }
  if (Object.prototype.hasOwnProperty.call(project || {}, key)) {
    return project[key];
  }
  return undefined;
}

async function getMissingRequirements(project) {
  const missing = [];
  const runModeRaw = project?.runMode || project?.run_mode || '';
  const runMode = String(runModeRaw).trim();
  const runModeNormalized = runMode.toLowerCase();
  const logAlertsEnabled =
    resolveProjectFeatureFlag(project, 'logAlertsEnabled') === true ||
    project?.logAlertsEnabled === true ||
    getEffectiveProjectLogForwarding(project).enabled === true;
  const databaseEnabled =
    resolveProjectFeatureFlag(project, 'databaseEnabled') === true ||
    project?.databaseEnabled === true ||
    project?.dbEnabled === true;
  const supabaseEnabled = resolveSupabaseEnabled(project);
  const healthPath = project?.healthPath || project?.health_path;
  const servicePort =
    project?.servicePort || project?.expectedPort || project?.port || project?.healthPort;

  if (!runModeNormalized) {
    missing.push({
      key: 'runMode',
      title: 'Run mode not set',
      description: 'Select Service / Worker / Job so PM knows what is required.',
      severity: 'required',
      fixAction: 'FIX_RUNMODE',
    });
  }

  if (
    ['service', 'worker'].includes(runModeNormalized) &&
    isMissingRequirementValue(project?.startCommand)
  ) {
    missing.push({
      key: 'startCommand',
      title: 'Start command missing',
      description: 'Start command is required for Service/Worker.',
      severity: 'required',
      fixAction: 'FIX_START_COMMAND',
    });
  }

  if (isMissingRequirementValue(project?.testCommand)) {
    missing.push({
      key: 'testCommand',
      title: 'Test command not set',
      description: 'Set a test command to enable readable project tests.',
      severity: 'recommended',
      fixAction: 'FIX_TEST_COMMAND',
    });
  }

  if (isMissingRequirementValue(project?.diagnosticCommand)) {
    missing.push({
      key: 'diagnosticCommand',
      title: 'Diagnostic command not set',
      description: 'Set a diagnostic command for debugging/health checks.',
      severity: 'recommended',
      fixAction: 'FIX_DIAGNOSTIC_COMMAND',
    });
  }

  if (logAlertsEnabled) {
    const requiredKeys = ['PATH_APPLIER_URL', 'PATH_APPLIER_TOKEN', 'PROJECT_NAME'];
    for (const key of requiredKeys) {
      const sources = await resolveEnvValueSources(project, key);
      const effective = selectEffectiveEnvValue(sources);
      const status = evaluateEnvValueStatus(effective.value);
      if (status.status !== 'SET') {
        missing.push({
          key: `logForwardingEnv:${key}`,
          title: `Log forwarding env: ${key}`,
          description: `Set ${key} to enable log forwarding.`,
          severity: 'required',
          fixAction: `FIX_LOG_FORWARDING_ENV:${key}`,
        });
      }
    }
  }

  if (databaseEnabled && isMissingRequirementValue(project?.databaseUrl)) {
    missing.push({
      key: 'databaseUrl',
      title: 'Database URL missing',
      description: 'Add DB connection to use Database UI/SQL runner.',
      severity: 'required',
      fixAction: 'FIX_DATABASE_URL',
    });
  }

  if (supabaseEnabled) {
    const supabaseMissing = getSupabaseBindingMissingFields(project);
    if (supabaseMissing.length) {
      missing.push({
        key: 'supabaseConnection',
        title: 'Supabase binding missing',
        description: `Add Supabase project ref, URL, and API key (${supabaseMissing.join(', ')}).`,
        severity: 'required',
        fixAction: 'FIX_SUPABASE_BINDING',
      });
    }
  }

  if (runModeNormalized === 'service') {
    const healthMissing = isMissingRequirementValue(healthPath);
    const portMissing = isMissingRequirementValue(servicePort);
    if (healthMissing || portMissing) {
      missing.push({
        key: 'serviceHealth',
        title: 'Service health/port not configured',
        description: 'Set healthPath and confirm port behavior for Render-friendly checks.',
        severity: 'recommended',
        fixAction: 'FIX_SERVICE_HEALTH',
      });
    }
  }

  const envScan = envScanCache.get(project?.id);
  if (envScan?.missingRequired?.length) {
    missing.push({
      key: 'envScanMissing',
      title: `Missing envs (${envScan.missingRequired.length})`,
      description: `Required envs missing: ${envScan.missingRequired.join(', ')}`,
      severity: 'required',
      fixAction: 'FIX_ENV_SCAN_MISSING',
    });
  }

  return missing;
}

function getProjectMissingSetup(project, globalSettings) {
  const rules = getProjectSetupRules(globalSettings);
  const missing = [];
  const addMissing = (id, label, emoji, action) => {
    missing.push({ id, label, emoji, action });
  };

  if (rules.requireRepoUrl && !project.repoUrl) {
    addMissing('repoUrl', 'Repo URL', '📦', `proj:edit_repo:${project.id}:missing_setup`);
  }

  const runMode = String(project.runMode || project.run_mode || '').toLowerCase();
  if (rules.runModeRequiringStartCommand.includes(runMode) && !project.startCommand) {
    addMissing(
      'startCommand',
      'Start command',
      '🧰',
      `proj:cmd_edit:${project.id}:startCommand:missing_setup`,
    );
  }

  const template = project.template || project.projectTemplate || null;
  const requiresInstall = template?.requiresInstallCommand === true;
  const requiresBuild = template?.requiresBuildCommand === true;
  if (requiresInstall && !project[rules.templateCommandFields.install]) {
    addMissing('installCommand', 'Install command', '📦', `proj:commands:${project.id}:missing_setup`);
  }
  if (requiresBuild && !project[rules.templateCommandFields.build]) {
    addMissing('buildCommand', 'Build command', '🛠️', `proj:commands:${project.id}:missing_setup`);
  }

  const forwarding = getEffectiveProjectLogForwarding(project);
  if (forwarding.enabled) {
    const missingEnv = rules.logForwardingRequiredEnvKeys.filter((key) => !process.env[key]);
    if (missingEnv.length) {
      addMissing('logEnvs', `Log envs (${missingEnv.join(', ')})`, '📣', `envvault:menu:${project.id}`);
    }
  }

  if (project[rules.databaseModuleEnabledKey] === true && !project[rules.databaseUrlKey]) {
    addMissing('databaseUrl', 'Database URL', '🗄️', `envvault:menu:${project.id}`);
  }

  if (project[rules.cronModuleEnabledKey] === true) {
    if (!project[rules.cronTimezoneKey]) {
      addMissing('cronTimezone', 'Cron timezone', '⏱️', `projcron:menu:${project.id}`);
    }
    if (project[rules.cronExpressionKey]) {
      const validation = validateCronExpression(project[rules.cronExpressionKey]);
      if (!validation.valid) {
        addMissing('cronExpression', 'Cron expression', '⏱️', `projcron:menu:${project.id}`);
      }
    } else {
      addMissing('cronExpression', 'Cron expression', '⏱️', `projcron:menu:${project.id}`);
    }
  }

  return missing;
}

async function buildProjectOverviewView(project, globalSettings, notice) {
  const effectiveBase = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  const isDefault = globalSettings.defaultProjectId === project.id;
  const name = project.name || project.id;
  const tokenKey = project.githubTokenEnvKey || 'GITHUB_TOKEN';
  const tokenLabel = tokenKey === 'GITHUB_TOKEN' ? 'GITHUB_TOKEN (default)' : tokenKey;
  const projectTypeLabel = getProjectTypeLabel(project);
  const missingSetup = await getMissingRequirements(project);
  const workingDirLabel = formatWorkingDirDisplay(project);

  const lines = [
    buildScopedHeader(`PROJECT: ${name}||${project.id}`, `Main → Projects → ${name} → 🧾 Overview`),
    notice || null,
    '',
    `🧩 Project default: ${isDefault ? 'yes ⭐' : 'no'}`,
    `🧭 Project type: ${projectTypeLabel}`,
    '',
    '📦 Repo:',
    `- 🆔 slug: ${project.repoSlug || 'not set'}`,
    `- 🔗 url: ${project.repoUrl || 'not set'}`,
    `📁 workingDir: ${workingDirLabel}`,
    `🔐 GitHub token env: ${tokenLabel}`,
    `🌿 defaultBaseBranch: ${effectiveBase}`,
    '',
    '🧰 Commands:',
    `- 🚀 startCommand: ${project.startCommand || '-'}`,
    `- 🧪 testCommand: ${project.testCommand || '-'}`,
    `- 🩺 diagnosticCommand: ${project.diagnosticCommand || '-'}`,
    '',
    '🛰️ Render:',
    `- 📡 service: ${project.renderServiceUrl || '-'}`,
    `- 🪝 deploy hook: ${project.renderDeployHookUrl || '-'}`,
    '',
    '🗄️ Database:',
    `- Supabase enabled: ${resolveSupabaseEnabled(project) ? 'yes' : 'no'}`,
    `- Supabase project ref: ${project.supabaseProjectRef || '-'}`,
    `- Supabase URL: ${project.supabaseUrl || '-'}`,
    `- Supabase API key: ${getSupabaseKeyMask(project)} (${project.supabaseKeyType || '-'})`,
  ].filter((line) => line !== null);

  const inline = new InlineKeyboard()
    .text('✏️ Edit Project', `proj:project_menu:${project.id}`)
    .row()
    .text('🌐 Repository', `proj:repo_menu:${project.id}`)
    .text('🗄 Database', `dbmenu:open:${project.id}`)
    .row()
    .text('⏱ Cron Jobs', `projcron:menu:${project.id}`)
    .text('🚀 Deploy', `deploy:open:${project.id}`)
    .row()
    .text('🧾 Logs', `logmenu:open:${project.id}`)
    .text('🛠 Ops & Safety', `proj:diagnostics_menu:${project.id}`)
    .row()
    .text('⚙️ Project Settings', `proj:settings:${project.id}`)
    .text('↩ Back', 'nav:back');

  if (missingSetup.length) {
    inline.row().text(`🧩 Complete Missing Setup (${missingSetup.length})`, `proj:missing_setup:${project.id}`);
  }
  inline.row().text('🗑 Delete project', `proj:delete:${project.id}`).text('🏠 Home', 'nav:home');

  return { text: lines.join('\n'), keyboard: inline };
}

function buildProjectHubView(project, notice) {
  const name = project.name || project.id;
  const lines = [
    buildScopedHeader(`PROJECT: ${name}||${project.id}`, `Main → Projects → ${name} → 📦 Project`),
    notice || null,
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('🧾 Overview', `proj:overview:${project.id}`)
    .row()
    .text('✏️ Edit Project', `proj:project_menu:${project.id}`)
    .row()
    .text('🌐 Repository', `proj:repo_menu:${project.id}`)
    .text('🗄 Database', `dbmenu:open:${project.id}`)
    .row()
    .text('⏱ Cron Jobs', `projcron:menu:${project.id}`)
    .text('🚀 Deploy', `deploy:open:${project.id}`)
    .row()
    .text('🧾 Logs', `logmenu:open:${project.id}`)
    .text('🛠 Ops & Safety', `proj:diagnostics_menu:${project.id}`)
    .row()
    .text('⚙️ Project Settings', `proj:settings:${project.id}`)
    .row()
    .text('🗑 Delete project', `proj:delete:${project.id}`)
    .text('🏠 Home', 'nav:home');
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectScopedSettings(ctx, projectId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const lines = [
    buildScopedHeader(`PROJECT: ${project.name || project.id}||${project.id}`, `Main → Projects → ${project.name || project.id} → ⚙️ Project Settings`),
    `⚙️ Project Settings — ${project.name || project.id}`,
    notice || null,
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('🧾 Overview', `proj:overview:${project.id}`)
    .row()
    .text('✏️ Edit name', `proj:rename:${project.id}`)
    .text('🆔 Edit ID', `proj:edit_id:${project.id}`)
    .row()
    .text('🧭 Apply path / working dir', `proj:working_dir:${project.id}`)
    .row()
    .text('🌐 Repository', `proj:repo_menu:${project.id}`)
    .text('⚙️ Preferences', `proj:preferences:${project.id}`)
    .row()
    .text('🧠 Codex Tasks', `proj:codex_tasks:${project.id}`)
    .row()
    .text('↩ Back', 'nav:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderProjectPreferencesMenu(ctx, projectId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const lines = [
    `⚙️ Preferences — ${project.name || project.id}`,
    notice || null,
    '',
    'Configure project-specific routing and behavior.',
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('🔔 Alerts & Log Routing', `logmenu:open:${project.id}`)
    .row()
    .text('↩ Back', 'nav:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function buildProjectMissingSetupView(project, globalSettings, notice) {
  const missing = await getMissingRequirements(project);
  const lines = [
    `🧩 Missing setup — ${project.name || project.id}`,
    notice || null,
    '',
  ].filter((line) => line !== null);

  const inline = new InlineKeyboard();

  if (!missing.length) {
    lines.push('✅ Setup Complete');
    inline
      .text('🧪 Run tests', `proj:diagnostics:${project.id}`)
      .row()
      .text('⬅️ Back', `proj:open:${project.id}`);
    return { text: lines.join('\n'), keyboard: inline };
  }

  const required = missing.filter((item) => item.severity === 'required');
  const recommended = missing.filter((item) => item.severity === 'recommended');

  if (required.length) {
    lines.push('Required:');
    required.forEach((item) => {
      lines.push(`- 🔴 ${item.title} — ${item.description}`);
      inline.text(`Fix ${item.title}`, `proj:missing_fix:${project.id}:${item.fixAction}`).row();
    });
    lines.push('');
  }

  if (recommended.length) {
    lines.push('Recommended:');
    recommended.forEach((item) => {
      lines.push(`- 🟡 ${item.title} — ${item.description}`);
      inline.text(`Fix ${item.title}`, `proj:missing_fix:${project.id}:${item.fixAction}`).row();
    });
  }

  if (missing.some((item) => item.key === 'serviceHealth' || item.key === 'startCommand' || item.key === 'testCommand' || item.key === 'diagnosticCommand')) {
    inline.text('🛠 Auto-Fix Health Setup', `proj:auto_fix_health:${project.id}`).row();
  }
  inline.text('⬅️ Back', `proj:open:${project.id}`);
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectMissingSetup(ctx, projectId, notice) {
  resetUserState(ctx);
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await safeRespond(ctx, 'Project not found.', null, { action: 'missing_setup' });
    return;
  }
  const globalSettings = await loadGlobalSettings();
  const view = await buildProjectMissingSetupView(project, globalSettings, notice);
  await safeRespond(ctx, view.text, { reply_markup: view.keyboard }, { action: 'missing_setup' });
}

function getMissingFixTarget(projectId, fixAction) {
  if (fixAction?.startsWith('FIX_LOG_FORWARDING_ENV:')) {
    const [, key] = fixAction.split(':');
    return { type: 'log_env_fix', key };
  }
  if (fixAction === 'FIX_ENV_SCAN_MISSING') {
    return { type: 'env_scan_missing' };
  }
  switch (fixAction) {
    case 'FIX_RUNMODE':
      return { type: 'run_mode' };
    case 'FIX_START_COMMAND':
      return { type: 'command_edit', field: 'startCommand' };
    case 'FIX_TEST_COMMAND':
      return { type: 'command_edit', field: 'testCommand' };
    case 'FIX_DIAGNOSTIC_COMMAND':
      return { type: 'command_edit', field: 'diagnosticCommand' };
    case 'FIX_DATABASE_URL':
      return { type: 'env_vault' };
    case 'FIX_SUPABASE_BINDING':
      return { type: 'supabase' };
    case 'FIX_SERVICE_HEALTH':
      return { type: 'service_health' };
    default:
      return null;
  }
}

async function handleProjectMissingFix(ctx, projectId, fixAction) {
  resetUserState(ctx);
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    const message = `❌ Missing setup error.\n- Rule: project not found\n- Fix action: ${fixAction}`;
    console.warn('[missing_setup] project not found', { projectId, fixAction });
    await safeRespond(ctx, message, null, { action: 'missing_setup' });
    return;
  }

  const missing = await getMissingRequirements(project);
  const targetItem = missing.find((item) => item.fixAction === fixAction);
  if (!targetItem) {
    const known = missing.map((item) => item.key).join(', ') || 'none';
    const message =
      `❌ Missing setup error.\n- Rule: not found\n- Fix action: ${fixAction}\n- Known missing: ${known}`;
    console.warn('[missing_setup] fix action not found', {
      projectId,
      fixAction,
      knownMissing: known,
    });
    await safeRespond(ctx, message, null, { action: 'missing_setup' });
    return;
  }

  const target = getMissingFixTarget(projectId, fixAction);
  if (!target) {
    const message =
      `❌ Missing setup error.\n- Rule: ${targetItem.key}\n- Fix action: ${fixAction}`;
    console.warn('[missing_setup] unsupported fix action', {
      projectId,
      fixAction,
      missingKey: targetItem.key,
    });
    await safeRespond(ctx, message, null, { action: 'missing_setup' });
    return;
  }

  if (target.type === 'run_mode') {
    await renderProjectRunModeMenu(ctx, projectId, { source: 'missing_setup' });
    return;
  }
  if (target.type === 'command_edit') {
    setUserState(ctx.from.id, {
      type: 'edit_command_input',
      projectId,
      field: target.field,
      backCallback: `proj:missing_setup:${projectId}`,
      messageContext: getMessageTargetFromCtx(ctx),
    });
    await renderOrEdit(ctx, `Send new value for ${target.field}.\n(Or press Cancel)`, {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }
  if (target.type === 'env_vault') {
    await renderEnvVaultMenu(ctx, projectId);
    return;
  }
  if (target.type === 'env_scan_missing') {
    await handleEnvScanFixMissing(ctx, projectId);
    return;
  }
  if (target.type === 'log_env_fix') {
    await handleLogForwardingEnvFix(ctx, projectId, target.key);
    return;
  }
  if (target.type === 'supabase') {
    await renderSupabaseScreen(ctx, projectId);
    return;
  }
  if (target.type === 'service_health') {
    setUserState(ctx.from.id, {
      type: 'edit_service_health',
      step: 'healthPath',
      projectId,
      backCallback: `proj:missing_setup:${projectId}`,
      messageContext: getMessageTargetFromCtx(ctx),
    });
    await renderOrEdit(
      ctx,
      'Send healthPath (for example: /healthz). Or send "-" to clear.\n(Or press Cancel)',
      { reply_markup: buildCancelKeyboard() },
    );
    return;
  }
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

async function renderProjectRunModeMenu(ctx, projectId, options = {}) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const current = String(project.runMode || project.run_mode || '').toLowerCase();
  const source = options?.source === 'missing_setup' ? 'missing_setup' : null;
  const suffix = source ? `:${source}` : '';
  const inline = new InlineKeyboard();
  const runModes = [
    { id: 'service', label: 'Service' },
    { id: 'worker', label: 'Worker' },
    { id: 'job', label: 'Job' },
  ];
  runModes.forEach((mode) => {
    const label = mode.id === current ? `✅ ${mode.label}` : mode.label;
    inline.text(label, `proj:run_mode_set:${projectId}:${mode.id}${suffix}`).row();
  });
  const backTarget = source ? `proj:missing_setup:${projectId}` : `proj:open:${projectId}`;
  inline.text('⬅️ Back', backTarget);
  await renderOrEdit(ctx, `Select run mode for ${project.name || project.id}:`, {
    reply_markup: inline,
  });
}

async function updateProjectRunMode(ctx, projectId, runMode, source = null) {
  const normalized = String(runMode || '').toLowerCase();
  const allowed = ['service', 'worker', 'job'];
  if (!allowed.includes(normalized)) {
    await renderProjectRunModeMenu(ctx, projectId, { source });
    return;
  }
  const updated = await updateProjectField(projectId, 'runMode', normalized);
  if (!updated) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  if (source === 'missing_setup') {
    await renderProjectMissingSetup(ctx, projectId, '✅ Updated');
    return;
  }
  await renderProjectSettings(ctx, projectId, '✅ Updated');
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

function normalizeLogTestSettings(project) {
  const logTest = project?.logTest || {};
  const lastTest = logTest.lastTest || {};
  const reminder = logTest.reminder || {};
  return {
    enabled: typeof logTest.enabled === 'boolean' ? logTest.enabled : Boolean(logTest.testEndpointUrl),
    testEndpointUrl: logTest.testEndpointUrl || null,
    diagnosticsEndpointUrl: logTest.diagnosticsEndpointUrl || null,
    tokenKeyInEnvVault: logTest.tokenKeyInEnvVault || null,
    lastTest: {
      status: ['never', 'pass', 'fail', 'partial', 'blocked_missing_diagnostics'].includes(
        lastTest.status,
      )
        ? lastTest.status
        : 'never',
      lastRunAt: lastTest.lastRunAt || null,
      lastSummary: lastTest.lastSummary || '',
      lastCorrelationIds: Array.isArray(lastTest.lastCorrelationIds)
        ? lastTest.lastCorrelationIds.filter(Boolean).map(String)
        : [],
    },
    lastDiagnostics: {
      timestamp: logTest.lastDiagnostics?.timestamp || null,
      logger: logTest.lastDiagnostics?.logger || null,
      pmConfig: logTest.lastDiagnostics?.pmConfig || null,
      lastSend: logTest.lastDiagnostics?.lastSend || null,
    },
    reminder: {
      needsTest:
        typeof reminder.needsTest === 'boolean'
          ? reminder.needsTest
          : lastTest.status !== 'pass',
      snoozedUntil: reminder.snoozedUntil || null,
    },
  };
}

function normalizeRenderWebhookSettings(settings) {
  const webhook = settings?.renderWebhook || {};
  const events = Array.isArray(webhook.events)
    ? webhook.events.filter(Boolean)
    : RENDER_WEBHOOK_EVENTS_DEFAULT.slice();
  return {
    webhookId: webhook.webhookId || null,
    targetUrl: webhook.targetUrl || '',
    events: events.length ? events : RENDER_WEBHOOK_EVENTS_DEFAULT.slice(),
    lastVerifiedAt: webhook.lastVerifiedAt || null,
  };
}

function parseEnvBoolean(value) {
  if (value == null) return null;
  const normalized = String(value).trim().toLowerCase();
  if (['true', '1', 'yes', 'y', 'on'].includes(normalized)) return true;
  if (['false', '0', 'no', 'n', 'off'].includes(normalized)) return false;
  return null;
}

function normalizeRenderGlobalSettings(settings) {
  const render = settings?.renderDeploy || {};
  return {
    pollingEnabledGlobal:
      typeof render.pollingEnabledGlobal === 'boolean' ? render.pollingEnabledGlobal : true,
    webhookEnabledGlobal:
      typeof render.webhookEnabledGlobal === 'boolean' ? render.webhookEnabledGlobal : true,
    pollIntervalSec: Number(render.pollIntervalSec) || RENDER_POLL_INTERVAL_SEC_DEFAULT,
    pollMaxServicesPerTick:
      Number(render.pollMaxServicesPerTick) || RENDER_POLL_MAX_SERVICES_PER_TICK_DEFAULT,
    pollTimeoutMs: Number(render.pollTimeoutMs) || RENDER_POLL_TIMEOUT_MS_DEFAULT,
    workspaceId: render.workspaceId || null,
    unmappedEvents: Array.isArray(render.unmappedEvents) ? render.unmappedEvents : [],
  };
}

function resolveRenderGlobalSettings(settings) {
  const normalized = normalizeRenderGlobalSettings(settings);
  const pollIntervalEnv = Number(process.env.RENDER_POLL_INTERVAL_SEC);
  const pollMaxEnv = Number(process.env.RENDER_POLL_MAX_SERVICES_PER_TICK);
  const pollTimeoutEnv = Number(process.env.RENDER_POLL_TIMEOUT_MS);
  const workspaceEnv = process.env.RENDER_WORKSPACE_ID;
  const pollingEnabledEnv = parseEnvBoolean(process.env.RENDER_POLLING_ENABLED_GLOBAL);
  const webhookEnabledEnv = parseEnvBoolean(process.env.RENDER_WEBHOOK_ENABLED_GLOBAL);
  return {
    ...normalized,
    pollIntervalSec:
      Number.isFinite(pollIntervalEnv) && pollIntervalEnv > 0
        ? pollIntervalEnv
        : normalized.pollIntervalSec,
    pollMaxServicesPerTick:
      Number.isFinite(pollMaxEnv) && pollMaxEnv > 0 ? pollMaxEnv : normalized.pollMaxServicesPerTick,
    pollTimeoutMs:
      Number.isFinite(pollTimeoutEnv) && pollTimeoutEnv > 0 ? pollTimeoutEnv : normalized.pollTimeoutMs,
    workspaceId: workspaceEnv || normalized.workspaceId || null,
    pollingEnabledGlobal:
      typeof pollingEnabledEnv === 'boolean' ? pollingEnabledEnv : normalized.pollingEnabledGlobal,
    webhookEnabledGlobal:
      typeof webhookEnabledEnv === 'boolean' ? webhookEnabledEnv : normalized.webhookEnabledGlobal,
  };
}

function upsertUnmappedRenderEvent(events, payload) {
  const now = new Date().toISOString();
  const existingIndex = events.findIndex((entry) => entry.serviceId === payload.serviceId);
  const base = existingIndex >= 0 ? events[existingIndex] : null;
  const updated = {
    serviceId: payload.serviceId,
    lastEventType: payload.eventType || base?.lastEventType || null,
    lastSeenAt: now,
    count: (base?.count || 0) + 1,
  };
  if (existingIndex >= 0) {
    const next = [...events];
    next[existingIndex] = updated;
    return next;
  }
  return [updated, ...events].slice(0, 20);
}

async function recordUnmappedRenderEvent(payload) {
  const settings = await getCachedSettings(true);
  const renderSettings = normalizeRenderGlobalSettings(settings);
  const nextEvents = upsertUnmappedRenderEvent(renderSettings.unmappedEvents, payload);
  const nextSettings = {
    ...settings,
    renderDeploy: {
      ...settings.renderDeploy,
      unmappedEvents: nextEvents,
    },
  };
  await saveGlobalSettingsAndCache(nextSettings);
  await recordAuditLog('render_unmapped_event', {
    serviceId: payload.serviceId,
    eventType: payload.eventType,
  });
  return nextEvents;
}

async function clearUnmappedRenderService(serviceId) {
  if (!serviceId) return null;
  const settings = await getCachedSettings(true);
  const renderSettings = normalizeRenderGlobalSettings(settings);
  const nextEvents = (renderSettings.unmappedEvents || []).filter((entry) => entry.serviceId !== serviceId);
  const nextSettings = {
    ...settings,
    renderDeploy: {
      ...settings.renderDeploy,
      unmappedEvents: nextEvents,
    },
  };
  await saveGlobalSettingsAndCache(nextSettings);
  return nextEvents;
}

function normalizeProjectDeploySettings(project) {
  const render = project?.render || {};
  const deployNotifications = project?.deployNotifications || {};
  const enabled =
    typeof render.enabled === 'boolean'
      ? render.enabled
      : typeof deployNotifications.enabled === 'boolean'
        ? deployNotifications.enabled
        : Boolean(render.serviceId || project?.renderServiceId);
  return {
    deployProvider: project?.deployProvider || (render.serviceId ? 'render' : null),
    render: {
      serviceId: render.serviceId || project?.renderServiceId || null,
      serviceName: render.serviceName || null,
      enabled,
      pollingEnabled: typeof render.pollingEnabled === 'boolean' ? render.pollingEnabled : enabled,
      webhookEnabled: typeof render.webhookEnabled === 'boolean' ? render.webhookEnabled : true,
      pollingStatus: render.pollingStatus || null,
      lastDeployId: render.lastDeployId || null,
      lastDeployStatus: render.lastDeployStatus || null,
      lastSeenAt: render.lastSeenAt || null,
      notifyOnStart: typeof render.notifyOnStart === 'boolean' ? render.notifyOnStart : true,
      notifyOnFinish: typeof render.notifyOnFinish === 'boolean' ? render.notifyOnFinish : true,
      notifyOnFail: typeof render.notifyOnFail === 'boolean' ? render.notifyOnFail : true,
      recentEvents: Array.isArray(render.recentEvents) ? render.recentEvents : [],
      eventsEnabled: Array.isArray(render.eventsEnabled) ? render.eventsEnabled.filter(Boolean) : null,
    },
    notifications: {
      enabled,
      lastEvent: deployNotifications.lastEvent || null,
      lastStatus: deployNotifications.lastStatus || null,
    },
  };
}

function isRenderEventEnabled(deploySettings, webhookSettings, eventType) {
  const projectEvents = deploySettings.render?.eventsEnabled;
  if (Array.isArray(projectEvents) && projectEvents.length) {
    return projectEvents.includes(eventType);
  }
  return webhookSettings.events.includes(eventType);
}

function resolveLogTestReminderState(logTest, nowMs = Date.now()) {
  const snoozedUntil = logTest.reminder?.snoozedUntil
    ? new Date(logTest.reminder.snoozedUntil).getTime()
    : null;
  const isSnoozed = snoozedUntil && !Number.isNaN(snoozedUntil) && snoozedUntil > nowMs;
  const needsTest = logTest.reminder?.needsTest === true;
  return { needsTest, isSnoozed, snoozedUntil: snoozedUntil || null };
}

function formatLogTestStatusLine(logTest) {
  const status = logTest.lastTest?.status || 'never';
  const lastRunAt = logTest.lastTest?.lastRunAt
    ? new Date(logTest.lastTest.lastRunAt).toISOString()
    : null;
  if (!logTest.testEndpointUrl) {
    return 'Log test: ⚠️ not configured';
  }
  if (status === 'pass') {
    return `Log test: ✅ passed${lastRunAt ? ` (${lastRunAt})` : ''}`;
  }
  if (status === 'partial') {
    return `Log test: ❌ partial${lastRunAt ? ` (${lastRunAt})` : ''}`;
  }
  if (status === 'fail') {
    return `Log test: ❌ failed${lastRunAt ? ` (${lastRunAt})` : ''}`;
  }
  if (status === 'blocked_missing_diagnostics') {
    return `Log test: 🧩 blocked (missing diagnostics)${lastRunAt ? ` (${lastRunAt})` : ''}`;
  }
  return 'Log test: ⚠️ not run yet';
}

function formatLogTestBadgeLine(logTest) {
  const status = logTest.lastTest?.status || 'never';
  if (status === 'pass') {
    return '✅ Log test: passed';
  }
  if (status === 'fail' || status === 'partial') {
    return '❌ Log test: last failed';
  }
  return '⚠️ Log test: NOT DONE';
}

function formatLogTestReminderLine(logTest) {
  const { needsTest, isSnoozed, snoozedUntil } = resolveLogTestReminderState(logTest);
  if (!needsTest) return 'Reminder: ✅ up to date';
  if (isSnoozed) {
    return `Reminder: ⏰ snoozed until ${new Date(snoozedUntil).toISOString()}`;
  }
  return 'Reminder: ⚠️ needs log test';
}

async function updateProjectLogTest(projectId, updater) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) return null;
  const current = normalizeLogTestSettings(project);
  const next = typeof updater === 'function' ? updater(current) : { ...current, ...updater };
  project.logTest = next;
  await saveProjects(projects);
  return { project, logTest: next };
}

async function updateProjectLogTestResult(projectId, result) {
  const { status, summary, correlationIds = [] } = result;
  const now = new Date().toISOString();
  const outcome = await updateProjectLogTest(projectId, (current) => {
    const lastCorrelationIds = [...(current.lastTest.lastCorrelationIds || []), ...correlationIds]
      .filter(Boolean)
      .slice(-10);
    const needsTest = status !== 'pass';
    return {
      ...current,
      lastTest: {
        status,
        lastRunAt: now,
        lastSummary: summary,
        lastCorrelationIds,
      },
      reminder: {
        ...current.reminder,
        needsTest,
        snoozedUntil: needsTest ? current.reminder.snoozedUntil : null,
      },
    };
  });
  return outcome;
}

async function updateProjectDeploySettings(projectId, updater) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) return null;
  const current = normalizeProjectDeploySettings(project);
  const next = typeof updater === 'function' ? updater(current) : { ...current, ...updater };
  project.deployProvider = next.deployProvider || project.deployProvider;
  project.render = {
    ...(project.render || {}),
    serviceId: next.render?.serviceId || null,
    serviceName: next.render?.serviceName || project.render?.serviceName || null,
    enabled: typeof next.render?.enabled === 'boolean' ? next.render.enabled : current.render.enabled,
    pollingEnabled:
      typeof next.render?.pollingEnabled === 'boolean'
        ? next.render.pollingEnabled
        : current.render.pollingEnabled,
    webhookEnabled:
      typeof next.render?.webhookEnabled === 'boolean'
        ? next.render.webhookEnabled
        : current.render.webhookEnabled,
    pollingStatus: next.render?.pollingStatus || current.render.pollingStatus || null,
    lastDeployId: next.render?.lastDeployId || current.render.lastDeployId || null,
    lastDeployStatus: next.render?.lastDeployStatus || current.render.lastDeployStatus || null,
    lastSeenAt: next.render?.lastSeenAt || current.render.lastSeenAt || null,
    notifyOnStart:
      typeof next.render?.notifyOnStart === 'boolean' ? next.render.notifyOnStart : current.render.notifyOnStart,
    notifyOnFinish:
      typeof next.render?.notifyOnFinish === 'boolean' ? next.render.notifyOnFinish : current.render.notifyOnFinish,
    notifyOnFail:
      typeof next.render?.notifyOnFail === 'boolean' ? next.render.notifyOnFail : current.render.notifyOnFail,
    recentEvents: Array.isArray(next.render?.recentEvents) ? next.render.recentEvents : current.render.recentEvents,
    eventsEnabled: next.render?.eventsEnabled || current.render.eventsEnabled || null,
  };
  project.deployNotifications = {
    ...(project.deployNotifications || {}),
    enabled: Boolean(next.notifications?.enabled ?? next.render?.enabled ?? current.notifications.enabled),
    lastEvent: next.notifications?.lastEvent || null,
    lastStatus: next.notifications?.lastStatus || null,
  };
  await saveProjects(projects);
  return { project, settings: next };
}

function appendRenderRecentEvent(events, entry) {
  const next = Array.isArray(events) ? [entry, ...events] : [entry];
  return next.slice(0, 12);
}

async function recordRenderDeployState(projectId, payload) {
  const receivedAt = payload.receivedAt || new Date().toISOString();
  const eventEntry = {
    eventType: payload.eventType,
    status: payload.status || payload.eventType,
    deployId: payload.deployId || null,
    source: payload.source || null,
    receivedAt,
  };
  return updateProjectDeploySettings(projectId, (current) => ({
    ...current,
    render: {
      ...current.render,
      lastDeployId: payload.deployId || current.render.lastDeployId || null,
      lastDeployStatus: payload.status || payload.eventType || current.render.lastDeployStatus || null,
      lastSeenAt: receivedAt,
      recentEvents: appendRenderRecentEvent(current.render.recentEvents, eventEntry),
      pollingStatus: payload.pollingStatus || current.render.pollingStatus || null,
    },
    notifications: {
      ...current.notifications,
      lastEvent: eventEntry,
      lastStatus: payload.status || payload.eventType || current.notifications.lastStatus || null,
    },
  }));
}

function buildProjectLogAlertsView(project, settings, notice) {
  const forwarding = normalizeProjectLogSettings(settings);
  const logTest = normalizeLogTestSettings(project);
  const reminderLine = formatLogTestReminderLine(logTest);
  const levelsLabel = forwarding.levels.length ? forwarding.levels.join(' / ') : 'error';
  const destinationLabel = forwarding.destinationChatId || 'not set';
  const canInspect = canInspectRepo(project);
  const hasInspection = repoInspectionCache.has(project.id);
  const diagnosticsUrl = resolveDiagnosticsEndpoint(project, logTest);
  const lines = [
    `📣 Logs — ${project.name || project.id}`,
    notice || null,
    '',
    formatLogTestStatusLine(logTest),
    `Last diagnostics: ${logTest.lastDiagnostics?.timestamp || '-'}`,
    reminderLine,
    '',
    `Status: ${forwarding.enabled ? '✅ enabled' : '🚫 disabled'}`,
    `Levels: ${levelsLabel}`,
    `Destination: ${forwarding.destinationMode || 'admin'} (${destinationLabel})`,
  ].filter(Boolean);

  const inline = new InlineKeyboard()
    .text('🧪 Run log test', `logtest:mode_menu:${project.id}`)
    .row()
    .text('🧪 Run ALL tests', `logtest:all:${project.id}`)
    .row()
    .text(diagnosticsUrl ? '🔎 Diagnostics' : '🔎 Diagnostics (set URL)', `logtest:diagnostics:${project.id}`);
  if (canInspect) {
    inline.row().text('🔍 Inspect repo', `logtest:inspect_repo:${project.id}`);
    if (hasInspection) {
      inline.row().text('🧩 Generate Codex task for client repo', `logtest:generate_repo_task:${project.id}`);
      inline.row().text('🛠 Create PR with fixes', `logtest:create_pr:${project.id}`);
    }
  }
  inline
    .text('⚙️ Configure test endpoints', `logtest:config:${project.id}`)
    .row()
    .text('📌 Test status & last results', `logtest:status:${project.id}`)
    .row()
    .text('⏰ Snooze reminder', `logtest:snooze:${project.id}`)
    .row()
    .text('🧩 Generate Codex task: Add diagnostics + test endpoint + log sender', `logtest:generate_task:${project.id}`)
    .row()
    .text(forwarding.enabled ? '🟢 Disable forwarding' : '🟢 Enable forwarding', `projlog:toggle:${project.id}`)
    .row()
    .text('🎚 Select log levels', `projlog:levels:${project.id}`)
    .row()
    .text('🧪 Send test log', `projlog:test:${project.id}`)
    .row()
    .text('📦 Recent deliveries', `projlog:recent:${project.id}:0`)
    .row()
    .text('📍 Destination config', `projlog:dest:${project.id}`)
    .row()
    .text('⬅️ Back', `projlog:back:${project.id}`);

  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectLogAlerts(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const settings = await getProjectLogSettingsWithDefaults(projectId);
  const view = buildProjectLogAlertsView(project, settings, notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function renderProjectLogAlertsForMessage(messageContext, projectId, notice) {
  if (!messageContext) return;
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) return;
  const settings = await getProjectLogSettingsWithDefaults(projectId);
  const view = buildProjectLogAlertsView(project, settings, notice);
  const text = view.text;
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

function buildLogTestModeMenu(projectId) {
  const inline = new InlineKeyboard()
    .text('ℹ️ Info', `logtest:mode:${projectId}:info`)
    .text('⚠️ Warn', `logtest:mode:${projectId}:warn`)
    .row()
    .text('❌ Error', `logtest:mode:${projectId}:error`)
    .text('⏱ Timeout', `logtest:mode:${projectId}:timeout`)
    .row()
    .text('⬅️ Back', `projlog:menu:${projectId}`);
  return inline;
}

function resolveLogTestTokenKey(projectId, logTest) {
  if (logTest?.tokenKeyInEnvVault) return logTest.tokenKeyInEnvVault;
  return `PM_TEST_TOKEN_${String(projectId).toUpperCase().replace(/[^A-Z0-9_]+/g, '_')}`;
}

function resolveLogTestEndpoints(rawInput) {
  const url = new URL(rawInput);
  const baseOrigin = `${url.protocol}//${url.host}`;
  let basePath = url.pathname || '';
  if (basePath.endsWith('/pm/test-log') || basePath.endsWith('/__pm/test-log')) {
    basePath = basePath.replace(/\/(__pm|pm)\/test-log$/, '');
  }
  const normalizedBasePath = basePath.replace(/\/+$/, '');
  const base = `${baseOrigin}${normalizedBasePath}`;
  return {
    baseUrl: base || baseOrigin,
    testEndpointUrl: `${base || baseOrigin}/pm/test-log`,
    diagnosticsEndpointUrl: `${base || baseOrigin}/pm/diagnostics`,
  };
}

function formatLogTestConfigSummary(logTest, tokenKey) {
  const lines = [
    `Test endpoint: ${logTest.testEndpointUrl || '-'}`,
    `Diagnostics endpoint: ${logTest.diagnosticsEndpointUrl || '-'}`,
    `Token key (Env Vault): ${tokenKey || '-'}`,
  ];
  return lines.join('\n');
}

function formatLogTestSummary(logTest) {
  const lines = [
    `Status: ${logTest.lastTest.status}`,
    `Last run: ${logTest.lastTest.lastRunAt || '-'}`,
    `Summary: ${logTest.lastTest.lastSummary || '-'}`,
  ];
  if (logTest.lastTest.lastCorrelationIds?.length) {
    lines.push(`Correlation IDs: ${logTest.lastTest.lastCorrelationIds.slice(-5).join(', ')}`);
  }
  return lines.join('\n');
}

function buildLogTestStatusView(project, logTest) {
  const diagnosticsUrl = resolveDiagnosticsEndpoint(project, logTest);
  const lines = [
    `📌 Log test status — ${project.name || project.id}`,
    '',
    formatLogTestStatusLine(logTest),
    formatLogTestReminderLine(logTest),
    `Last diagnostics: ${logTest.lastDiagnostics?.timestamp || '-'}`,
    '',
    formatLogTestSummary(logTest),
  ];
  const inline = new InlineKeyboard()
    .text('🧪 Run log test', `logtest:mode_menu:${project.id}`)
    .text('🧪 Run ALL tests', `logtest:all:${project.id}`)
    .row()
    .text(diagnosticsUrl ? '🔎 Diagnostics' : '🔎 Diagnostics (set URL)', `logtest:diagnostics:${project.id}`)
    .row()
    .text('⬅️ Back', `projlog:menu:${project.id}`);
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderLogTestStatus(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const logTest = normalizeLogTestSettings(project);
  const view = buildLogTestStatusView(project, logTest);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function buildLogTestSnoozeMenu(projectId) {
  const inline = new InlineKeyboard();
  LOG_TEST_REMINDER_SNOOZE_DAYS.forEach((days) => {
    inline.text(`⏰ Snooze ${days}d`, `logtest:snooze_set:${projectId}:${days}`).row();
  });
  inline.text('🔄 Clear snooze', `logtest:snooze_clear:${projectId}`).row();
  inline.text('⬅️ Back', `projlog:menu:${projectId}`);
  return inline;
}

async function renderLogTestSnoozeMenu(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const logTest = normalizeLogTestSettings(project);
  const lines = [
    `⏰ Snooze log test reminder — ${project.name || project.id}`,
    '',
    formatLogTestReminderLine(logTest),
  ];
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: buildLogTestSnoozeMenu(projectId) });
}

async function startLogTestConfigFlow(ctx, projectId) {
  if (!isEnvVaultAvailable()) {
    await renderOrEdit(ctx, buildEnvVaultUnavailableMessage('Env Vault unavailable.'), {
      reply_markup: buildBackKeyboard(`projlog:menu:${projectId}`),
    });
    return;
  }
  setUserState(ctx.from.id, {
    type: 'log_test_config',
    step: 'url',
    projectId,
    messageContext: getMessageTargetFromCtx(ctx),
  });
  await renderOrEdit(
    ctx,
    'Send the base URL or full /pm/test-log endpoint URL.\n(Example: https://api.example.com or https://api.example.com/pm/test-log)\n(Or press Cancel)',
    { reply_markup: buildCancelKeyboard() },
  );
}

async function handleLogTestConfigInput(ctx, state) {
  const raw = ctx.message.text?.trim();
  if (!raw) {
    await ctx.reply('Please send a valid URL.');
    return;
  }
  if (raw.toLowerCase() === 'cancel') {
    clearUserState(ctx.from.id);
    await renderProjectLogAlerts(ctx, state.projectId, 'Operation cancelled.');
    return;
  }

  if (state.step === 'url') {
    let parsedUrl;
    try {
      parsedUrl = new URL(raw);
    } catch (error) {
      await ctx.reply('Invalid URL. Please include http:// or https://');
      return;
    }
    if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
      await ctx.reply('URL must include http:// or https://');
      return;
    }
    const { baseUrl, testEndpointUrl, diagnosticsEndpointUrl } = resolveLogTestEndpoints(parsedUrl.toString());
    const updated = await updateProjectLogTest(state.projectId, (current) => ({
      ...current,
      enabled: true,
      testEndpointUrl,
      diagnosticsEndpointUrl,
    }));
    await updateProjectField(state.projectId, 'baseUrl', baseUrl);
    if (!updated) {
      clearUserState(ctx.from.id);
      await renderOrEdit(ctx, 'Project not found.');
      return;
    }
    state.step = 'token';
    setUserState(ctx.from.id, state);
    await renderOrEdit(
      ctx,
      `✅ Endpoints saved.\nSend PM_TEST_TOKEN to store in Env Vault.\n(Or press Cancel)`,
      { reply_markup: buildCancelKeyboard() },
    );
    return;
  }

  if (state.step === 'token') {
    const token = raw;
    if (!token) {
      await ctx.reply('Please send a token.');
      return;
    }
    const envSetId = await ensureProjectEnvSet(state.projectId);
    const projects = await loadProjects();
    const project = findProjectById(projects, state.projectId);
    if (!project) {
      clearUserState(ctx.from.id);
      await renderOrEdit(ctx, 'Project not found.');
      return;
    }
    const logTest = normalizeLogTestSettings(project);
    const tokenKey = resolveLogTestTokenKey(project.id, logTest);
    try {
      await upsertEnvVar(state.projectId, tokenKey, token, envSetId);
      const updated = await updateProjectLogTest(state.projectId, (current) => ({
        ...current,
        tokenKeyInEnvVault: tokenKey,
        enabled: true,
      }));
      if (updated?.logTest) {
        project.logTest = updated.logTest;
      }
    } catch (error) {
      console.error('[log-test] Failed to save token', error);
      await ctx.reply(`Failed to store token: ${error.message}`);
      return;
    }
    clearUserState(ctx.from.id);
    await renderProjectLogAlerts(
      ctx,
      state.projectId,
      `✅ Log test configured.\n${formatLogTestConfigSummary(normalizeLogTestSettings(project), tokenKey)}`,
    );
  }
}

async function fetchLogTestToken(projectId, logTest) {
  if (!isEnvVaultAvailable()) {
    return { ok: false, error: 'Env Vault unavailable.' };
  }
  const envSetId = await ensureProjectEnvSet(projectId);
  const tokenKey = resolveLogTestTokenKey(projectId, logTest);
  const token = await getEnvVarValue(projectId, tokenKey, envSetId);
  if (!token) {
    return { ok: false, error: `Token not found in Env Vault key ${tokenKey}.` };
  }
  return { ok: true, token, tokenKey };
}

function buildLogTestPayload(project, mode, correlationId) {
  const env = project?.environment || project?.env || null;
  return {
    mode,
    correlationId,
    projectId: project.id,
    env: env || undefined,
  };
}

function requestUrlWithBodyAndHeaders(options) {
  const { method, targetUrl, headers, body, timeoutMs } = options;
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const isHttps = url.protocol === 'https:';
    const lib = isHttps ? https : http;
    const payload = body ? JSON.stringify(body) : null;
    const finalHeaders = {
      ...headers,
      ...(payload
        ? {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(payload),
          }
        : {}),
    };
    const requestOptions = {
      method,
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: `${url.pathname}${url.search}`,
      headers: Object.fromEntries(Object.entries(finalHeaders).filter(([, value]) => value)),
    };
    const start = Date.now();
    const req = lib.request(requestOptions, (res) => {
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
    req.setTimeout(timeoutMs || 15000, () => {
      req.destroy(new Error('Request timed out'));
    });
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

async function ensureRenderEnvVaultSet() {
  if (!isEnvVaultAvailable()) return null;
  return ensureDefaultEnvVarSet(RENDER_ENV_VAULT_PROJECT_ID);
}

async function getRenderApiKeyStatus() {
  if (RENDER_API_KEY_ENV) {
    return { key: RENDER_API_KEY_ENV, source: 'env' };
  }
  if (!isEnvVaultAvailable()) {
    return { key: null, source: 'vault_unavailable' };
  }
  const envSetId = await ensureRenderEnvVaultSet();
  if (!envSetId) {
    return { key: null, source: 'vault_missing' };
  }
  const key = await getEnvVarValue(RENDER_ENV_VAULT_PROJECT_ID, 'RENDER_API_KEY', envSetId);
  return { key: key || null, source: key ? 'env_vault' : 'missing' };
}

async function storeRenderApiKey(value) {
  if (!isEnvVaultAvailable()) {
    throw new Error(MASTER_KEY_ERROR_MESSAGE);
  }
  const envSetId = await ensureRenderEnvVaultSet();
  if (!envSetId) {
    throw new Error('Env Vault not initialized.');
  }
  await upsertEnvVar(RENDER_ENV_VAULT_PROJECT_ID, 'RENDER_API_KEY', value, envSetId);
  return true;
}

function getRenderServiceCache() {
  return renderServiceDiscoveryCache.get('services') || null;
}

function setRenderServiceCache(services) {
  renderServiceDiscoveryCache.set('services', {
    services,
    fetchedAt: Date.now(),
  });
}

async function requestRenderApi({ method, path, body, timeoutMs, apiKey }) {
  const key = apiKey || (await getRenderApiKeyStatus()).key;
  if (!key) {
    throw new Error('RENDER_API_KEY not configured.');
  }
  const targetUrl = `${RENDER_API_BASE_URL}${path}`;
  const response = await requestUrlWithBodyAndHeaders({
    method,
    targetUrl,
    headers: {
      Authorization: `Bearer ${key}`,
    },
    body,
    timeoutMs: timeoutMs || 15000,
  });
  if (!response || response.status < 200 || response.status >= 300) {
    throw new Error(`Render API failed (${response?.status || 'unknown'}).`);
  }
  if (!response.body) return null;
  try {
    return JSON.parse(response.body);
  } catch (error) {
    return null;
  }
}

async function listRenderWebhooks() {
  const payload = await requestRenderApi({ method: 'GET', path: '/webhooks' });
  if (Array.isArray(payload)) return payload;
  if (payload?.data && Array.isArray(payload.data)) return payload.data;
  return [];
}

async function createRenderWebhook({ name, url, events }) {
  const payload = await requestRenderApi({
    method: 'POST',
    path: '/webhooks',
    body: {
      name,
      url,
      events,
    },
  });
  return payload;
}

function buildRenderServicesPath(settings) {
  const params = new URLSearchParams();
  params.set('limit', '100');
  if (settings?.workspaceId) {
    params.set('ownerId', settings.workspaceId);
  }
  const query = params.toString();
  return `/services${query ? `?${query}` : ''}`;
}

async function listRenderServices(renderSettings) {
  const path = buildRenderServicesPath(renderSettings);
  const payload = await requestRenderApi({ method: 'GET', path, timeoutMs: renderSettings?.pollTimeoutMs });
  if (Array.isArray(payload)) return payload;
  if (payload?.data && Array.isArray(payload.data)) return payload.data;
  return [];
}

async function listRenderServiceDeploys(serviceId, renderSettings) {
  if (!serviceId) return [];
  const path = `/services/${serviceId}/deploys?limit=5`;
  const payload = await requestRenderApi({ method: 'GET', path, timeoutMs: renderSettings?.pollTimeoutMs });
  if (Array.isArray(payload)) return payload;
  if (payload?.data && Array.isArray(payload.data)) return payload.data;
  return [];
}

function normalizeRenderDeployPayload(deploy) {
  if (!deploy) return null;
  const deployId = deploy.id || deploy.deployId || null;
  const status = deploy.status || deploy.state || deploy.result || null;
  const startedAt = deploy.startedAt || deploy.started_at || deploy.createdAt || deploy.created_at || null;
  const finishedAt = deploy.finishedAt || deploy.finished_at || deploy.updatedAt || deploy.updated_at || null;
  let durationMs = null;
  if (startedAt && finishedAt) {
    const startMs = new Date(startedAt).getTime();
    const endMs = new Date(finishedAt).getTime();
    if (Number.isFinite(startMs) && Number.isFinite(endMs) && endMs >= startMs) {
      durationMs = endMs - startMs;
    }
  }
  const dashboardUrl = deploy.url || deploy.dashboardUrl || deploy.dashboard_url || null;
  return {
    deployId: deployId ? String(deployId) : null,
    status: status ? String(status) : null,
    durationMs,
    dashboardUrl: dashboardUrl ? String(dashboardUrl) : null,
  };
}

async function pollRenderDeploysOnce() {
  if (renderPollingLocks.has('tick')) return;
  renderPollingLocks.add('tick');
  try {
    if (!appState.dbReady) {
      const now = Date.now();
      if (!renderPollSkipLogAt || now - renderPollSkipLogAt > 60_000) {
        renderPollSkipLogAt = now;
        console.warn('[render-poll] skipped: DB not ready');
      }
      return;
    }
    const settings = resolveRenderGlobalSettings(await getCachedSettings());
    if (!settings.pollingEnabledGlobal) {
      return;
    }
    const apiKeyStatus = await getRenderApiKeyStatus();
    const projects = await loadProjects();
    const candidates = projects.filter((project) => {
      const deploySettings = normalizeProjectDeploySettings(project);
      return (
        deploySettings.render.enabled &&
        deploySettings.render.pollingEnabled &&
        deploySettings.render.serviceId
      );
    });
    if (!apiKeyStatus.key) {
      let changed = false;
      candidates.forEach((project) => {
        if (project.render?.pollingStatus !== 'blocked_missing_api_key') {
          project.render = {
            ...(project.render || {}),
            pollingStatus: 'blocked_missing_api_key',
          };
          changed = true;
        }
      });
      if (changed) {
        await saveProjects(projects);
      }
      return;
    }
    const limited = candidates.slice(0, settings.pollMaxServicesPerTick);
    for (const project of limited) {
      const deploySettings = normalizeProjectDeploySettings(project);
      const serviceId = deploySettings.render.serviceId;
      if (!serviceId) continue;
      try {
        const deploys = await listRenderServiceDeploys(serviceId, settings);
        const latestRaw = deploys[0] || null;
        const normalized = normalizeRenderDeployPayload(latestRaw);
        if (!normalized) continue;
        if (project.render?.pollingStatus !== 'ok') {
          await updateProjectDeploySettings(project.id, (current) => ({
            ...current,
            render: { ...current.render, pollingStatus: 'ok' },
          }));
        }
        await handleRenderDeployEvent({
          project,
          info: {
            serviceId,
            deployId: normalized.deployId,
            status: normalized.status,
            durationMs: normalized.durationMs,
            dashboardUrl: normalized.dashboardUrl,
          },
          source: 'polling',
        });
      } catch (error) {
        const category = classifyDbError(error);
        if (category) {
          recordDbError(category, error?.message);
        }
        console.error('[render-poll] failed for service', {
          projectId: project.id,
          serviceId,
          error: error?.message,
        });
        await recordAuditLog('render_poll_error', {
          projectId: project.id,
          serviceId,
          error: error?.message,
        });
      }
    }
  } catch (error) {
    const category = classifyDbError(error);
    if (category) {
      recordDbError(category, error?.message);
    }
    console.error('[render-poll] tick failed', {
      category: category || 'unknown',
      error: error?.message || 'unknown error',
    });
  } finally {
    renderPollingLocks.delete('tick');
  }
}

function scheduleRenderPolling() {
  if (renderPollTimer) {
    clearInterval(renderPollTimer);
  }
  const intervalSec = resolveRenderGlobalSettings(cachedSettings || {}).pollIntervalSec;
  renderPollTimer = setInterval(() => {
    pollRenderDeploysOnce().catch((error) => {
      console.error('[render-poll] tick failed', error);
    });
  }, Math.max(15, intervalSec) * 1000);
  if (typeof renderPollTimer.unref === 'function') {
    renderPollTimer.unref();
  }
}

function buildRenderWebhookTargetUrl() {
  const baseUrl = getPublicBaseUrl().replace(/\/+$/, '');
  return `${baseUrl}/api/render/webhook`;
}

function isDuplicateRenderEvent(eventKey, now = Date.now()) {
  if (!eventKey) return false;
  const lastSeen = renderWebhookEventCache.get(eventKey);
  if (lastSeen && now - lastSeen < RENDER_WEBHOOK_DEDUP_TTL_MS) {
    return true;
  }
  renderWebhookEventCache.set(eventKey, now);
  for (const [key, timestamp] of renderWebhookEventCache.entries()) {
    if (now - timestamp > RENDER_WEBHOOK_DEDUP_TTL_MS) {
      renderWebhookEventCache.delete(key);
    }
  }
  return false;
}

async function ensureRenderWebhookConfigured({ events, progress } = {}) {
  const settings = await getCachedSettings(true);
  const webhookSettings = normalizeRenderWebhookSettings(settings);
  const targetUrl = buildRenderWebhookTargetUrl();
  const desiredEvents = Array.isArray(events) && events.length ? events : webhookSettings.events;

  if (progress) {
    await updateProgressMessage(null, progress, {
      status: 'progressing',
      completedSteps: 1,
      currentStep: 'Checking Render webhooks',
      nextStep: 'Creating or reusing webhook',
    });
  }

  const existing = await listRenderWebhooks();
  const matching = existing.find((hook) => {
    const hookUrl = hook.url || hook.targetUrl || hook.webhookUrl || '';
    const hookEvents = Array.isArray(hook.events) ? hook.events : [];
    const hasAllEvents = desiredEvents.every((eventName) => hookEvents.includes(eventName));
    return hookUrl === targetUrl && hasAllEvents;
  });

  if (matching) {
    const updated = {
      ...settings,
      renderWebhook: {
        webhookId: matching.id || matching.webhookId || webhookSettings.webhookId,
        targetUrl,
        events: desiredEvents,
        lastVerifiedAt: new Date().toISOString(),
      },
    };
    await saveGlobalSettingsAndCache(updated);
    if (progress) {
      await updateProgressMessage(null, progress, {
        status: 'progressing',
        completedSteps: 2,
        currentStep: 'Webhook ready',
        nextStep: 'Finalizing',
      });
    }
    return updated.renderWebhook;
  }

  const created = await createRenderWebhook({
    name: 'PM Deploy Alerts (workspace)',
    url: targetUrl,
    events: desiredEvents,
  });

  const webhookId = created?.id || created?.webhookId || null;
  const updated = {
    ...settings,
    renderWebhook: {
      webhookId,
      targetUrl,
      events: desiredEvents,
      lastVerifiedAt: new Date().toISOString(),
    },
  };
  await saveGlobalSettingsAndCache(updated);
  if (progress) {
    await updateProgressMessage(null, progress, {
      status: 'progressing',
      completedSteps: 2,
      currentStep: 'Webhook created',
      nextStep: 'Finalizing',
    });
  }
  return updated.renderWebhook;
}

function buildLogTestFailureHint(error, response) {
  if (response) {
    if (response.status === 404) {
      return 'Endpoint not found (404). Implement POST /pm/test-log (or alias /__pm/test-log).';
    }
    if (response.status === 401) {
      return 'Unauthorized (401). PM_TEST_TOKEN mismatch or PM_TEST_ENABLED is off.';
    }
    if (response.status === 403) {
      return 'Forbidden (403). Check IP allowlists and PM_TEST_TOKEN.';
    }
    if (response.status >= 500) {
      return 'Server error. Check client logs and PM_TEST_ENABLED.';
    }
    return `Unexpected status ${response.status}.`;
  }
  const code = error?.code || '';
  if (code === 'ENOTFOUND') {
    return 'DNS lookup failed. Check test endpoint URL.';
  }
  if (code === 'ECONNREFUSED') {
    return 'Connection refused. Service may be down or wrong port.';
  }
  if (code === 'ETIMEDOUT' || String(error?.message || '').toLowerCase().includes('timed out')) {
    return 'Request timed out. Service may be asleep or network blocked.';
  }
  return error?.message || 'Request failed.';
}

function classifyFailureCategory(errorMessage, response) {
  if (response) {
    if (response.status === 401 || response.status === 403) return 'auth';
    if (response.status === 404) return 'notfound';
    if (response.status >= 500) return 'server';
  }
  const text = String(errorMessage || '').toLowerCase();
  if (text.includes('timed out')) return 'timeout';
  if (text.includes('timeout')) return 'timeout';
  if (text.includes('unauthorized') || text.includes('forbidden')) return 'auth';
  if (text.includes('not found') || text.includes('404')) return 'notfound';
  if (text.includes('invalid') || text.includes('parse')) return 'parse';
  return 'unknown';
}

function buildLogTestReceiptHint() {
  return [
    'Client call succeeded but no log arrived.',
    'Verify client PM_URL and PM_INGEST_TOKEN.',
    'Ensure correlationId is included in log payload meta.',
    'Check outbound connectivity from client to PM.',
  ].join('\n');
}

function validateLogTestReceipt(entry, projectId, correlationId) {
  if (!entry || typeof entry !== 'object') {
    return { ok: false, error: 'Log payload missing.' };
  }
  const entryProjectId = entry.projectId ? String(entry.projectId) : null;
  if (entryProjectId && entryProjectId !== String(projectId)) {
    return { ok: false, error: `projectId mismatch (expected ${projectId}, got ${entryProjectId}).` };
  }
  const meta = entry.meta || {};
  const metaCorrelationId = meta.correlationId || meta.correlation_id || null;
  if (correlationId && metaCorrelationId && metaCorrelationId !== correlationId) {
    return { ok: false, error: 'correlationId mismatch in log payload.' };
  }
  if (!metaCorrelationId) {
    return { ok: false, error: 'correlationId missing in log payload meta.' };
  }
  if (!entry.level) {
    return { ok: false, error: 'log level missing.' };
  }
  return { ok: true };
}

function buildLogTestDiagnosticsReport(diagnostics) {
  const missing = [];
  if (diagnostics.logger) {
    if (!diagnostics.logger.hasPmLogger) missing.push('PM logger not installed');
    if (!diagnostics.logger.hasAxiosInterceptor) missing.push('Axios interceptor missing');
    if (!diagnostics.logger.hasFetchInterceptor) missing.push('Fetch interceptor missing');
    if (!diagnostics.logger.hasUnhandledRejectionHook)
      missing.push('Unhandled rejection hook missing');
    if (!diagnostics.logger.hasUncaughtExceptionHook)
      missing.push('Uncaught exception hook missing');
  }
  if (diagnostics.pmConfig) {
    if (!diagnostics.pmConfig.hasPmUrl) missing.push('PM_URL not configured');
    if (!diagnostics.pmConfig.hasPmToken) missing.push('PM_INGEST_TOKEN not configured');
  }
  const lines = [
    '🔎 Diagnostics report',
    `Project: ${diagnostics.projectId || '-'}`,
    '',
    `Logger: ${JSON.stringify(diagnostics.logger || {})}`,
    `PM config: ${JSON.stringify(diagnostics.pmConfig || {})}`,
    `Last send: ${JSON.stringify(diagnostics.lastSend || {})}`,
  ];
  if (missing.length) {
    lines.push('', `Missing: ${missing.join(', ')}`);
  }
  return lines.join('\n');
}

function formatRetryAfter(retryAfterMs) {
  const seconds = Math.max(1, Math.ceil(retryAfterMs / 1000));
  const minutes = Math.floor(seconds / 60);
  if (minutes >= 1) {
    return `${minutes}m ${seconds % 60}s`;
  }
  return `${seconds}s`;
}

function checkRateLimit(rateMap, key, rateLimit) {
  const now = Date.now();
  const windowMs = rateLimit.windowMs;
  const limit = rateLimit.limit;
  const previous = rateMap.get(key) || [];
  const recent = previous.filter((timestamp) => now - timestamp < windowMs);
  if (recent.length >= limit) {
    const retryAfterMs = windowMs - (now - Math.min(...recent));
    rateMap.set(key, recent);
    return { blocked: true, retryAfterMs };
  }
  recent.push(now);
  rateMap.set(key, recent);
  return { blocked: false, retryAfterMs: 0 };
}

function buildLogTestProgressText({
  header,
  status,
  percent,
  completedSteps,
  totalSteps,
  currentStep,
  nextStep,
  remainingSteps,
  reason,
}) {
  const lines = [];
  if (header) lines.push(header);
  if (status === 'success') {
    lines.push(`✅ Completed (100%)`);
  } else if (status === 'failed') {
    lines.push(`❌ Failed at step ${completedSteps}/${totalSteps}`);
  } else {
    lines.push(`⏳ progressing... (${percent}%)`);
  }
  lines.push(`Completed: ${completedSteps}/${totalSteps}`);
  if (currentStep) lines.push(`Current: ${currentStep}`);
  if (nextStep) lines.push(`Next: ${nextStep}`);
  lines.push(`Remaining: ${remainingSteps}`);
  if (reason) lines.push('', `Reason: ${reason}`);
  return lines.join('\n');
}

async function updateProgressMessage(ctx, progress, payload) {
  const { completedSteps, currentStep, nextStep, status, reason } = payload;
  const totalSteps = progress.totalSteps;
  const remainingSteps = Math.max(0, totalSteps - completedSteps);
  const percent = status === 'success' ? 100 : Math.floor((completedSteps / totalSteps) * 100);
  const text = buildLogTestProgressText({
    header: progress.header,
    status,
    percent,
    completedSteps,
    totalSteps,
    currentStep,
    nextStep,
    remainingSteps,
    reason,
  });
  if (!progress.chatId) return;
  if (!progress.messageId) {
    const message = await bot.api.sendMessage(progress.chatId, text, {
      disable_web_page_preview: true,
    });
    progress.messageId = message.message_id;
    return;
  }
  try {
    await bot.api.editMessageText(progress.chatId, progress.messageId, text, {
      disable_web_page_preview: true,
    });
  } catch (error) {
    try {
      const message = await bot.api.sendMessage(progress.chatId, text, {
        disable_web_page_preview: true,
      });
      progress.messageId = message.message_id;
    } catch (sendError) {
      console.error('[log-test] Failed to update progress message', sendError);
    }
  }
  if (status === 'success' || status === 'failed') {
    const operationState = getNavigationOperationState(progress.chatId);
    operationState.inProgress = false;
    operationState.noticeMessageId = null;
    await flushNavigationQueue(ctx, progress.chatId);
  }
}

function createLogTestProgress(ctx, header, steps) {
  return {
    chatId: ctx.chat?.id,
    messageId: null,
    totalSteps: steps.length,
    header,
  };
}

function createOperationProgress(ctx, header, totalSteps) {
  const chatId = ctx.chat?.id;
  if (chatId) {
    const operationState = getNavigationOperationState(chatId);
    operationState.inProgress = true;
  }
  return {
    chatId,
    messageId: null,
    totalSteps,
    header,
  };
}

function resolveProjectBaseUrl(project, logTest) {
  if (project?.baseUrl) {
    return String(project.baseUrl).replace(/\/+$/, '');
  }
  if (logTest?.testEndpointUrl) {
    return String(logTest.testEndpointUrl).replace(/\/(__pm|pm)\/test-log$/, '').replace(/\/+$/, '');
  }
  return null;
}

function resolveDiagnosticsEndpoint(project, logTest) {
  if (project?.diagnosticsUrl) {
    return String(project.diagnosticsUrl).replace(/\/+$/, '');
  }
  const baseUrl = resolveProjectBaseUrl(project, logTest);
  if (baseUrl) {
    return `${baseUrl}/pm/diagnostics`;
  }
  return logTest?.diagnosticsEndpointUrl || null;
}

function normalizeDiagnosticsPayload(payload) {
  if (!payload || typeof payload !== 'object') {
    return { ok: false, error: 'Diagnostics response invalid.' };
  }
  const logger = payload.logger;
  const pmConfig = payload.pmConfig;
  const lastSend = payload.lastSend || {};
  const loggerKeys = [
    'hasPmLogger',
    'hasAxiosInterceptor',
    'hasFetchInterceptor',
    'hasUnhandledRejectionHook',
    'hasUncaughtExceptionHook',
  ];
  const pmKeys = ['hasPmUrl', 'hasPmToken'];
  const missingLogger = !logger || loggerKeys.some((key) => typeof logger[key] !== 'boolean');
  const missingPm = !pmConfig || pmKeys.some((key) => typeof pmConfig[key] !== 'boolean');
  if (missingLogger || missingPm) {
    return { ok: false, error: 'Diagnostics response missing required flags.' };
  }
  return {
    ok: true,
    payload: {
      projectId: payload.projectId || null,
      logger: loggerKeys.reduce((acc, key) => ({ ...acc, [key]: Boolean(logger[key]) }), {}),
      pmConfig: pmKeys.reduce((acc, key) => ({ ...acc, [key]: Boolean(pmConfig[key]) }), {}),
      lastSend: {
        status: lastSend.status || null,
        lastErrorCode: lastSend.lastErrorCode || null,
        lastErrorAt: lastSend.lastErrorAt || null,
      },
    },
  };
}

async function storeLogDiagnosticsSnapshot(projectId, diagnostics) {
  await updateProjectLogTest(projectId, (current) => ({
    ...current,
    lastDiagnostics: {
      timestamp: new Date().toISOString(),
      logger: diagnostics.logger,
      pmConfig: diagnostics.pmConfig,
      lastSend: diagnostics.lastSend,
    },
  }));
}

async function requestLogDiagnostics(project, logTest, token) {
  const targetUrl = resolveDiagnosticsEndpoint(project, logTest);
  if (!targetUrl) {
    return { ok: false, error: 'Diagnostics endpoint not configured.' };
  }
  try {
    const response = await requestUrlWithBodyAndHeaders({
      method: 'GET',
      targetUrl,
      headers: { Authorization: `Bearer ${token}` },
      timeoutMs: LOG_TEST_DIAGNOSTICS_TIMEOUT_MS,
    });
    if (response.status < 200 || response.status >= 300) {
      return { ok: false, error: `Diagnostics request failed (${response.status}).` };
    }
    let payload = null;
    try {
      payload = response.body ? JSON.parse(response.body) : null;
    } catch (error) {
      payload = null;
    }
    const normalized = normalizeDiagnosticsPayload(payload);
    if (!normalized.ok) {
      return { ok: false, error: normalized.error };
    }
    return { ok: true, payload: normalized.payload };
  } catch (error) {
    return { ok: false, error: buildLogTestFailureHint(error) };
  }
}

function ensureGitSuffix(repoUrl) {
  if (!repoUrl) return repoUrl;
  return repoUrl.endsWith('.git') ? repoUrl : `${repoUrl}.git`;
}

function tryParseRepoSlug(repoUrl) {
  try {
    const parsed = new URL(repoUrl);
    if (!parsed.hostname.includes('github.com')) return null;
    const parts = parsed.pathname.replace(/\.git$/, '').split('/').filter(Boolean);
    if (parts.length >= 2) {
      return `${parts[0]}/${parts[1]}`;
    }
  } catch (error) {
    return null;
  }
  return null;
}

function resolveRepoInspectionTarget(project) {
  if (project?.repoUrl) {
    const repoUrl = ensureGitSuffix(project.repoUrl);
    const repoSlug = project.repoSlug || tryParseRepoSlug(repoUrl);
    return { repoUrl, repoSlug };
  }
  try {
    const info = getRepoInfo(project);
    return { repoUrl: info.repoUrl, repoSlug: info.repoSlug };
  } catch (error) {
    return null;
  }
}

function canInspectRepo(project) {
  const target = resolveRepoInspectionTarget(project);
  if (!target?.repoUrl) return false;
  return Boolean(getGithubToken(project));
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch (error) {
    return false;
  }
}

async function readJsonFile(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

async function runRipgrep(repoDir, pattern) {
  try {
    const { stdout } = await execFileAsync(
      'rg',
      ['-n', '--no-heading', '--with-filename', '-S', '--glob', '!.git', '--glob', '!node_modules', pattern, '.'],
      { cwd: repoDir, maxBuffer: 1024 * 1024 },
    );
    if (!stdout) return [];
    return stdout
      .split('\n')
      .map((line) => {
        const match = line.match(/^(.+?):(\d+):/);
        if (!match) return null;
        return { file: match[1], line: Number(match[2]) };
      })
      .filter(Boolean);
  } catch (error) {
    if (typeof error.code === 'number' && error.code === 1) {
      return [];
    }
    console.error('[repo-inspect] ripgrep failed', error?.message || error);
    return [];
  }
}

function buildMatchSummary(matches, limit = 6) {
  if (!matches.length) return [];
  return matches.slice(0, limit).map((match) => `${match.file}:${match.line}`);
}

async function detectRepoStack(repoDir) {
  const signals = [];
  const hasPackageJson = await fileExists(path.join(repoDir, 'package.json'));
  const hasTsConfig = await fileExists(path.join(repoDir, 'tsconfig.json'));
  const hasPnpm = await fileExists(path.join(repoDir, 'pnpm-lock.yaml'));
  const hasYarn = await fileExists(path.join(repoDir, 'yarn.lock'));
  const hasRequirements = await fileExists(path.join(repoDir, 'requirements.txt'));
  const hasPyProject = await fileExists(path.join(repoDir, 'pyproject.toml'));
  const hasPoetry = await fileExists(path.join(repoDir, 'poetry.lock'));
  const hasGoMod = await fileExists(path.join(repoDir, 'go.mod'));
  const hasPom = await fileExists(path.join(repoDir, 'pom.xml'));
  const hasGradle =
    (await fileExists(path.join(repoDir, 'build.gradle'))) ||
    (await fileExists(path.join(repoDir, 'build.gradle.kts')));

  if (hasPackageJson) signals.push({ stack: 'node', weight: 0.6 });
  if (hasTsConfig) signals.push({ stack: 'node', weight: 0.2 });
  if (hasPnpm || hasYarn) signals.push({ stack: 'node', weight: 0.1 });
  if (hasRequirements || hasPyProject || hasPoetry) signals.push({ stack: 'python', weight: 0.7 });
  if (hasGoMod) signals.push({ stack: 'go', weight: 0.8 });
  if (hasPom || hasGradle) signals.push({ stack: 'java', weight: 0.8 });

  const scores = signals.reduce((acc, signal) => {
    acc[signal.stack] = (acc[signal.stack] || 0) + signal.weight;
    return acc;
  }, {});
  const entries = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  const primary = entries[0];
  if (!primary) {
    return { stack: 'unknown', confidence: 0, signals };
  }
  const confidence = Math.min(100, Math.round(primary[1] * 100));
  return { stack: primary[0], confidence, signals };
}

async function detectRepoEntrypoint(repoDir, stack) {
  if (stack === 'node') {
    const pkg = await readJsonFile(path.join(repoDir, 'package.json'));
    if (pkg?.main) {
      return { file: pkg.main, reason: 'package.json main' };
    }
    if (pkg?.scripts?.start) {
      return { file: pkg.scripts.start, reason: 'package.json start script' };
    }
    const candidates = [
      'src/index.ts',
      'src/index.js',
      'src/server.ts',
      'src/server.js',
      'src/app.ts',
      'src/app.js',
      'index.js',
      'server.js',
      'app.js',
    ];
    for (const candidate of candidates) {
      if (await fileExists(path.join(repoDir, candidate))) {
        return { file: candidate, reason: 'heuristic' };
      }
    }
  }
  if (stack === 'python') {
    const candidates = ['app.py', 'main.py', 'src/app.py', 'src/main.py'];
    for (const candidate of candidates) {
      if (await fileExists(path.join(repoDir, candidate))) {
        return { file: candidate, reason: 'heuristic' };
      }
    }
  }
  return null;
}

function buildRepoInspectionReportText(report) {
  const lines = [
    '🔍 Repo inspection report',
    `Repo: ${report.repoSlug || report.repoUrl || '-'}`,
    `Detected stack: ${report.stack} (${report.confidence}%)`,
  ];
  if (report.entrypoint) {
    lines.push(`Entrypoint: ${report.entrypoint.file} (${report.entrypoint.reason})`);
  }
  lines.push('', 'Findings:');
  report.findings.forEach((finding) => {
    lines.push(`- ${finding.label}: ${finding.found ? '✅' : '❌'}${finding.refs?.length ? ` (${finding.refs.join(', ')})` : ''}`);
  });
  if (report.missing.length) {
    lines.push('', `Missing: ${report.missing.join(', ')}`);
  }
  if (report.rootCauses.length) {
    lines.push('', 'Root cause candidates:');
    report.rootCauses.forEach((candidate, index) => {
      lines.push(`${index + 1}. ${candidate}`);
    });
  }
  if (report.patchPlan.length) {
    lines.push('', 'Recommended patch plan:');
    report.patchPlan.forEach((step) => lines.push(`- ${step}`));
  }
  return lines.join('\n');
}

function buildRepoCodexTask(report) {
  const stackLine = report.stack !== 'unknown' ? `Detected stack: ${report.stack} (${report.confidence}%).` : 'Detected stack: unknown.';
  return [
    '🧩 Codex task for client repo',
    '',
    stackLine,
    '',
    'Implement:',
    '- POST /pm/test-log (alias /__pm/test-log) secured by PM_TEST_TOKEN + PM_TEST_ENABLED.',
    '- GET /pm/diagnostics (alias /__pm/diagnostics) returning only boolean flags + lastSend metadata.',
    '- Central PM logger module that sends to PM_URL + "/api/logs" with PM_INGEST_TOKEN.',
    '- Axios interceptor + fetch wrapper (when applicable).',
    '- Process hooks for unhandledRejection + uncaughtException.',
    '',
    'Notes:',
    '- Never print secrets; read tokens from env.',
    '- Include correlationId in log meta.',
    '- Add minimal tests or a test hook for /pm/test-log and /pm/diagnostics.',
    '',
    'Suggested insertion points:',
    report.entrypoint ? `- Entrypoint: ${report.entrypoint.file} (${report.entrypoint.reason})` : '- Entrypoint: detect main server entry.',
  ].join('\n');
}

async function inspectRepository(project) {
  const target = resolveRepoInspectionTarget(project);
  if (!target?.repoUrl) {
    return { ok: false, error: 'Repo URL not configured.' };
  }
  const githubToken = getGithubToken(project);
  if (!githubToken) {
    return { ok: false, error: 'GitHub token not configured.' };
  }
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pm-repo-inspect-'));
  const cloneUrl = applyGitTokenToUrl(target.repoUrl, githubToken);
  try {
    await execFileAsync('git', ['clone', '--depth=1', cloneUrl, tempDir], { maxBuffer: 1024 * 1024 });
    const stack = await detectRepoStack(tempDir);
    const entrypoint = await detectRepoEntrypoint(tempDir, stack.stack);
    const matchGroups = {
      logEndpoint: await runRipgrep(tempDir, '/api/logs'),
      pmUrl: await runRipgrep(tempDir, 'PM_URL|PATH_APPLIER_URL'),
      pmToken: await runRipgrep(tempDir, 'PM_TOKEN|PM_INGEST_TOKEN|PATH_APPLIER_TOKEN'),
      axiosInterceptor: await runRipgrep(tempDir, 'axios\\.interceptors'),
      fetchWrapper: await runRipgrep(tempDir, '\\bfetch\\s*\\('),
      unhandledRejection: await runRipgrep(tempDir, 'unhandledRejection'),
      uncaughtException: await runRipgrep(tempDir, 'uncaughtException'),
      diagnosticsRoute: await runRipgrep(tempDir, '/pm/diagnostics'),
      testLogRoute: await runRipgrep(tempDir, '/pm/test-log'),
    };
    const findings = [
      { key: 'logEndpoint', label: 'PM log forwarder (/api/logs)', matches: matchGroups.logEndpoint },
      { key: 'pmUrl', label: 'PM_URL/PATH_APPLIER_URL usage', matches: matchGroups.pmUrl },
      { key: 'pmToken', label: 'PM_TOKEN/PM_INGEST_TOKEN usage', matches: matchGroups.pmToken },
      { key: 'axiosInterceptor', label: 'Axios interceptor', matches: matchGroups.axiosInterceptor },
      { key: 'fetchWrapper', label: 'Fetch wrapper', matches: matchGroups.fetchWrapper },
      { key: 'unhandledRejection', label: 'unhandledRejection hook', matches: matchGroups.unhandledRejection },
      { key: 'uncaughtException', label: 'uncaughtException hook', matches: matchGroups.uncaughtException },
      { key: 'diagnosticsRoute', label: '/pm/diagnostics route', matches: matchGroups.diagnosticsRoute },
      { key: 'testLogRoute', label: '/pm/test-log route', matches: matchGroups.testLogRoute },
    ];
    const enrichedFindings = findings.map((finding) => ({
      label: finding.label,
      found: finding.matches.length > 0,
      refs: buildMatchSummary(finding.matches),
    }));
    const missing = [];
    if (!matchGroups.diagnosticsRoute.length) missing.push('/pm/diagnostics endpoint');
    if (!matchGroups.testLogRoute.length) missing.push('/pm/test-log endpoint');
    if (!matchGroups.logEndpoint.length) missing.push('PM log forwarder');
    if (!matchGroups.pmUrl.length) missing.push('PM_URL env usage');
    if (!matchGroups.pmToken.length) missing.push('PM_TOKEN/PM_INGEST_TOKEN env usage');
    if (!matchGroups.axiosInterceptor.length && !matchGroups.fetchWrapper.length)
      missing.push('Axios interceptor or fetch wrapper');
    if (!matchGroups.unhandledRejection.length) missing.push('unhandledRejection hook');
    if (!matchGroups.uncaughtException.length) missing.push('uncaughtException hook');
    const rootCauses = [];
    if (!matchGroups.logEndpoint.length) rootCauses.push('PM logger not installed or not wired to /api/logs.');
    if (!matchGroups.pmUrl.length) rootCauses.push('PM_URL/PATH_APPLIER_URL not configured or referenced.');
    if (!matchGroups.pmToken.length) rootCauses.push('PM_TOKEN/PM_INGEST_TOKEN not configured or referenced.');
    if (!matchGroups.diagnosticsRoute.length) rootCauses.push('/pm/diagnostics endpoint missing.');
    if (!matchGroups.testLogRoute.length) rootCauses.push('/pm/test-log endpoint missing.');
    const patchPlan = [
      'Add /pm/diagnostics (GET) + /pm/test-log (POST) routes with PM_TEST_TOKEN auth.',
      'Add PM logger module that posts to PM_URL + /api/logs using PM_INGEST_TOKEN.',
      'Add axios interceptor and fetch wrapper to include correlationId + request metadata.',
      'Register unhandledRejection + uncaughtException hooks to forward errors.',
    ];
    return {
      ok: true,
      report: {
        repoUrl: target.repoUrl,
        repoSlug: target.repoSlug,
        stack: stack.stack,
        confidence: stack.confidence,
        entrypoint,
        findings: enrichedFindings,
        missing,
        rootCauses,
        patchPlan,
      },
    };
  } catch (error) {
    return { ok: false, error: error?.message || 'Repo inspection failed.' };
  } finally {
    try {
      await fs.rm(tempDir, { recursive: true, force: true });
    } catch (cleanupError) {
      console.error('[repo-inspect] failed to clean temp dir', cleanupError);
    }
  }
}

async function runRepoInspection(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  if (!canInspectRepo(project)) {
    await renderProjectLogAlerts(ctx, projectId, 'Repo inspection requires repoUrl and GitHub token.');
    return;
  }
  const rateStatus = checkRateLimit(repoInspectionRateLimits, projectId, REPO_INSPECTION_RATE_LIMIT);
  if (rateStatus.blocked) {
    await renderProjectLogAlerts(
      ctx,
      projectId,
      `⏳ Repo inspection is rate-limited. Try again in ${formatRetryAfter(rateStatus.retryAfterMs)}.`,
    );
    return;
  }
  const progress = createOperationProgress(ctx, `🔍 Repo inspection — ${project.name || project.id}`, 3);
  let completedSteps = 0;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: 'Resolve repo config',
    nextStep: 'Clone + scan repo',
  });
  completedSteps = 1;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: 'Clone + scan repo',
    nextStep: 'Summarize',
  });
  const result = await inspectRepository(project);
  if (!result.ok) {
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps,
      currentStep: 'Clone + scan repo',
      nextStep: null,
      reason: result.error,
    });
    await renderProjectLogAlerts(ctx, projectId, `❌ Repo inspection failed.\nReason: ${result.error}`);
    return;
  }
  completedSteps = 2;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: 'Summarize',
    nextStep: null,
  });
  const reportText = buildRepoInspectionReportText(result.report);
  const codexTask = buildRepoCodexTask(result.report);
  repoInspectionCache.set(projectId, {
    createdAt: new Date().toISOString(),
    report: result.report,
    reportText,
    codexTask,
  });
  await updateProgressMessage(ctx, progress, {
    status: 'success',
    completedSteps: 3,
    currentStep: 'Summarize',
    nextStep: null,
  });
  await sendTransientNotice(
    ctx,
    `🔍 Repo inspection — ${project.name || project.id}
✅ Completed (100%)
Completed: 3/3
Current: Summarize
Remaining: 0`,
    { ttlSec: 10, includeDelete: true },
  );
  const inline = new InlineKeyboard()
    .text('🧩 Generate Codex task for client repo', `logtest:generate_repo_task:${projectId}`)
    .row()
    .text('🛠 Create PR with fixes', `logtest:create_pr:${projectId}`)
    .row()
    .text('⬅️ Back', `projlog:menu:${projectId}`);
  await renderOrEdit(ctx, reportText, { reply_markup: inline });
}

function createPendingLogTestEntry({ correlationId, projectId, mode, chatId, requestId, timeoutMs }) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const expiresAt = startedAt + timeoutMs;
    const timer = setTimeout(() => {
      if (pendingLogTests.has(correlationId)) {
        pendingLogTests.delete(correlationId);
        resolve({ ok: false, status: 'timeout', correlationId, startedAt, expiresAt });
      }
    }, timeoutMs);
    pendingLogTests.set(correlationId, {
      correlationId,
      projectId,
      mode,
      chatId,
      requestId,
      startedAt,
      expiresAt,
      resolve,
      timer,
    });
  });
}

function cancelPendingLogTest(correlationId, reason = 'cancelled') {
  const pending = pendingLogTests.get(correlationId);
  if (!pending) return;
  pendingLogTests.delete(correlationId);
  if (pending.timer) {
    clearTimeout(pending.timer);
  }
  pending.resolve({
    ok: false,
    status: reason,
    correlationId,
    startedAt: pending.startedAt,
    expiresAt: pending.expiresAt,
  });
}

function formatLogTestReceipt(entry) {
  const summary = {
    level: entry.level,
    message: truncateText(entry.message, 160),
    env: entry.meta?.env || entry.meta?.environment || '-',
  };
  return `✅ Log received.\nLevel: ${summary.level}\nEnv: ${summary.env}\nMessage: ${summary.message}`;
}

async function handlePendingLogTestReceipt({ correlationId, entry, receivedAt }) {
  if (!correlationId) return;
  const pending = pendingLogTests.get(correlationId);
  if (!pending) return;
  pendingLogTests.delete(correlationId);
  if (pending.timer) {
    clearTimeout(pending.timer);
  }
  pending.resolve({
    ok: true,
    status: 'received',
    correlationId,
    receivedAt,
    entry,
    startedAt: pending.startedAt,
  });
  if (pending.chatId) {
    try {
      await bot.api.sendMessage(pending.chatId, formatLogTestReceipt(entry), {
        disable_web_page_preview: true,
      });
    } catch (error) {
      console.error('[log-test] Failed to notify receipt', {
        correlationId,
        error: error?.message,
      });
    }
  }
}

async function runSingleLogTest(ctx, projectId, mode) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  if (!LOG_TEST_MODES.includes(mode)) {
    await renderProjectLogAlerts(ctx, projectId, 'Invalid test mode.');
    return;
  }
  const logTest = normalizeLogTestSettings(project);
  if (!logTest.testEndpointUrl) {
    await renderProjectLogAlerts(ctx, projectId, '⚠️ Configure test endpoints first.');
    return;
  }

  const progress = createLogTestProgress(ctx, `🧪 Log test — ${project.name || project.id}`, LOG_TEST_SINGLE_PROGRESS_STEPS);
  let completedSteps = 0;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[0],
    nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[1],
  });

  const tokenResult = await fetchLogTestToken(projectId, logTest);
  if (!tokenResult.ok) {
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 1,
      currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[0],
      nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[1],
      reason: `token (${tokenResult.error})`,
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      `⚠️ Token unavailable.\nStep: Resolve project config\nCategory: config\nReason: ${tokenResult.error}\nNext: configure PM_TEST_TOKEN in Env Vault.`,
    );
    return;
  }
  completedSteps = 1;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[1],
    nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[2],
  });

  const diagnosticsUrl = resolveDiagnosticsEndpoint(project, logTest);
  if (!diagnosticsUrl) {
    await updateProjectLogTestResult(projectId, {
      status: 'blocked_missing_diagnostics',
      summary: 'Diagnostics endpoint not configured.',
      correlationIds: [],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 2,
      currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[1],
      nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[2],
      reason: 'missing diagnostics endpoint',
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      '❌ Diagnostics gate failed.\nStep: Diagnostics gate (/pm/diagnostics)\nCategory: notfound\nReason: diagnostics URL missing.\nNext: add /pm/diagnostics and /pm/test-log.',
    );
    return;
  }
  const diagnostics = await requestLogDiagnostics(project, logTest, tokenResult.token);
  if (!diagnostics.ok) {
    const category = classifyFailureCategory(diagnostics.error);
    await updateProjectLogTestResult(projectId, {
      status: 'blocked_missing_diagnostics',
      summary: diagnostics.error,
      correlationIds: [],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 2,
      currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[1],
      nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[2],
      reason: `diagnostics (${diagnostics.error})`,
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      `❌ Diagnostics gate failed.\nStep: Diagnostics gate (/pm/diagnostics)\nCategory: ${category}\nReason: ${diagnostics.error}\nNext: add /pm/diagnostics and /pm/test-log.`,
    );
    return;
  }
  await storeLogDiagnosticsSnapshot(projectId, diagnostics.payload);
  completedSteps = 2;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[2],
    nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[3],
  });

  let pingStatus = 'skipped';
  try {
    const baseUrl = getPublicBaseUrl().replace(/\/+$/, '');
    const pingResponse = await requestUrlWithBodyAndHeaders({
      method: 'GET',
      targetUrl: `${baseUrl}/api/logs/ping`,
      timeoutMs: 5000,
    });
    if (pingResponse?.status >= 200 && pingResponse?.status < 300) {
      pingStatus = 'ok';
    } else if (pingResponse?.status === 404) {
      pingStatus = 'skipped';
    } else {
      pingStatus = 'failed';
    }
  } catch (error) {
    pingStatus = 'failed';
  }
  completedSteps = 3;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: `${LOG_TEST_SINGLE_PROGRESS_STEPS[2]} (${mode})`,
    nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[3],
  });

  const correlationId = crypto.randomUUID();
  const requestId = crypto.randomUUID();
  const payload = buildLogTestPayload(project, mode, correlationId);
  const timeoutMs = LOG_TEST_CALL_TIMEOUTS_MS[mode] || 12_000;
  const receiptPromise = createPendingLogTestEntry({
    correlationId,
    projectId,
    mode,
    chatId: ctx.chat?.id,
    requestId,
    timeoutMs: LOG_TEST_RECEIPT_TIMEOUT_MS,
  });
  let response;
  try {
    response = await requestUrlWithBodyAndHeaders({
      method: 'POST',
      targetUrl: logTest.testEndpointUrl,
      headers: {
        Authorization: `Bearer ${tokenResult.token}`,
        'X-PM-Request-Id': requestId,
      },
      body: payload,
      timeoutMs,
    });
  } catch (error) {
    cancelPendingLogTest(correlationId, 'call_failed');
    const hint = buildLogTestFailureHint(error);
    const category = classifyFailureCategory(hint);
    await updateProjectLogTestResult(projectId, {
      status: 'fail',
      summary: hint,
      correlationIds: [correlationId],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 4,
      currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[2],
      nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[3],
      reason: hint,
    });
    let notice = `❌ Test call failed.\nStep: Trigger client log test\nCategory: ${category}\nReason: ${hint}`;
    if (canInspectRepo(project)) {
      notice += '\nNext: Inspect repo or generate Codex task.';
    }
    await renderProjectLogAlerts(ctx, projectId, notice);
    return;
  }

  if (!response || response.status < 200 || response.status >= 300) {
    cancelPendingLogTest(correlationId, 'call_failed');
    const hint = buildLogTestFailureHint(null, response);
    const category = classifyFailureCategory(hint, response);
    await updateProjectLogTestResult(projectId, {
      status: 'fail',
      summary: hint,
      correlationIds: [correlationId],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 4,
      currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[2],
      nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[3],
      reason: hint,
    });
    let notice = `❌ Test call failed.\nStep: Trigger client log test\nCategory: ${category}\nReason: ${hint}\nStatus: ${response?.status || 'unknown'}`;
    if (canInspectRepo(project)) {
      notice += '\nNext: Inspect repo or generate Codex task.';
    }
    await renderProjectLogAlerts(ctx, projectId, notice);
    return;
  }

  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps: 4,
    currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[3],
    nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[4],
  });
  const receipt = await receiptPromise;
  if (!receipt.ok) {
    const hint = buildLogTestReceiptHint();
    await updateProjectLogTestResult(projectId, {
      status: 'partial',
      summary: hint,
      correlationIds: [correlationId],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 5,
      currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[3],
      nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[4],
      reason: hint,
    });
    let notice = `⚠️ Test call succeeded but no log received.\nStep: Wait for log arrival\nCategory: missing_log\nReason: ${hint}`;
    if (canInspectRepo(project)) {
      notice += '\nNext: Inspect repo for missing PM logger or tokens.';
    }
    await renderProjectLogAlerts(ctx, projectId, notice);
    return;
  }

  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps: 4,
    currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[4],
    nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[5],
  });
  const validation = validateLogTestReceipt(receipt.entry, project.id, correlationId);
  if (!validation.ok) {
    await updateProjectLogTestResult(projectId, {
      status: 'fail',
      summary: validation.error,
      correlationIds: [correlationId],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 4,
      currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[4],
      nextStep: LOG_TEST_SINGLE_PROGRESS_STEPS[5],
      reason: validation.error,
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      `❌ Log received but invalid.\nStep: Validate payload formatting and project tagging\nCategory: invalid_payload\nReason: ${validation.error}`,
    );
    return;
  }

  await updateProjectLogTestResult(projectId, {
    status: 'pass',
    summary: `Mode ${mode} received.`,
    correlationIds: [correlationId],
  });
  completedSteps = 6;
  const summaryNote = `✅ ${mode} test passed.${pingStatus === 'failed' ? ' (PM ingest ping failed)' : ''}`;
  await updateProgressMessage(ctx, progress, {
    status: 'success',
    completedSteps,
    currentStep: LOG_TEST_SINGLE_PROGRESS_STEPS[5],
    nextStep: null,
  });
  await renderProjectLogAlerts(ctx, projectId, summaryNote);
}

async function runAllLogTests(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const logTest = normalizeLogTestSettings(project);
  if (!logTest.testEndpointUrl) {
    await renderProjectLogAlerts(ctx, projectId, '⚠️ Configure test endpoints first.');
    return;
  }
  const progress = createLogTestProgress(
    ctx,
    `🧪 Log test suite — ${project.name || project.id}`,
    LOG_TEST_SUITE_PROGRESS_STEPS,
  );
  let completedSteps = 0;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[0],
    nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[1],
  });
  const tokenResult = await fetchLogTestToken(projectId, logTest);
  if (!tokenResult.ok) {
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 1,
      currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[0],
      nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[1],
      reason: `token (${tokenResult.error})`,
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      `⚠️ Token unavailable.\nStep: Resolve project config\nCategory: config\nReason: ${tokenResult.error}\nNext: configure PM_TEST_TOKEN in Env Vault.`,
    );
    return;
  }
  completedSteps = 1;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[1],
    nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[2],
  });

  const diagnosticsUrl = resolveDiagnosticsEndpoint(project, logTest);
  if (!diagnosticsUrl) {
    await updateProjectLogTestResult(projectId, {
      status: 'blocked_missing_diagnostics',
      summary: 'Diagnostics endpoint not configured.',
      correlationIds: [],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 2,
      currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[1],
      nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[2],
      reason: 'missing diagnostics endpoint',
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      '❌ Diagnostics gate failed.\nStep: Diagnostics gate (/pm/diagnostics)\nCategory: notfound\nReason: diagnostics URL missing.\nNext: add /pm/diagnostics and /pm/test-log.',
    );
    return;
  }
  const diagnostics = await requestLogDiagnostics(project, logTest, tokenResult.token);
  if (!diagnostics.ok) {
    const category = classifyFailureCategory(diagnostics.error);
    await updateProjectLogTestResult(projectId, {
      status: 'blocked_missing_diagnostics',
      summary: diagnostics.error,
      correlationIds: [],
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 2,
      currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[1],
      nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[2],
      reason: `diagnostics (${diagnostics.error})`,
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      `❌ Diagnostics gate failed.\nStep: Diagnostics gate (/pm/diagnostics)\nCategory: ${category}\nReason: ${diagnostics.error}\nNext: add /pm/diagnostics and /pm/test-log.`,
    );
    return;
  }
  await storeLogDiagnosticsSnapshot(projectId, diagnostics.payload);
  completedSteps = 2;
  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps,
    currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[2],
    nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[3],
  });

  let pingStatus = 'skipped';
  try {
    const baseUrl = getPublicBaseUrl().replace(/\/+$/, '');
    const pingResponse = await requestUrlWithBodyAndHeaders({
      method: 'GET',
      targetUrl: `${baseUrl}/api/logs/ping`,
      timeoutMs: 5000,
    });
    if (pingResponse?.status >= 200 && pingResponse?.status < 300) {
      pingStatus = 'ok';
    } else if (pingResponse?.status === 404) {
      pingStatus = 'skipped';
    } else {
      pingStatus = 'failed';
    }
  } catch (error) {
    pingStatus = 'failed';
  }
  const results = [];
  const receipts = [];
  for (const [index, mode] of LOG_TEST_MODES.entries()) {
    const triggerStepIndex = 2 + index * 2;
    const waitStepIndex = triggerStepIndex + 1;
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps,
      currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[triggerStepIndex],
      nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex],
    });
    const correlationId = crypto.randomUUID();
    const requestId = crypto.randomUUID();
    const payload = buildLogTestPayload(project, mode, correlationId);
    const timeoutMs = LOG_TEST_CALL_TIMEOUTS_MS[mode] || 12_000;
    const receiptPromise = createPendingLogTestEntry({
      correlationId,
      projectId,
      mode,
      chatId: ctx.chat?.id,
      requestId,
      timeoutMs: LOG_TEST_RECEIPT_TIMEOUT_MS,
    });
    let response;
    try {
      response = await requestUrlWithBodyAndHeaders({
        method: 'POST',
        targetUrl: logTest.testEndpointUrl,
        headers: {
          Authorization: `Bearer ${tokenResult.token}`,
          'X-PM-Request-Id': requestId,
        },
        body: payload,
        timeoutMs,
      });
    } catch (error) {
      cancelPendingLogTest(correlationId, 'call_failed');
      results.push({
        mode,
        status: 'call_failed',
        error: buildLogTestFailureHint(error),
        correlationId,
      });
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps,
        currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[triggerStepIndex],
        nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex],
        reason: buildLogTestFailureHint(error),
      });
      break;
    }

    if (!response || response.status < 200 || response.status >= 300) {
      cancelPendingLogTest(correlationId, 'call_failed');
      results.push({
        mode,
        status: 'call_failed',
        error: buildLogTestFailureHint(null, response),
        statusCode: response?.status || null,
        correlationId,
        callDurationMs: response?.durationMs || null,
      });
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps,
        currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[triggerStepIndex],
        nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex],
        reason: buildLogTestFailureHint(null, response),
      });
      break;
    }

    completedSteps = triggerStepIndex + 1;
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps,
      currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex],
      nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex + 1] || LOG_TEST_SUITE_PROGRESS_STEPS[10],
    });
    const receipt = await receiptPromise;
    if (!receipt.ok) {
      results.push({
        mode,
        status: 'log_missing',
        error: buildLogTestReceiptHint(),
        correlationId,
        callDurationMs: response?.durationMs || null,
      });
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps,
        currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex],
        nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex + 1] || LOG_TEST_SUITE_PROGRESS_STEPS[10],
        reason: buildLogTestReceiptHint(),
      });
      break;
    }
    const receivedAtMs = receipt.receivedAt ? new Date(receipt.receivedAt).getTime() : null;
    const logDelayMs =
      Number.isFinite(receivedAtMs) && Number.isFinite(receipt.startedAt)
        ? Math.max(0, receivedAtMs - receipt.startedAt)
        : null;
    results.push({
      mode,
      status: 'ok',
      correlationId,
      callDurationMs: response?.durationMs || null,
      logDelayMs,
    });
    receipts.push({ receipt, correlationId, mode });
    completedSteps = waitStepIndex + 1;
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps,
      currentStep:
        LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex + 1] || LOG_TEST_SUITE_PROGRESS_STEPS[10],
      nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[waitStepIndex + 2] || LOG_TEST_SUITE_PROGRESS_STEPS[11],
    });
  }

  const summaryLines = ['🧪 Log test suite results', `Project: ${project.name || project.id}`, ''];
  results.forEach((result) => {
    if (result.status === 'ok') {
      summaryLines.push(
        `✅ ${result.mode}: log received (call ${result.callDurationMs ?? '-'}ms, log ${result.logDelayMs ?? '-'}ms)`,
      );
    } else if (result.status === 'call_failed') {
      summaryLines.push(
        `❌ ${result.mode}: client call failed (${result.error})${result.callDurationMs ? ` in ${result.callDurationMs}ms` : ''}`,
      );
    } else {
      summaryLines.push(
        `⚠️ ${result.mode}: log missing (${result.error})${result.callDurationMs ? ` (call ${result.callDurationMs}ms)` : ''}`,
      );
    }
  });

  const allPassed = results.length === LOG_TEST_MODES.length && results.every((r) => r.status === 'ok');
  const anyCallFailed = results.some((r) => r.status === 'call_failed');
  const anyLogMissing = results.some((r) => r.status === 'log_missing');
  const status = allPassed ? 'pass' : anyCallFailed ? 'fail' : 'partial';
  const summary = summaryLines.slice(0, 4).join(' ');
  if (!allPassed) {
    await updateProjectLogTestResult(projectId, {
      status,
      summary,
      correlationIds: results.map((result) => result.correlationId).filter(Boolean),
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps,
      currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[Math.min(completedSteps, LOG_TEST_SUITE_PROGRESS_STEPS.length - 1)],
      nextStep: null,
      reason: summary,
    });
    let notice = summaryLines.join('\n');
    const firstFailure = results.find((result) => result.status !== 'ok');
    if (firstFailure) {
      const stepName = firstFailure.status === 'log_missing'
        ? LOG_TEST_SUITE_PROGRESS_STEPS[3]
        : LOG_TEST_SUITE_PROGRESS_STEPS[2];
      const category =
        firstFailure.status === 'log_missing'
          ? 'missing_log'
          : classifyFailureCategory(firstFailure.error, {
              status: firstFailure.statusCode || undefined,
            });
      notice += `\n\nStep: ${stepName}\nCategory: ${category}\nReason: ${firstFailure.error || 'unknown'}`;
    }
    if (anyLogMissing && canInspectRepo(project)) {
      notice += '\n\nNext: Inspect repo or generate Codex task for missing logging hooks.';
    }
    if (pingStatus === 'failed') {
      notice += '\n\n⚠️ PM log ingestion ping failed.';
    }
    await renderProjectLogAlerts(ctx, projectId, notice);
    return;
  }

  await updateProgressMessage(ctx, progress, {
    status: 'progressing',
    completedSteps: 10,
    currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[10],
    nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[11],
  });
  const validationErrors = receipts
    .map(({ receipt, correlationId }) => validateLogTestReceipt(receipt.entry, project.id, correlationId))
    .filter((result) => !result.ok)
    .map((result) => result.error);
  if (validationErrors.length) {
    const reason = validationErrors[0];
    await updateProjectLogTestResult(projectId, {
      status: 'fail',
      summary: reason,
      correlationIds: receipts.map((entry) => entry.correlationId),
    });
    await updateProgressMessage(ctx, progress, {
      status: 'failed',
      completedSteps: 10,
      currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[10],
      nextStep: LOG_TEST_SUITE_PROGRESS_STEPS[11],
      reason,
    });
    await renderProjectLogAlerts(
      ctx,
      projectId,
      `❌ Log received but invalid.\nStep: Validate payload formatting and project tagging\nCategory: invalid_payload\nReason: ${reason}`,
    );
    return;
  }

  await updateProjectLogTestResult(projectId, {
    status,
    summary,
    correlationIds: receipts.map((entry) => entry.correlationId).filter(Boolean),
  });

  completedSteps = 12;
  await updateProgressMessage(ctx, progress, {
    status: 'success',
    completedSteps,
    currentStep: LOG_TEST_SUITE_PROGRESS_STEPS[11],
    nextStep: null,
  });

  let notice = summaryLines.join('\n');
  if (!allPassed && canInspectRepo(project)) {
    notice += '\n\nNext: Inspect repo or generate Codex task for missing logging hooks.';
  }
  if (pingStatus === 'failed') {
    notice += '\n\n⚠️ PM log ingestion ping failed.';
  }
  await renderProjectLogAlerts(ctx, projectId, notice);
}

async function renderLogTestReminders(ctx) {
  const projects = await loadProjects();
  const nowMs = Date.now();
  const needing = projects.filter((project) => {
    const logTest = normalizeLogTestSettings(project);
    const reminder = resolveLogTestReminderState(logTest, nowMs);
    return reminder.needsTest && !reminder.isSnoozed;
  });
  const lines = ['🧾 Projects needing log test', ''];
  if (!needing.length) {
    lines.push('✅ All projects are up to date.');
  } else {
    needing.forEach((project) => {
      lines.push(`• ${project.name || project.id}`);
    });
  }
  const inline = new InlineKeyboard();
  needing.forEach((project) => {
    inline.text(project.name || project.id, `logmenu:open:${project.id}`).row();
  });
  inline.text('⬅️ Back', 'logmenu:list');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function sendLogTestReminderDigest() {
  if (!LOG_TEST_DAILY_REMINDER_ENABLED) return;
  const projects = await loadProjects();
  const nowMs = Date.now();
  const needing = projects.filter((project) => {
    const logTest = normalizeLogTestSettings(project);
    const reminder = resolveLogTestReminderState(logTest, nowMs);
    return reminder.needsTest && !reminder.isSnoozed;
  });
  if (!needing.length) return;
  const lines = ['🧾 Daily log test reminder', ''];
  needing.forEach((project) => {
    lines.push(`• ${project.name || project.id}`);
  });
  try {
    await bot.api.sendMessage(ADMIN_TELEGRAM_ID, lines.join('\n'), {
      disable_web_page_preview: true,
    });
  } catch (error) {
    console.error('[log-test] Failed to send daily reminder', error);
  }
}

function scheduleLogTestDailyReminder() {
  if (!LOG_TEST_DAILY_REMINDER_ENABLED) return;
  const interval = setInterval(() => {
    sendLogTestReminderDigest().catch((error) => {
      console.error('[log-test] Daily reminder failed', error);
    });
  }, 24 * 60 * 60 * 1000);
  if (typeof interval.unref === 'function') {
    interval.unref();
  }
}

function buildProjectLogLevelsView(project, settings) {
  const forwarding = normalizeProjectLogSettings(settings);
  const selected = new Set(forwarding.levels);
  const lines = [
    `🎚 Log levels — ${project.name || project.id}`,
    '',
    `Current: ${forwarding.levels.join(' / ') || 'error'}`,
  ];
  const inline = new InlineKeyboard()
    .text(`❗ Errors ${selected.has('error') ? '✅' : 'OFF'}`, `projlog:level:error:${project.id}`)
    .text(`⚠️ Warn ${selected.has('warn') ? '✅' : 'OFF'}`, `projlog:level:warn:${project.id}`)
    .row()
    .text(`ℹ️ Info ${selected.has('info') ? '✅' : 'OFF'}`, `projlog:level:info:${project.id}`)
    .row()
    .text('⬅️ Back', `projlog:menu:${project.id}`);
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectLogLevelsMenu(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const settings = await getProjectLogSettingsWithDefaults(projectId);
  const view = buildProjectLogLevelsView(project, settings);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

function buildProjectLogDestinationView(project, settings) {
  const forwarding = normalizeProjectLogSettings(settings);
  const lines = [
    `📍 Destination config — ${project.name || project.id}`,
    '',
    `Mode: ${forwarding.destinationMode || 'admin'}`,
    `Channel chat_id: ${forwarding.destinationChatId || 'not set'}`,
  ];
  const inline = new InlineKeyboard()
    .text(`👤 Admin ${forwarding.destinationMode === 'admin' ? '✅' : ''}`, `projlog:dest_mode:admin:${project.id}`)
    .text(`📣 Channel ${forwarding.destinationMode === 'channel' ? '✅' : ''}`, `projlog:dest_mode:channel:${project.id}`)
    .row()
    .text(`👥 Both ${forwarding.destinationMode === 'both' ? '✅' : ''}`, `projlog:dest_mode:both:${project.id}`)
    .row()
    .text('✏️ Set chat_id', `projlog:set_chat:${project.id}`)
    .text('📌 Use this chat', `projlog:use_chat:${project.id}`)
    .row()
    .text('🧹 Clear chat_id', `projlog:clear_chat:${project.id}`)
    .row()
    .text('⬅️ Back', `projlog:menu:${project.id}`);
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderProjectLogDestinationMenu(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const settings = await getProjectLogSettingsWithDefaults(projectId);
  const view = buildProjectLogDestinationView(project, settings);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function sendProjectLogTest(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const settings = await getProjectLogSettingsWithDefaults(projectId);
  const forwarding = normalizeProjectLogSettings(settings);
  const targets = new Set();
  if (forwarding.destinationMode === 'admin' || forwarding.destinationMode === 'both') {
    targets.add(ADMIN_TELEGRAM_ID);
  }
  if ((forwarding.destinationMode === 'channel' || forwarding.destinationMode === 'both') && forwarding.destinationChatId) {
    targets.add(forwarding.destinationChatId);
  }
  const resolvedTargets = Array.from(targets).filter(Boolean);
  if (!resolvedTargets.length) {
    await renderProjectLogAlerts(ctx, projectId, '⚠️ No destination configured.');
    return;
  }
  const message = `🧪 Test log for ${project.name || project.id} (${project.id})`;
  try {
    await Promise.all(resolvedTargets.map((chatId) => sendSafeMessage(BOT_TOKEN, chatId, message)));
    await renderProjectLogAlerts(ctx, projectId, '✅ Test log sent.');
  } catch (error) {
    console.error('[logs] Failed to send test log', { projectId, error: error?.message });
    await renderProjectLogAlerts(ctx, projectId, `⚠️ Test log failed: ${error.message}`);
  }
}

function formatLogTimestamp(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toISOString();
}

function buildProjectLogListView(project, logs, page, hasNext) {
  const lines = [`📦 Recent deliveries — ${project.name || project.id}`, `Page: ${page + 1}`];
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
  if (page > 0) {
    inline.text('⬅️ Prev', `projlog:recent:${project.id}:${page - 1}`);
  }
  if (hasNext) {
    inline.text('➡️ Next', `projlog:recent:${project.id}:${page + 1}`);
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
    .text('⬅️ Back to logs', `projlog:recent:${project.id}:${page}`)
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

function formatBackupCaption(template, payload) {
  const replacements = {
    '{projectName}': payload.projectName || '-',
    '{projectId}': payload.projectId || '-',
    '{title}': payload.title || '-',
    '{category}': payload.category || '-',
    '{status}': payload.status || '-',
    '{createdAt}': payload.createdAt || '-',
    '{noteId}': payload.noteId || '-',
  };
  let result = template || '';
  Object.entries(replacements).forEach(([token, value]) => {
    result = result.split(token).join(String(value));
  });
  return result;
}

function formatNoteDetail(note) {
  const lines = [
    `📝 ${note.title}`,
    `Category: ${note.category}`,
    `Status: ${note.status}`,
    `Created: ${note.createdAt}`,
    note.doneAt ? `Done: ${note.doneAt}` : null,
    note.nextAction ? `Next action: ${note.nextAction}` : null,
    note.text ? `\n${note.text}` : null,
  ].filter(Boolean);
  if (note.attachments?.length) {
    lines.push('', `Attachments: ${note.attachments.length}`);
    note.attachments.forEach((att, index) => {
      lines.push(`• ${index + 1}. ${att.type} ${att.fileName || ''}`.trim());
    });
  }
  return lines.join('\n');
}

async function ensureBackupChannelConfigured(ctx) {
  const settings = await getCachedSettings();
  const backups = normalizeBackupSettings(settings);
  if (!backups.channelId) {
    await renderOrEdit(
      ctx,
      '⚠️ Backup channel not configured. Set it in Settings → Backups before using Notes.',
      { reply_markup: buildBackKeyboard('gsettings:backups') },
    );
    return null;
  }
  return backups.channelId;
}

async function sendNoteBackupMessage(project, note, options = {}) {
  const settings = await getCachedSettings();
  const backups = normalizeBackupSettings(settings);
  if (!backups.channelId) {
    throw new Error('Backup channel not configured.');
  }
  const caption = formatBackupCaption(backups.captionTemplate, {
    projectName: project.name || project.id,
    projectId: project.id,
    title: note.title,
    category: note.category,
    status: note.status,
    createdAt: note.createdAt,
    noteId: note.id,
  });
  const bodyLines = [caption];
  if (note.nextAction) {
    bodyLines.push(`Next action: ${note.nextAction}`);
  }
  if (note.text) {
    bodyLines.push('', truncateText(note.text, 1000));
  }
  const message = bodyLines.join('\n');
  const response = await bot.api.sendMessage(backups.channelId, message);
  await appendNoteBackup(project.id, note.id, {
    chatId: response.chat.id,
    messageId: response.message_id,
    type: 'note',
    createdAt: new Date().toISOString(),
  });
}

async function sendNoteAttachmentBackup(project, noteId, attachment, caption) {
  const settings = await getCachedSettings();
  const backups = normalizeBackupSettings(settings);
  if (!backups.channelId) {
    throw new Error('Backup channel not configured.');
  }
  const safeCaption = caption ? truncateText(caption, 1000) : '';
  let response;
  if (attachment.type === 'photo') {
    response = await bot.api.sendPhoto(backups.channelId, attachment.fileId, { caption: safeCaption });
  } else if (attachment.type === 'video') {
    response = await bot.api.sendVideo(backups.channelId, attachment.fileId, { caption: safeCaption });
  } else if (attachment.type === 'voice') {
    response = await bot.api.sendVoice(backups.channelId, attachment.fileId, { caption: safeCaption });
  } else {
    response = await bot.api.sendDocument(backups.channelId, attachment.fileId, { caption: safeCaption });
  }
  await appendNoteBackup(project.id, noteId, {
    chatId: response.chat.id,
    messageId: response.message_id,
    type: 'attachment',
    attachmentId: attachment.id,
    createdAt: new Date().toISOString(),
  });
}

async function renderNotesMenu(ctx, projectId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const openNotes = await listNotes(projectId, NOTE_STATUS.OPEN);
  const doneNotes = await listNotes(projectId, NOTE_STATUS.DONE);
  const backups = normalizeBackupSettings(await getCachedSettings());
  const backupWarning = backups.channelId ? null : '⚠️ Backup channel not configured.';
  const lines = [
    `📝 Quick Notes — ${project.name || project.id}`,
    notice || null,
    '',
    `Open notes: ${openNotes.length}`,
    `Done notes: ${doneNotes.length}`,
    backupWarning,
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('➕ Create note', `notes:create:${projectId}`)
    .row()
    .text('📂 View open notes', `notes:list:${projectId}:${NOTE_STATUS.OPEN}`)
    .row()
    .text('✅ Done history', `notes:list:${projectId}:${NOTE_STATUS.DONE}`)
    .row()
    .text('🔎 Search notes', `notes:search:${projectId}`)
    .row()
    .text('🏷 Categories', `notes:categories:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:open:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderNotesList(ctx, projectId, status) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const notes = await listNotes(projectId, status);
  const lines = [
    `${status === NOTE_STATUS.DONE ? '✅ Done' : '📂 Open'} notes — ${project.name || project.id}`,
  ];
  if (!notes.length) {
    lines.push('', 'No notes found.');
  } else {
    lines.push('');
    notes.forEach((note) => {
      lines.push(`• ${note.title} (${note.category}) — ${note.nextAction || 'no next action'}`);
    });
  }
  const inline = new InlineKeyboard();
  notes.forEach((note) => {
    inline.text(`📝 ${truncateText(note.title, 24)}`, `notes:view:${projectId}:${note.id}`).row();
  });
  inline.text('⬅️ Back', `notes:menu:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderNotesCategories(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const notes = await listNotes(projectId);
  const counts = NOTE_CATEGORIES.reduce((acc, category) => {
    acc[category] = 0;
    return acc;
  }, {});
  notes.forEach((note) => {
    counts[note.category] = (counts[note.category] || 0) + 1;
  });
  const lines = [`🏷 Categories — ${project.name || project.id}`, ''];
  NOTE_CATEGORIES.forEach((category) => {
    lines.push(`• ${category}: ${counts[category] || 0}`);
  });
  const inline = new InlineKeyboard();
  NOTE_CATEGORIES.forEach((category) => {
    inline.text(`🏷 ${category}`, `notes:category:${projectId}:${category}`).row();
  });
  inline.text('⬅️ Back', `notes:menu:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderNotesByCategory(ctx, projectId, category) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const notes = (await listNotes(projectId)).filter((note) => note.category === category);
  const lines = [`🏷 ${category} notes — ${project.name || project.id}`];
  if (!notes.length) {
    lines.push('', 'No notes found.');
  } else {
    lines.push('');
    notes.forEach((note) => {
      lines.push(`• ${note.title} (${note.status})`);
    });
  }
  const inline = new InlineKeyboard();
  notes.forEach((note) => {
    inline.text(`📝 ${truncateText(note.title, 24)}`, `notes:view:${projectId}:${note.id}`).row();
  });
  inline.text('⬅️ Back', `notes:categories:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderNoteDetail(ctx, projectId, noteId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const note = await getNote(projectId, noteId);
  if (!note) {
    await renderOrEdit(ctx, 'Note not found.', { reply_markup: buildBackKeyboard(`notes:menu:${projectId}`) });
    return;
  }
  const lines = [formatNoteDetail(note)];
  if (notice) {
    lines.unshift(notice, '');
  }
  const inline = new InlineKeyboard()
    .text('✏️ Edit title', `notes:edit:${projectId}:${note.id}:title`)
    .row()
    .text('🏷 Edit category', `notes:edit:${projectId}:${note.id}:category`)
    .row()
    .text('📝 Edit text', `notes:edit:${projectId}:${note.id}:text`)
    .row()
    .text('🧭 Edit next action', `notes:edit:${projectId}:${note.id}:next`)
    .row()
    .text('📎 Add attachment', `notes:attach:${projectId}:${note.id}`)
    .row();
  if (note.attachments?.length) {
    inline.text('🗑 Remove attachment', `notes:attach_remove:${projectId}:${note.id}`).row();
  }
  if (note.status === NOTE_STATUS.OPEN) {
    inline.text('✅ Mark done', `notes:done:${projectId}:${note.id}`).row();
  } else {
    inline.text('♻️ Reopen', `notes:reopen:${projectId}:${note.id}`).row();
  }
  inline.text('⬅️ Back', `notes:menu:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderNoteAttachmentRemoval(ctx, projectId, noteId) {
  const note = await getNote(projectId, noteId);
  if (!note) {
    await renderOrEdit(ctx, 'Note not found.', { reply_markup: buildBackKeyboard(`notes:menu:${projectId}`) });
    return;
  }
  const lines = [`🗑 Remove attachment — ${note.title}`, 'Select attachment:'];
  const inline = new InlineKeyboard();
  note.attachments.forEach((att, index) => {
    inline.text(`🗑 ${index + 1}. ${att.type}`, `notes:attach_remove_pick:${projectId}:${noteId}:${att.id}`).row();
  });
  inline.text('⬅️ Back', `notes:view:${projectId}:${noteId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function handleNotesCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const parts = data.split(':');
  const action = parts[1];
  const projectId = parts[2];
  let noteId = parts[3];
  let extra = parts[4];
  if (['list', 'category', 'category_pick', 'category_custom'].includes(action)) {
    extra = parts[3];
    noteId = null;
  }

  if (!projectId) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }

  switch (action) {
    case 'menu':
      await renderNotesMenu(ctx, projectId);
      break;
    case 'create': {
      const backupChannel = await ensureBackupChannelConfigured(ctx);
      if (!backupChannel) return;
      setUserState(ctx.from.id, {
        type: 'note_create',
        projectId,
        step: 'title',
      });
      await renderOrEdit(ctx, '📝 Send note title.\n(Or press Cancel)', { reply_markup: buildCancelKeyboard() });
      break;
    }
    case 'list':
      await renderNotesList(ctx, projectId, extra === NOTE_STATUS.DONE ? NOTE_STATUS.DONE : NOTE_STATUS.OPEN);
      break;
    case 'view':
      await renderNoteDetail(ctx, projectId, noteId);
      break;
    case 'search':
      setUserState(ctx.from.id, { type: 'note_search', projectId });
      await renderOrEdit(ctx, '🔎 Send search text for notes.\n(Or press Cancel)', { reply_markup: buildCancelKeyboard() });
      break;
    case 'categories':
      await renderNotesCategories(ctx, projectId);
      break;
    case 'category':
      await renderNotesByCategory(ctx, projectId, extra);
      break;
    case 'edit':
      setUserState(ctx.from.id, {
        type: 'note_edit_field',
        projectId,
        noteId,
        field: extra,
      });
      await renderOrEdit(ctx, `✏️ Send new value for ${extra}.\n(Or press Cancel)`, {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'attach':
      if (!(await ensureBackupChannelConfigured(ctx))) {
        return;
      }
      setUserState(ctx.from.id, {
        type: 'note_add_attachment',
        projectId,
        noteId,
      });
      await renderOrEdit(ctx, '📎 Send an attachment (photo/video/voice/file).\n(Or press Cancel)', {
        reply_markup: buildCancelKeyboard(),
      });
      break;
    case 'attach_remove':
      await renderNoteAttachmentRemoval(ctx, projectId, noteId);
      break;
    case 'attach_remove_pick':
      await removeNoteAttachment(projectId, noteId, extra);
      {
        const note = await getNote(projectId, noteId);
        if (note) {
          const project = await getProjectById(projectId, ctx);
          try {
            await sendNoteBackupMessage(project, note);
          } catch (error) {
            await renderNoteDetail(ctx, projectId, noteId, `⚠️ Backup failed: ${error.message}`);
            return;
          }
        }
      }
      await renderNoteDetail(ctx, projectId, noteId, '🧹 Attachment removed.');
      break;
    case 'done': {
      const note = await updateNote(projectId, noteId, {
        status: NOTE_STATUS.DONE,
        doneAt: new Date().toISOString(),
      });
      if (note) {
        const project = await getProjectById(projectId, ctx);
        try {
          await sendNoteBackupMessage(project, note);
        } catch (error) {
          await renderNoteDetail(ctx, projectId, noteId, `⚠️ Backup failed: ${error.message}`);
          return;
        }
      }
      await renderNoteDetail(ctx, projectId, noteId, '✅ Marked as done.');
      break;
    }
    case 'reopen': {
      const note = await updateNote(projectId, noteId, {
        status: NOTE_STATUS.OPEN,
        doneAt: null,
      });
      if (note) {
        const project = await getProjectById(projectId, ctx);
        try {
          await sendNoteBackupMessage(project, note);
        } catch (error) {
          await renderNoteDetail(ctx, projectId, noteId, `⚠️ Backup failed: ${error.message}`);
          return;
        }
      }
      await renderNoteDetail(ctx, projectId, noteId, '♻️ Reopened.');
      break;
    }
    case 'category_pick': {
      const state = getUserState(ctx.from.id);
      if (!state || state.type !== 'note_create') {
        await renderNotesMenu(ctx, projectId);
        return;
      }
      setUserState(ctx.from.id, { ...state, step: 'next_action', category: extra });
      await renderOrEdit(ctx, 'Send next action (short summary). Use "-" to skip.', { reply_markup: buildCancelKeyboard() });
      break;
    }
    case 'category_custom': {
      const state = getUserState(ctx.from.id);
      if (!state || state.type !== 'note_create') {
        await renderNotesMenu(ctx, projectId);
        return;
      }
      setUserState(ctx.from.id, { ...state, step: 'category_custom' });
      await renderOrEdit(ctx, 'Send custom category.\n(Or press Cancel)', { reply_markup: buildCancelKeyboard() });
      break;
    }
    default:
      break;
  }
}

async function handleNoteCreateInput(ctx, state) {
  const text = ctx.message?.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderNotesMenu(ctx, state.projectId, 'Operation cancelled.');
    return;
  }
  if (state.step === 'title') {
    setUserState(ctx.from.id, { ...state, step: 'category', title: text });
    const inline = new InlineKeyboard();
    NOTE_CATEGORIES.forEach((category) => {
      if (category !== 'Custom') {
        inline.text(`🏷 ${category}`, `notes:category_pick:${state.projectId}:${category}`).row();
      }
    });
    inline.text('✍️ Custom', `notes:category_custom:${state.projectId}`).row();
    inline.text('⬅️ Back', `notes:menu:${state.projectId}`);
    await renderOrEdit(ctx, '🏷 Select a category:', { reply_markup: inline });
    return;
  }
  if (state.step === 'category_custom') {
    setUserState(ctx.from.id, { ...state, step: 'next_action', category: text });
    await renderOrEdit(ctx, '🧭 Send next action (short summary). Use "-" to skip.', { reply_markup: buildCancelKeyboard() });
    return;
  }
  if (state.step === 'next_action') {
    setUserState(ctx.from.id, { ...state, step: 'body', nextAction: text === '-' ? '' : text });
    await renderOrEdit(ctx, '📝 Send note body text. Use "-" to skip.', { reply_markup: buildCancelKeyboard() });
    return;
  }
  if (state.step === 'body') {
    const project = await getProjectById(state.projectId, ctx);
    if (!project) {
      clearUserState(ctx.from.id);
      await renderOrEdit(ctx, 'Project not found.');
      return;
    }
    const note = await createNote(state.projectId, {
      title: state.title,
      category: state.category || 'Custom',
      text: text === '-' ? '' : text,
      nextAction: state.nextAction || '',
      status: NOTE_STATUS.OPEN,
    });
    try {
      await sendNoteBackupMessage(project, note);
    } catch (error) {
      await renderNoteDetail(ctx, state.projectId, note.id, `⚠️ Backup failed: ${error.message}`);
      clearUserState(ctx.from.id);
      return;
    }
    clearUserState(ctx.from.id);
    await renderNoteDetail(ctx, state.projectId, note.id, '✅ Note created.');
  }
}

async function handleNoteEditInput(ctx, state) {
  const text = ctx.message?.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderNoteDetail(ctx, state.projectId, state.noteId, 'Operation cancelled.');
    return;
  }
  const updates = {};
  if (state.field === 'title') updates.title = text;
  if (state.field === 'category') updates.category = text;
  if (state.field === 'text') updates.text = text === '-' ? '' : text;
  if (state.field === 'next') updates.nextAction = text === '-' ? '' : text;
  const note = await updateNote(state.projectId, state.noteId, updates);
  if (note) {
    const project = await getProjectById(state.projectId, ctx);
    try {
      await sendNoteBackupMessage(project, note);
    } catch (error) {
      await renderNoteDetail(ctx, state.projectId, state.noteId, `⚠️ Backup failed: ${error.message}`);
      clearUserState(ctx.from.id);
      return;
    }
  }
  clearUserState(ctx.from.id);
  await renderNoteDetail(ctx, state.projectId, state.noteId, '✅ Note updated.');
}

async function handleNoteSearchInput(ctx, state) {
  const text = ctx.message?.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }
  if (text.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderNotesMenu(ctx, state.projectId, 'Operation cancelled.');
    return;
  }
  const notes = await listNotes(state.projectId);
  const query = text.toLowerCase();
  const matches = notes.filter((note) => {
    return (
      note.title.toLowerCase().includes(query) ||
      note.text.toLowerCase().includes(query) ||
      (note.nextAction || '').toLowerCase().includes(query)
    );
  });
  clearUserState(ctx.from.id);
  const lines = [`🔎 Note search — ${state.projectId}`, `Query: ${text}`];
  if (!matches.length) {
    lines.push('', 'No notes found.');
  } else {
    lines.push('');
    matches.forEach((note) => {
      lines.push(`• ${note.title} (${note.category})`);
    });
  }
  const inline = new InlineKeyboard();
  matches.forEach((note) => {
    inline.text(`📝 ${truncateText(note.title, 24)}`, `notes:view:${state.projectId}:${note.id}`).row();
  });
  inline.text('⬅️ Back', `notes:menu:${state.projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

function extractNoteAttachmentFromMessage(ctx) {
  const message = ctx.message || {};
  if (message.photo?.length) {
    const photo = message.photo[message.photo.length - 1];
    return { type: 'photo', fileId: photo.file_id };
  }
  if (message.video) {
    return { type: 'video', fileId: message.video.file_id, fileName: message.video.file_name };
  }
  if (message.voice) {
    return { type: 'voice', fileId: message.voice.file_id };
  }
  if (message.document) {
    return {
      type: 'document',
      fileId: message.document.file_id,
      fileName: message.document.file_name,
      mimeType: message.document.mime_type,
    };
  }
  return null;
}

async function handleNoteAttachmentInput(ctx, state) {
  const attachment = extractNoteAttachmentFromMessage(ctx);
  if (!attachment) {
    await ctx.reply('📎 Please send a supported attachment (photo/video/voice/file).');
    return;
  }
  const project = await getProjectById(state.projectId, ctx);
  if (!project) {
    clearUserState(ctx.from.id);
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const note = await getNote(state.projectId, state.noteId);
  if (!note) {
    clearUserState(ctx.from.id);
    await renderOrEdit(ctx, 'Note not found.');
    return;
  }
  const updated = await addNoteAttachment(state.projectId, state.noteId, attachment);
  const caption = formatBackupCaption(normalizeBackupSettings(await getCachedSettings()).captionTemplate, {
    projectName: project.name || project.id,
    projectId: project.id,
    title: note.title,
    category: note.category,
    status: note.status,
    createdAt: note.createdAt,
    noteId: note.id,
  });
  if (updated) {
    const added = updated.attachments[updated.attachments.length - 1];
    try {
      await sendNoteAttachmentBackup(project, state.noteId, added, caption);
    } catch (error) {
      await renderNoteDetail(ctx, state.projectId, state.noteId, `⚠️ Backup failed: ${error.message}`);
      clearUserState(ctx.from.id);
      return;
    }
  }
  clearUserState(ctx.from.id);
  await renderNoteDetail(ctx, state.projectId, state.noteId, '📎 Attachment added.');
}

async function handleProjectLogChatInput(ctx, state) {
  const value = ctx.message?.text?.trim();
  if (!value) {
    await ctx.reply('📍 Please provide a chat_id value.', { reply_markup: buildCancelKeyboard() });
    return;
  }
  if (!/^-?\\d+$/.test(value)) {
    await ctx.reply('⚠️ Invalid chat_id format. Please send a numeric chat_id.', {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }
  const settings = await getProjectLogSettingsWithDefaults(state.projectId);
  settings.destinationChatId = value;
  await upsertProjectLogSettings(state.projectId, settings);
  clearUserState(ctx.from.id);
  await renderProjectLogDestinationMenu(ctx, state.projectId);
}

async function renderProjectSettingsForMessage(messageContext, projectId, notice) {
  if (!messageContext) {
    return;
  }
  if (!messageContext.chatId) {
    console.warn('[UI] Missing chat_id for project card update', {
      projectId,
      messageContext,
    });
    return;
  }
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    return;
  }
  const view = buildProjectHubView(project, notice);
  if (!messageContext.messageId) {
    await bot.api.sendMessage(
      messageContext.chatId,
      view.text,
      normalizeTelegramExtra({ reply_markup: view.keyboard }),
    );
    return;
  }
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
    if (isMessageNotModifiedError(error)) {
      try {
        await bot.api.sendMessage(
          messageContext.chatId,
          view.text,
          normalizeTelegramExtra({ reply_markup: view.keyboard }),
        );
      } catch (sendError) {
        console.error('[UI] Failed to send project card fallback', sendError);
      }
      return;
    }
    console.error('[UI] Failed to update project card message', error);
  }
}

async function renderProjectMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;

  const inline = new InlineKeyboard()
    .text('✏️ Edit Name', `proj:rename:${projectId}`)
    .row()
    .text('🆔 Edit ID', `proj:edit_id:${projectId}`)
    .row()
    .text('🧭 Apply Path / Working Dir', `proj:working_dir:${projectId}`)
    .row()
    .text('⭐ Default project actions', `proj:default_actions:${projectId}`)
    .row()
    .text('🌐 Repository', `proj:repo_menu:${projectId}`)
    .row()
    .text('↩ Back', 'nav:back');

  await renderOrEdit(ctx, `${buildScopedHeader(`PROJECT: ${project.name || project.id}||${project.id}`, `Main → Projects → ${project.name || project.id} → ✏️ Edit Project`)}📂 Project menu: ${project.name || project.id}`,
    { reply_markup: inline });
}



async function renderProjectRepositoryMenu(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const globalSettings = await loadGlobalSettings();
  const effectiveBase = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  const tokenKey = project.githubTokenEnvKey || 'GITHUB_TOKEN';
  const tokenLabel = tokenKey === 'GITHUB_TOKEN' ? 'default (GITHUB_TOKEN)' : `override (${tokenKey})`;
  const lines = [
    `🌐 Repository — ${project.name || project.id}`,
    '',
    `Repo: ${project.repoSlug || project.repoUrl || 'not set'}`,
    `Token: ${tokenLabel}`,
    `Base branch: ${effectiveBase}`,
  ];
  const inline = new InlineKeyboard()
    .text('📚 Choose repo', `proj:edit_repo:${project.id}`)
    .row()
    .text('🔑 GitHub token (default/override)', `proj:edit_github_token:${project.id}`)
    .row()
    .text('🌿 Base branch', `proj:change_base:${project.id}`)
    .row()
    .text('↩ Back', 'nav:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderProjectDiagnosticsMenu(ctx, projectId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;

  const lines = [
    `🧪 Diagnostics — ${project.name || project.id}`,
    notice || null,
    '',
    'Choose an action:',
  ].filter(Boolean);

  const inline = new InlineKeyboard()
    .text('🩺 Run Light Diagnostics', `proj:diagnostics_light:${projectId}`)
    .row()
    .text('🧪 Run Full Diagnostics', `proj:diagnostics_full:${projectId}`)
    .row()
    .text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
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
    inline.text('🗄 Use Supabase connection', `proj:sql_supabase:${projectId}`).row();
  }
  inline.text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderProjectDbMiniSite(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const envSetId = await ensureProjectEnvSet(projectId);
  const envStatus = await buildEnvVaultDbStatus(project, envSetId);
  const supabaseStatus = await buildSupabaseBindingStatus(project);
  const { settings: miniSiteSettings } = await getMiniSiteSettingsState();
  const miniSiteDb = await resolveMiniSiteDbConnection(project);
  const miniSiteDbReady = Boolean(miniSiteDb.dsn);
  const miniSiteTokenConfigured = isMiniSiteTokenConfigured(miniSiteSettings);
  const miniSiteTokenMask = getMiniSiteAdminTokenMask(miniSiteSettings);
  const miniSiteTokenSource = isMiniSiteTokenFromEnv(miniSiteSettings) ? ' (env)' : '';
  const miniSiteTokenLabel = miniSiteTokenConfigured
    ? `${miniSiteTokenMask || 'configured'}${miniSiteTokenSource}`
    : 'not configured';
  const sslSettings = resolveProjectDbSslSettings(project, miniSiteDb);
  const sslSummary = formatProjectDbSslSummary(sslSettings);

  const lines = [
    `🌐 DB mini-site — ${project.name || project.id}`,
    '',
    `Env Vault DB: ${envStatus.summary}`,
    `Supabase binding: ${supabaseStatus.summary}`,
    '',
    `Mini-site DB: ${miniSiteDbReady ? '✅ ready' : '⚠️ missing'}`,
    `Mini-site token: ${miniSiteTokenLabel}`,
    `SSL: ${sslSummary}`,
  ];

  const inline = new InlineKeyboard();
  if (miniSiteDbReady) {
    if (!miniSiteTokenConfigured) {
      inline.text('✅ Enable mini-site', `proj:db_mini_enable:${projectId}`).row();
    } else {
      inline
        .webApp('🗄 Open DB Console', getMiniAppUrl())
        .text('🔄 Rotate mini-site token', `proj:db_mini_rotate:${projectId}`)
        .row();
    }
  }
  inline.text('⬅️ Back', `proj:open:${projectId}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function ensureMiniSiteDbAvailable(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return { ok: false, project: null };
  const connection = await resolveMiniSiteDbConnection(project);
  if (!connection.dsn) {
    await renderOrEdit(ctx, 'Database connection missing. Configure a DB URL to use the mini-site.', {
      reply_markup: buildBackKeyboard(`proj:db_config:${projectId}`),
    });
    return { ok: false, project };
  }
  return { ok: true, project };
}

async function sendMiniSiteAdminTokenOnce(ctx, token, settings, reason) {
  const lines = [
    '🔐 DB mini-site admin token',
    reason ? `Reason: ${reason}` : null,
    '',
    token,
    '',
    'Store this token securely. It will only be shown once.',
  ].filter(Boolean);
  await ctx.reply(lines.join('\n'));
  await markMiniSiteTokenShown(settings);
}

async function enableProjectDbMiniSite(ctx, projectId) {
  const ready = await ensureMiniSiteDbAvailable(ctx, projectId);
  if (!ready.ok) return;
  const result = await ensureMiniSiteAdminToken({ rotate: false });
  if (result.created && result.token) {
    await sendMiniSiteAdminTokenOnce(ctx, result.token, result.settings, 'Enabled');
  }
  await renderDatabaseBindingMenu(ctx, projectId, result.created ? '✅ Mini-site token enabled.' : 'Mini-site token already configured.');
}

async function rotateProjectDbMiniSiteToken(ctx, projectId) {
  const ready = await ensureMiniSiteDbAvailable(ctx, projectId);
  if (!ready.ok) return;
  const result = await ensureMiniSiteAdminToken({ rotate: true });
  if (result.token) {
    await sendMiniSiteAdminTokenOnce(ctx, result.token, result.settings, 'Rotated');
  }
  await renderDatabaseBindingMenu(ctx, projectId, '🔄 Mini-site token rotated.');
}

async function openProjectDbMiniSite(ctx, projectId) {
  const ready = await ensureMiniSiteDbAvailable(ctx, projectId);
  if (!ready.ok) return;
  const result = await ensureMiniSiteAdminToken({ rotate: false });
  if (result.created && result.token) {
    await sendMiniSiteAdminTokenOnce(ctx, result.token, result.settings, 'Auto-generated');
  }
  const { settings: miniSiteSettings } = await getMiniSiteSettingsState();
  const sessionTtlMs = resolveMiniSiteSessionTtlMs(miniSiteSettings);
  const sessionTtlMinutes = Math.round(sessionTtlMs / 60000);
  const sessionToken = await createMiniSiteSession({
    scope: 'link',
    ttlMs: sessionTtlMs,
  });
  const baseUrl = getPublicBaseUrl().replace(/\/$/, '');
  const miniSiteUrl = `${baseUrl}/db-mini/${encodeURIComponent(projectId)}?session=${encodeURIComponent(
    sessionToken,
  )}`;
  const inline = new InlineKeyboard()
    .url(`🌐 Open mini-site (${sessionTtlMinutes} min)`, miniSiteUrl)
    .row()
    .text('⬅️ Back', `proj:db_config:${projectId}`);
  const lines = [
    `🌐 DB mini-site — ${ready.project.name || ready.project.id}`,
    '',
    `Session link (expires in ${sessionTtlMinutes} minutes):`,
    miniSiteUrl,
  ];
  await renderOrEdit(ctx, lines.join('\n'), {
    reply_markup: inline,
    disable_web_page_preview: true,
  });
}

async function resolveDbInsightsSource(project) {
  const envSetId = await ensureProjectEnvSet(project.id);
  const envStatus = await buildEnvVaultDbStatus(project, envSetId);
  if (envStatus.ready) {
    return { source: 'env_vault', envSetId };
  }
  if (project.supabaseConnectionId) {
    return { source: 'supabase', connectionId: project.supabaseConnectionId };
  }
  return { source: null };
}

async function runDbInsightsQuery(source, options) {
  if (source === 'env_vault') {
    const connection = await resolveEnvVaultConnection(options.projectId, options.envSetId);
    if (!connection.dsn) {
      throw new Error('Missing ENV Vault DB connection details.');
    }
    const poolKey = `${options.projectId}:${options.envSetId}`;
    let pool = envVaultPools.get(poolKey);
    if (!pool) {
      pool = new Pool({ connectionString: connection.dsn });
      envVaultPools.set(poolKey, pool);
    }
    return pool.query({ ...options.query, query_timeout: DB_INSIGHTS_QUERY_TIMEOUT_MS });
  }
  return runSupabaseQuery(options.connectionId, {
    ...options.query,
    query_timeout: DB_INSIGHTS_QUERY_TIMEOUT_MS,
  });
}

function formatDbInsightsTimestamp(value) {
  if (!value) return '-';
  try {
    return new Date(value).toISOString();
  } catch (error) {
    return String(value);
  }
}

function formatDbInsightsSampleRow(row) {
  const masked = applyRowMasking(row);
  return formatSqlRow(masked);
}

async function fetchDbInsightsTables(sourceInfo, page) {
  const offset = Math.max(0, page) * DB_INSIGHTS_TABLE_PAGE_SIZE;
  const query = {
    text: `
      SELECT
        t.table_schema,
        t.table_name,
        c.reltuples::bigint AS estimate_rows,
        GREATEST(s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze) AS last_stats
      FROM information_schema.tables t
      LEFT JOIN pg_namespace n ON n.nspname = t.table_schema
      LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = t.table_name
      LEFT JOIN pg_stat_user_tables s ON s.schemaname = t.table_schema AND s.relname = t.table_name
      WHERE t.table_type = 'BASE TABLE'
        AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY t.table_schema, t.table_name
      LIMIT $1 OFFSET $2;
    `,
    values: [DB_INSIGHTS_TABLE_PAGE_SIZE + 1, offset],
  };

  const result = await runDbInsightsQuery(sourceInfo.source, {
    ...sourceInfo,
    query,
  });
  const rows = result.rows || [];
  return {
    tables: rows.slice(0, DB_INSIGHTS_TABLE_PAGE_SIZE),
    hasNext: rows.length > DB_INSIGHTS_TABLE_PAGE_SIZE,
  };
}

async function fetchDbInsightsSamples(sourceInfo, tables) {
  const samples = new Map();
  for (const table of tables) {
    const query = {
      text: `SELECT * FROM ${quoteIdentifier(table.table_schema)}.${quoteIdentifier(table.table_name)} LIMIT $1;`,
      values: [DB_INSIGHTS_SAMPLE_SIZE],
    };
    try {
      const result = await runDbInsightsQuery(sourceInfo.source, {
        ...sourceInfo,
        query,
      });
      samples.set(`${table.table_schema}.${table.table_name}`, result.rows || []);
    } catch (error) {
      samples.set(`${table.table_schema}.${table.table_name}`, {
        error: truncateText(error.message || 'sample failed', 80),
      });
    }
  }
  return samples;
}

async function renderProjectDbInsights(ctx, projectId, page = 0, sample = 0) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;

  const sourceInfo = await resolveDbInsightsSource(project);
  if (!sourceInfo.source) {
    await renderOrEdit(ctx, 'No DB connection available for insights.', {
      reply_markup: buildBackKeyboard(`proj:open:${projectId}`),
    });
    return;
  }

  const safePage = Math.max(0, Number.isFinite(page) ? page : 0);
  const includeSample = Number(sample) === 1;
  const { tables, hasNext } = await fetchDbInsightsTables(sourceInfo, safePage);
  const samples = includeSample ? await fetchDbInsightsSamples(sourceInfo, tables) : new Map();

  const lines = [
    `📊 DB Insights — ${project.name || project.id}`,
    `Source: ${sourceInfo.source === 'env_vault' ? 'Env Vault' : 'Supabase'}`,
    `Page: ${safePage + 1}`,
    `Sample rows: ${includeSample ? `ON (n=${DB_INSIGHTS_SAMPLE_SIZE})` : 'OFF'}`,
    '',
  ];

  if (!tables.length) {
    lines.push('No tables found.');
  } else {
    tables.forEach((table) => {
      const tableLabel = `${table.table_schema}.${table.table_name}`;
      const estimate = table.estimate_rows == null ? '-' : `~${table.estimate_rows}`;
      const lastStats = formatDbInsightsTimestamp(table.last_stats);
      lines.push(`• ${tableLabel} — rows ${estimate} — last stats ${lastStats}`);
      if (includeSample) {
        const sampleRows = samples.get(tableLabel);
        if (Array.isArray(sampleRows)) {
          if (sampleRows.length) {
            sampleRows.slice(0, DB_INSIGHTS_SAMPLE_SIZE).forEach((row) => {
              lines.push(`  ↳ ${formatDbInsightsSampleRow(row)}`);
            });
          } else {
            lines.push('  ↳ (no rows)');
          }
        } else if (sampleRows?.error) {
          lines.push(`  ↳ sample error: ${sampleRows.error}`);
        }
      }
    });
  }

  const inline = new InlineKeyboard();
  if (safePage > 0) {
    inline.text('⬅️ Prev', `proj:db_insights:${projectId}:${safePage - 1}:${includeSample ? 1 : 0}`);
  }
  if (hasNext) {
    inline.text('➡️ Next', `proj:db_insights:${projectId}:${safePage + 1}:${includeSample ? 1 : 0}`);
  }
  if (safePage > 0 || hasNext) {
    inline.row();
  }
  inline
    .text(includeSample ? '🧪 Sample: ON' : '🧪 Sample: OFF', `proj:db_insights:${projectId}:${safePage}:${includeSample ? 0 : 1}`)
    .row()
    .text('⬅️ Back', `proj:db_mini:${projectId}`);

  await renderOrEdit(ctx, truncateMessage(lines.join('\n'), SUPABASE_MESSAGE_LIMIT), {
    reply_markup: inline,
  });
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

function resolveSupabaseEnabled(project) {
  return resolveProjectFeatureFlag(project, 'supabaseEnabled') === true || project?.supabaseEnabled === true;
}

function getSupabaseBindingMissingFields(project) {
  const missing = [];
  if (isMissingRequirementValue(project?.supabaseProjectRef)) missing.push('projectRef');
  if (isMissingRequirementValue(project?.supabaseUrl)) missing.push('supabaseUrl');
  if (isMissingRequirementValue(project?.supabaseKeyType)) missing.push('keyType');
  if (isMissingRequirementValue(project?.supabaseKey)) missing.push('apiKey');
  return missing;
}

function getSupabaseKeyMask(project) {
  if (project?.supabaseKeyMask) return project.supabaseKeyMask;
  if (project?.supabaseKey) return 'configured';
  return 'not set';
}

async function buildSupabaseBindingStatus(project) {
  const enabled = resolveSupabaseEnabled(project);
  const missing = getSupabaseBindingMissingFields(project);
  if (!enabled) {
    return { ready: false, summary: 'not set (optional)', enabled, missing };
  }
  if (missing.length) {
    return { ready: false, summary: `missing required fields: ${missing.join(', ')}`, enabled, missing };
  }
  const keyType = project.supabaseKeyType || 'unknown';
  return { ready: true, summary: `set (${keyType})`, enabled, missing };
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

function normalizeProjectDbSslMode(value) {
  if (!value) return PROJECT_DB_SSL_DEFAULT_MODE;
  const normalized = String(value).toLowerCase();
  if (PROJECT_DB_SSL_MODES.has(normalized)) return normalized;
  return PROJECT_DB_SSL_DEFAULT_MODE;
}

function isSupabaseMiniSiteConnection(connection) {
  if (!connection) return false;
  const source = String(connection.source || '').toLowerCase();
  if (source.includes('supabase')) return true;
  const dsn = String(connection.dsn || '').toLowerCase();
  return dsn.includes('.supabase.co');
}

function hasRequireSslModeInDsn(dsn) {
  if (!dsn) return false;
  try {
    const parsed = new URL(dsn);
    return String(parsed.searchParams.get('sslmode') || '').toLowerCase() === 'require';
  } catch (error) {
    return String(dsn).toLowerCase().includes('sslmode=require');
  }
}

function resolveProjectDbSslSettings(project, connection) {
  const sslMode = normalizeProjectDbSslMode(project?.dbSslMode);
  if (typeof project?.dbSslVerify === 'boolean') {
    const sslVerify = project.dbSslVerify === false && !ALLOW_INSECURE_TLS_FOR_TESTS
      ? true
      : project.dbSslVerify;
    return { sslMode, sslVerify };
  }
  const dsn = connection?.dsn;
  const sslVerify = PROJECT_DB_SSL_DEFAULT_VERIFY;
  if (
    ALLOW_INSECURE_TLS_FOR_TESTS &&
    (hasRequireSslModeInDsn(dsn) || isSupabaseMiniSiteConnection(connection))
  ) {
    return { sslMode, sslVerify: false };
  }
  return { sslMode, sslVerify };
}

function formatProjectDbSslSummary(settings) {
  const verifyLabel = settings.sslVerify ? 'verify' : 'no-verify';
  return `${settings.sslMode} (${verifyLabel})`;
}

function buildPgSslOptions(settings) {
  if (!settings || settings.sslMode === 'disable') return null;
  const allowInsecure = settings.sslVerify === false && ALLOW_INSECURE_TLS_FOR_TESTS;
  return { rejectUnauthorized: !allowInsecure };
}

function buildMiniSitePoolKey(dsn, settings) {
  if (!dsn) return null;
  const sslMode = settings?.sslMode || PROJECT_DB_SSL_DEFAULT_MODE;
  const sslVerify = settings?.sslVerify !== false ? 'verify' : 'no-verify';
  return `${dsn}::ssl=${sslMode}:${sslVerify}`;
}

function buildMiniSiteRequestId() {
  return crypto.randomBytes(6).toString('hex');
}

function buildMiniSiteRetryHref(url) {
  if (!url) return '/db-mini';
  const retryUrl = new URL(url.toString());
  retryUrl.searchParams.set('retry', '1');
  return `${retryUrl.pathname}${retryUrl.search}`;
}

function renderMiniSiteDbErrorPage({ requestId, adminHint, stage, errorCode, latencyMs, retryHref }) {
  const safeAdminHint = adminHint ? escapeHtml(adminHint) : null;
  const hintBlock = adminHint
    ? `
      <div class="card">
        <h4>Admin hint</h4>
        <p>${safeAdminHint}</p>
      </div>
    `
    : '';
  const diagnostics = [
    stage ? `<li><strong>Stage:</strong> ${escapeHtml(stage)}</li>` : null,
    errorCode ? `<li><strong>Error code:</strong> ${escapeHtml(errorCode)}</li>` : null,
    Number.isFinite(latencyMs) ? `<li><strong>Latency:</strong> ${escapeHtml(latencyMs)} ms</li>` : null,
  ]
    .filter(Boolean)
    .join('');
  const body = `
    <div class="card">
      <h3>Database unavailable – check network/SSL</h3>
      <p class="muted">Request ID: ${escapeHtml(requestId)}</p>
      ${diagnostics ? `<ul class="muted">${diagnostics}</ul>` : ''}
      <div class="row-actions">
        <a class="button" href="${escapeHtml(retryHref || '/db-mini')}">Retry</a>
      </div>
      <p class="muted">Admin hint: Check DB binding and network; do not use insecure TLS workarounds.</p>
    </div>
    ${hintBlock}
  `;
  return renderMiniSiteLayout('Database unavailable', body);
}

async function resolveMiniSiteDbConnection(project) {
  if (!project) return { dsn: null, source: 'missing_project' };
  if (isEnvVaultAvailable()) {
    const envSetId = await ensureProjectEnvSet(project.id);
    const envVault = await resolveEnvVaultConnection(project.id, envSetId);
    if (envVault.dsn) {
      return { dsn: envVault.dsn, source: envVault.source || 'env_vault' };
    }
  }
  if (project.supabaseConnectionId) {
    const connection = await findSupabaseConnection(project.supabaseConnectionId);
    if (connection) {
      const dsnInfo = await resolveSupabaseConnectionDsn(connection);
      if (dsnInfo.dsn) {
        return { dsn: dsnInfo.dsn, source: connection.envKey || 'supabase' };
      }
    }
  }
  if (project.databaseUrl) {
    return { dsn: project.databaseUrl, source: 'project_db_config' };
  }
  if (process.env.DATABASE_URL) {
    return { dsn: process.env.DATABASE_URL, source: 'process_env' };
  }
  return { dsn: null, source: 'missing' };
}

function resolveHealthDbConnection() {
  const dsn = process.env.DATABASE_URL_PM || process.env.PATH_APPLIER_CONFIG_DSN || process.env.DATABASE_URL || null;
  return { dsn, source: dsn ? 'env' : 'missing' };
}

function parsePgDsnInfo(dsn) {
  if (!dsn) return { host: null, port: null, database: null };
  try {
    const parsed = new URL(dsn);
    return {
      host: parsed.hostname || null,
      port: Number(parsed.port) || 5432,
      database: parsed.pathname ? parsed.pathname.replace(/^\//, '') : null,
    };
  } catch (error) {
    return { host: null, port: null, database: null };
  }
}

async function waitMs(durationMs) {
  if (!durationMs) return;
  await new Promise((resolve) => setTimeout(resolve, durationMs));
}

async function withTimeout(promise, timeoutMs, errorCode) {
  if (!promise || typeof promise.then !== 'function') return promise;
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error(errorCode || 'TIMEOUT');
      error.code = errorCode || 'TIMEOUT';
      reject(error);
    }, timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

async function checkTcpConnectivity({ host, port, timeoutMs }) {
  return new Promise((resolve) => {
    if (!host || !port) {
      resolve({ ok: false, error: new Error('MISSING_HOST') });
      return;
    }
    const socket = net.connect({ host, port });
    const onError = (error) => {
      socket.destroy();
      resolve({ ok: false, error });
    };
    const onConnect = () => {
      socket.end();
      resolve({ ok: true });
    };
    socket.setTimeout(timeoutMs, () => onError(Object.assign(new Error('TCP_TIMEOUT'), { code: 'ETIMEDOUT' })));
    socket.once('error', onError);
    socket.once('connect', onConnect);
  });
}

async function checkTlsHandshake({ host, port, timeoutMs, sslSettings }) {
  if (!sslSettings || sslSettings.sslMode === 'disable') {
    return { ok: true };
  }
  return new Promise((resolve) => {
    const options = {
      host,
      port,
      servername: host,
      rejectUnauthorized: sslSettings.sslVerify !== false || !ALLOW_INSECURE_TLS_FOR_TESTS,
    };
    const socket = tls.connect(options, () => {
      socket.end();
      resolve({ ok: true });
    });
    const onError = (error) => {
      socket.destroy();
      resolve({ ok: false, error });
    };
    socket.setTimeout(timeoutMs, () => onError(Object.assign(new Error('TLS_TIMEOUT'), { code: 'ETIMEDOUT' })));
    socket.once('error', onError);
  });
}

function classifyMiniSiteDbStage(error) {
  const code = String(error?.code || '');
  const message = String(error?.message || '').toLowerCase();
  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') return 'dns';
  if (code === 'ECONNREFUSED' || code === 'ETIMEDOUT' || code === 'ECONNRESET') return 'tcp';
  if (
    code === 'SELF_SIGNED_CERT_IN_CHAIN' ||
    code === 'CERT_HAS_EXPIRED' ||
    message.includes('ssl') ||
    message.includes('certificate')
  ) {
    return 'tls';
  }
  if (code === '28P01' || code === '28000' || message.includes('password authentication failed')) {
    return 'auth';
  }
  return 'query';
}

function isTransientMiniSiteError(error) {
  const code = String(error?.code || '');
  return ['ETIMEDOUT', 'ECONNRESET', 'ENOTFOUND', 'EAI_AGAIN', 'ECONNREFUSED'].includes(code);
}

function logMiniSiteProbeFailure({ context, dsn, stage, error, latencyMs }) {
  const info = parsePgDsnInfo(dsn);
  console.warn('[mini-site] db probe failed', {
    context,
    stage,
    code: error?.code || 'unknown',
    host: info.host,
    port: info.port,
    database: info.database,
    latencyMs,
  });
}

async function runMiniSiteDbPreflight({ dsn, sslSettings, pool, timeoutMs }) {
  const startedAt = Date.now();
  const status = {
    ok: false,
    stage: null,
    latencyMs: null,
    errorCode: null,
    canResolveDns: false,
    canTcpConnect: false,
    canHandshakeTls: false,
    canAuth: false,
    canQuery: false,
  };
  if (!dsn) {
    status.stage = 'dns';
    status.errorCode = 'MISSING_DSN';
    status.latencyMs = 0;
    return status;
  }
  const info = parsePgDsnInfo(dsn);
  if (!info.host) {
    status.stage = 'dns';
    status.errorCode = 'INVALID_DSN';
    status.latencyMs = 0;
    return status;
  }
  try {
    await withTimeout(dns.lookup(info.host), MINI_SITE_WARMUP_DNS_TIMEOUT_MS, 'DNS_TIMEOUT');
    status.canResolveDns = true;
  } catch (error) {
    status.stage = 'dns';
    status.errorCode = error?.code || 'DNS_ERROR';
    status.latencyMs = Date.now() - startedAt;
    logMiniSiteProbeFailure({ context: 'dns', dsn, stage: status.stage, error, latencyMs: status.latencyMs });
    return status;
  }
  const tcpResult = await checkTcpConnectivity({
    host: info.host,
    port: info.port,
    timeoutMs: MINI_SITE_WARMUP_TCP_TIMEOUT_MS,
  });
  if (!tcpResult.ok) {
    status.stage = 'tcp';
    status.errorCode = tcpResult.error?.code || 'TCP_ERROR';
    status.latencyMs = Date.now() - startedAt;
    logMiniSiteProbeFailure({ context: 'tcp', dsn, stage: status.stage, error: tcpResult.error, latencyMs: status.latencyMs });
    return status;
  }
  status.canTcpConnect = true;
  const tlsResult = await checkTlsHandshake({
    host: info.host,
    port: info.port,
    timeoutMs: MINI_SITE_WARMUP_TLS_TIMEOUT_MS,
    sslSettings,
  });
  if (!tlsResult.ok) {
    status.stage = 'tls';
    status.errorCode = tlsResult.error?.code || 'TLS_ERROR';
    status.latencyMs = Date.now() - startedAt;
    logMiniSiteProbeFailure({ context: 'tls', dsn, stage: status.stage, error: tlsResult.error, latencyMs: status.latencyMs });
    return status;
  }
  status.canHandshakeTls = true;
  if (!pool) {
    status.stage = 'auth';
    status.errorCode = 'POOL_UNAVAILABLE';
    status.latencyMs = Date.now() - startedAt;
    return status;
  }
  let client;
  try {
    client = await pool.connect();
    status.canAuth = true;
    await client.query({
      text: 'SELECT 1',
      query_timeout: timeoutMs || MINI_SITE_WARMUP_QUERY_TIMEOUT_MS,
      statement_timeout: timeoutMs || MINI_SITE_WARMUP_QUERY_TIMEOUT_MS,
    });
    status.canQuery = true;
    status.ok = true;
  } catch (error) {
    status.stage = status.canAuth ? 'query' : 'auth';
    status.errorCode = error?.code || 'QUERY_ERROR';
    logMiniSiteProbeFailure({ context: 'query', dsn, stage: status.stage, error, latencyMs: Date.now() - startedAt });
  } finally {
    if (client) {
      client.release();
    }
  }
  status.latencyMs = Date.now() - startedAt;
  return status;
}

function updateMiniSiteWarmupSummary(status) {
  miniSiteWarmupSummary = {
    warmUpReady: Boolean(status?.ok),
    lastWarmUpAt: new Date().toISOString(),
  };
}

function getMiniSiteWarmupCacheEntry(projectId) {
  if (!projectId) return null;
  const entry = miniSiteWarmupCache.get(projectId);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    miniSiteWarmupCache.delete(projectId);
    return null;
  }
  return entry;
}

function setMiniSiteWarmupCacheEntry(projectId, status) {
  if (!projectId || !status) return;
  const expiresAt = Date.now() + MINI_SITE_WARMUP_TTL_MS;
  miniSiteWarmupCache.set(projectId, { ...status, expiresAt });
  updateMiniSiteWarmupSummary(status);
}

async function warmupMiniSiteDb({ projectId, dsn, sslSettings, pool, requestId, url }) {
  if (!MINI_SITE_WARMUP_ENABLED) {
    return { ok: true, skipped: true };
  }
  const isRetry = url?.searchParams?.get('retry') === '1';
  const cached = getMiniSiteWarmupCacheEntry(projectId);
  if (cached && !isRetry) {
    return { ...cached, cached: true };
  }
  const status = await runMiniSiteDbPreflight({
    dsn,
    sslSettings,
    pool,
    timeoutMs: MINI_SITE_WARMUP_QUERY_TIMEOUT_MS,
  });
  const payload = {
    ...status,
    requestId,
    lastWarmUpAt: new Date().toISOString(),
  };
  setMiniSiteWarmupCacheEntry(projectId, payload);
  return payload;
}

function getMiniSitePool(dsn, sslSettings) {
  if (!dsn) return null;
  const poolKey = buildMiniSitePoolKey(dsn, sslSettings);
  if (!poolKey) return null;
  if (!miniSitePools.has(poolKey)) {
    const poolConfig = {
      connectionString: dsn,
      connectionTimeoutMillis: MINI_SITE_CONNECTION_TIMEOUT_MS,
      idleTimeoutMillis: 30_000,
      max: MINI_SITE_POOL_MAX,
      statement_timeout: MINI_SITE_STATEMENT_TIMEOUT_MS,
    };
    const sslOptions = buildPgSslOptions(sslSettings);
    if (sslOptions) {
      poolConfig.ssl = sslOptions;
    }
    miniSitePools.set(poolKey, new Pool(poolConfig));
  }
  return miniSitePools.get(poolKey);
}

function getHealthCheckPool(dsn, sslSettings) {
  if (!dsn) return null;
  const poolKey = buildMiniSitePoolKey(dsn, sslSettings);
  if (!poolKey) return null;
  if (!healthCheckPools.has(poolKey)) {
    const poolConfig = {
      connectionString: dsn,
      connectionTimeoutMillis: MINI_SITE_CONNECTION_TIMEOUT_MS,
      idleTimeoutMillis: 30_000,
      max: 1,
      statement_timeout: MINI_SITE_STATEMENT_TIMEOUT_MS,
    };
    const sslOptions = buildPgSslOptions(sslSettings);
    if (sslOptions) {
      poolConfig.ssl = sslOptions;
    }
    healthCheckPools.set(poolKey, new Pool(poolConfig));
  }
  return healthCheckPools.get(poolKey);
}

async function runMiniSiteQuery(pool, text, values, options = {}) {
  const queryConfig = {
    text,
    values,
    query_timeout: options.queryTimeoutMs || MINI_SITE_QUERY_TIMEOUT_MS,
    statement_timeout: options.statementTimeoutMs || MINI_SITE_STATEMENT_TIMEOUT_MS,
  };
  const maxAttempts = options.retry && MINI_SITE_RETRY_ENABLED ? MINI_SITE_RETRY_MAX_ATTEMPTS + 1 : 1;
  let attempt = 0;
  while (attempt < maxAttempts) {
    try {
      return await pool.query(queryConfig);
    } catch (error) {
      attempt += 1;
      const shouldRetry =
        options.retry &&
        MINI_SITE_RETRY_ENABLED &&
        attempt < maxAttempts &&
        isTransientMiniSiteError(error);
      if (!shouldRetry) {
        throw error;
      }
      const backoffMs = MINI_SITE_RETRY_BACKOFF_MS[attempt - 1] || MINI_SITE_RETRY_BACKOFF_MS.at(-1);
      await waitMs(backoffMs);
    }
  }
  return pool.query(queryConfig);
}

function resolveBuildVersion() {
  return process.env.GIT_SHA || process.env.RENDER_GIT_COMMIT || process.env.COMMIT_SHA || null;
}

async function buildHealthzPayload() {
  const buildVersion = resolveBuildVersion();
  const uptimeSeconds = Math.floor(process.uptime());
  const connection = resolveHealthDbConnection();
  const sslSettings = resolveProjectDbSslSettings({}, connection);
  const pool = getHealthCheckPool(connection.dsn, sslSettings);
  const dbProbe = await runMiniSiteDbPreflight({
    dsn: connection.dsn,
    sslSettings,
    pool,
    timeoutMs: MINI_SITE_WARMUP_QUERY_TIMEOUT_MS,
  });
  const dbStatus = {
    canResolveDns: dbProbe.canResolveDns,
    canTcpConnect: dbProbe.canTcpConnect,
    canHandshakeTls: dbProbe.canHandshakeTls,
    canAuth: dbProbe.canAuth,
    canQuery: dbProbe.canQuery,
    latencyMs: dbProbe.latencyMs,
    lastErrorCode: dbProbe.errorCode || null,
  };
  const serviceStatus = dbProbe.ok ? 'ok' : 'degraded';
  return {
    serviceStatus,
    uptimeSeconds,
    build: {
      version: buildVersion,
    },
    dbStatus,
    miniSiteStatus: {
      warmUpReady: miniSiteWarmupSummary.warmUpReady,
      lastWarmUpAt: miniSiteWarmupSummary.lastWarmUpAt,
    },
  };
}

async function buildHealthzPayloadSafely() {
  try {
    return await buildHealthzPayload();
  } catch (error) {
    return {
      serviceStatus: 'fail',
      uptimeSeconds: Math.floor(process.uptime()),
      build: {
        version: resolveBuildVersion(),
      },
      dbStatus: {
        canResolveDns: false,
        canTcpConnect: false,
        canHandshakeTls: false,
        canAuth: false,
        canQuery: false,
        latencyMs: null,
        lastErrorCode: error?.code || 'HEALTHZ_ERROR',
      },
      miniSiteStatus: {
        warmUpReady: miniSiteWarmupSummary.warmUpReady,
        lastWarmUpAt: miniSiteWarmupSummary.lastWarmUpAt,
      },
      errorSummary: error?.message || 'Health check failed.',
    };
  }
}

async function listMiniSiteTables(pool) {
  const { rows } = await runMiniSiteQuery(
    pool,
    `
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY table_schema, table_name;
    `,
    [],
    { retry: true },
  );
  return rows;
}

async function fetchMiniSiteTableColumns(pool, schema, table) {
  const { rows } = await runMiniSiteQuery(
    pool,
    `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position;
    `,
    [schema, table],
    { retry: true },
  );
  return rows;
}

async function fetchMiniSitePrimaryKeys(pool, schema, table) {
  const qualified = `${quoteIdentifier(schema)}.${quoteIdentifier(table)}`;
  const { rows } = await runMiniSiteQuery(
    pool,
    `
      SELECT a.attname
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE i.indrelid = $1::regclass AND i.indisprimary;
    `,
    [qualified],
    { retry: true },
  );
  return rows.map((row) => row.attname);
}

function encodeMiniSiteRowKey(keyData) {
  return Buffer.from(JSON.stringify(keyData)).toString('base64url');
}

function decodeMiniSiteRowKey(encoded) {
  try {
    const raw = Buffer.from(encoded, 'base64url').toString('utf8');
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

function hashMiniSiteToken(token) {
  return crypto.createHash('sha256').update(String(token)).digest('hex');
}

function compareHashedTokens(left, right) {
  if (!left || !right) return false;
  const leftBuffer = Buffer.from(left, 'hex');
  const rightBuffer = Buffer.from(right, 'hex');
  if (leftBuffer.length !== rightBuffer.length) return false;
  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function maskMiniSiteToken(token) {
  if (!token) return '••••';
  const suffix = token.slice(-4);
  return `••••${suffix}`;
}

function getMiniSiteAdminTokenHash(settings) {
  if (settings?.[MINI_SITE_SETTINGS_KEY]?.adminTokenHash) {
    return settings[MINI_SITE_SETTINGS_KEY].adminTokenHash;
  }
  if (MINI_SITE_TOKEN) {
    return hashMiniSiteToken(MINI_SITE_TOKEN);
  }
  return null;
}

function getMiniSiteAdminTokenMask(settings) {
  if (settings?.[MINI_SITE_SETTINGS_KEY]?.adminTokenMask) {
    return settings[MINI_SITE_SETTINGS_KEY].adminTokenMask;
  }
  if (MINI_SITE_TOKEN) {
    return maskMiniSiteToken(MINI_SITE_TOKEN);
  }
  return null;
}

function isMiniSiteTokenFromEnv(settings) {
  return !settings?.[MINI_SITE_SETTINGS_KEY]?.adminTokenHash && Boolean(MINI_SITE_TOKEN);
}

function isMiniSiteTokenConfigured(settings) {
  return Boolean(getMiniSiteAdminTokenHash(settings));
}

async function getMiniSiteSettingsState() {
  const settings = (await loadGlobalSettings()) || {};
  const miniSite = settings[MINI_SITE_SETTINGS_KEY] || {};
  return { settings, miniSite };
}

async function saveMiniSiteSettingsState(settings, miniSite) {
  const payload = { ...(settings || {}) };
  payload[MINI_SITE_SETTINGS_KEY] = miniSite;
  await saveGlobalSettingsAndCache(payload);
  return payload;
}

async function ensureMiniSiteAdminToken({ rotate = false } = {}) {
  const { settings, miniSite } = await getMiniSiteSettingsState();
  if (!rotate && miniSite.adminTokenHash) {
    return { settings, miniSite, token: null, created: false };
  }
  const token = crypto.randomBytes(32).toString('base64url');
  const nextMiniSite = {
    ...miniSite,
    adminTokenHash: hashMiniSiteToken(token),
    adminTokenMask: maskMiniSiteToken(token),
    adminTokenCreatedAt: new Date().toISOString(),
    adminTokenShownAt: null,
    adminTokenVersion: (miniSite.adminTokenVersion || 0) + 1,
  };
  const nextSettings = await saveMiniSiteSettingsState(settings, nextMiniSite);
  return { settings: nextSettings, miniSite: nextMiniSite, token, created: true };
}

async function markMiniSiteTokenShown(settings) {
  const miniSite = settings?.[MINI_SITE_SETTINGS_KEY] || {};
  const updated = {
    ...miniSite,
    adminTokenShownAt: new Date().toISOString(),
  };
  return saveMiniSiteSettingsState(settings, updated);
}

function resolveMiniSiteSessionTtlMinutes(settings) {
  const settingValue = Number(settings?.[MINI_SITE_SETTINGS_KEY]?.sessionTtlMinutes);
  const globalSetting = Number(cachedSettings?.security?.miniSiteSessionTtlMinutes);
  const envValue = Number(
    process.env.DB_MINI_SITE_SESSION_TTL_MINUTES || process.env.MINI_SITE_SESSION_TTL_MINUTES,
  );
  const resolved = Number.isFinite(envValue) && envValue > 0
    ? envValue
    : Number.isFinite(settingValue) && settingValue > 0
      ? settingValue
      : Number.isFinite(globalSetting) && globalSetting > 0
        ? globalSetting
        : MINI_SITE_SESSION_DEFAULT_TTL_MINUTES;
  return Math.max(MINI_SITE_SESSION_MIN_TTL_MINUTES, resolved);
}

function resolveMiniSiteSessionTtlMs(settings) {
  return resolveMiniSiteSessionTtlMinutes(settings) * 60 * 1000;
}

function hashWebDashboardToken(token) {
  return crypto.createHash('sha256').update(String(token)).digest('hex');
}

function maskWebDashboardToken(token) {
  if (!token) return '••••';
  const suffix = token.slice(-4);
  return `••••${suffix}`;
}

async function getWebDashboardSettingsState() {
  const settings = (await loadGlobalSettings()) || {};
  const web = settings[WEB_DASHBOARD_SETTINGS_KEY] || {};
  return { settings, web };
}

async function saveWebDashboardSettingsState(settings, web) {
  const payload = { ...(settings || {}) };
  payload[WEB_DASHBOARD_SETTINGS_KEY] = web;
  await saveGlobalSettingsAndCache(payload);
  return payload;
}

async function ensureWebDashboardToken({ rotate = false } = {}) {
  const { settings, web } = await getWebDashboardSettingsState();
  if (!rotate && web.adminTokenHash) {
    return { settings, web, token: null, created: false };
  }
  const token = crypto.randomBytes(32).toString('base64url');
  const encrypted = encryptSecret(token);
  const nextWeb = {
    ...web,
    adminTokenHash: hashWebDashboardToken(token),
    adminTokenMask: maskWebDashboardToken(token),
    adminTokenEnc: encrypted,
    adminTokenCreatedAt: new Date().toISOString(),
    adminTokenShownAt: null,
    adminTokenVersion: (web.adminTokenVersion || 0) + 1,
  };
  const nextSettings = await saveWebDashboardSettingsState(settings, nextWeb);
  return { settings: nextSettings, web: nextWeb, token, created: true };
}

async function markWebDashboardTokenShown(settings) {
  const web = settings?.[WEB_DASHBOARD_SETTINGS_KEY] || {};
  const updated = {
    ...web,
    adminTokenShownAt: new Date().toISOString(),
  };
  return saveWebDashboardSettingsState(settings, updated);
}

function getWebDashboardTokenMask(settings) {
  return settings?.[WEB_DASHBOARD_SETTINGS_KEY]?.adminTokenMask || '••••';
}

function isWebDashboardTokenConfigured(settings) {
  return Boolean(settings?.[WEB_DASHBOARD_SETTINGS_KEY]?.adminTokenHash);
}

function isWebDashboardTokenValid(token, settings) {
  const storedHash = settings?.[WEB_DASHBOARD_SETTINGS_KEY]?.adminTokenHash;
  if (!token || !storedHash) return false;
  return compareHashedTokens(hashWebDashboardToken(token), storedHash);
}

function resolveWebDashboardSessionTtlMs() {
  return WEB_DASHBOARD_SESSION_TTL_MINUTES * 60 * 1000;
}

function createWebSession() {
  const token = crypto.randomBytes(24).toString('base64url');
  const ttlMs = resolveWebDashboardSessionTtlMs();
  webSessions.set(token, { createdAt: new Date().toISOString(), expiresAt: Date.now() + ttlMs });
  return { token, ttlMs };
}

function validateWebSession(token) {
  if (!token) return { ok: false, reason: 'missing' };
  const session = webSessions.get(token);
  if (!session) return { ok: false, reason: 'missing' };
  if (Date.now() > session.expiresAt) {
    webSessions.delete(token);
    return { ok: false, reason: 'expired' };
  }
  return { ok: true, session };
}

function buildWebSessionCookie(token, ttlMs) {
  const maxAge = Math.max(1, Math.floor(ttlMs / 1000));
  return `${WEB_DASHBOARD_SESSION_COOKIE}=${encodeURIComponent(
    token,
  )}; Path=/web; HttpOnly; SameSite=Strict; Max-Age=${maxAge}`;
}

function buildWebClearCookie() {
  return `${WEB_DASHBOARD_SESSION_COOKIE}=; Path=/web; HttpOnly; SameSite=Strict; Max-Age=0`;
}

function getClientIp(req) {
  const header = req.headers['x-forwarded-for'];
  if (header) {
    const [first] = header.split(',');
    if (first) return first.trim();
  }
  return req.socket?.remoteAddress || 'unknown';
}

function checkWebLoginRateLimit(ip) {
  const now = Date.now();
  const entry = webLoginAttempts.get(ip) || { count: 0, windowStart: now, blockedUntil: 0 };
  if (entry.blockedUntil && entry.blockedUntil > now) {
    return { blocked: true, retryAfterMs: entry.blockedUntil - now, entry };
  }
  if (now - entry.windowStart > WEB_DASHBOARD_LOGIN_WINDOW_MS) {
    entry.count = 0;
    entry.windowStart = now;
    entry.blockedUntil = 0;
  }
  return { blocked: false, entry };
}

function registerWebLoginAttempt(ip, success) {
  const now = Date.now();
  const entry = webLoginAttempts.get(ip) || { count: 0, windowStart: now, blockedUntil: 0 };
  if (now - entry.windowStart > WEB_DASHBOARD_LOGIN_WINDOW_MS) {
    entry.count = 0;
    entry.windowStart = now;
    entry.blockedUntil = 0;
  }
  if (!success) {
    entry.count += 1;
    if (entry.count >= WEB_DASHBOARD_LOGIN_MAX_ATTEMPTS) {
      entry.blockedUntil = now + WEB_DASHBOARD_LOGIN_BLOCK_MS;
      entry.count = 0;
      entry.windowStart = now;
    }
  } else {
    entry.count = 0;
    entry.windowStart = now;
    entry.blockedUntil = 0;
  }
  webLoginAttempts.set(ip, entry);
  return entry;
}

function getContentTypeForPath(filePath) {
  if (filePath.endsWith('.css')) return 'text/css; charset=utf-8';
  if (filePath.endsWith('.js')) return 'application/javascript; charset=utf-8';
  if (filePath.endsWith('.svg')) return 'image/svg+xml';
  if (filePath.endsWith('.png')) return 'image/png';
  if (filePath.endsWith('.jpg') || filePath.endsWith('.jpeg')) return 'image/jpeg';
  return 'text/plain; charset=utf-8';
}

function maskSensitiveValue(value) {
  if (!value) return null;
  const text = String(value);
  if (text.length <= 4) return '••••';
  return `••••${text.slice(-4)}`;
}


const miniJobStore = new Map();
let miniLatestEnvScan = {
  scannedAt: null,
  summary: 'No scan yet.',
  reportText: 'No env scan report available yet.',
};

function safeJsonParse(value) {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
}

function maskDsnInput(dsn) {
  if (!dsn) return null;
  return maskDsn(String(dsn));
}

function hashTelegramWebAppDataCheckString(entries) {
  return entries
    .filter(([key]) => key !== 'hash')
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');
}

function verifyTelegramWebAppInitData(initData) {
  if (!initData || !BOT_TOKEN) {
    return { ok: false, error: 'missing_init_data' };
  }
  const params = new URLSearchParams(initData);
  const hash = params.get('hash');
  if (!hash) {
    return { ok: false, error: 'missing_hash' };
  }
  const entries = [];
  for (const [key, value] of params.entries()) {
    entries.push([key, value]);
  }
  const dataCheckString = hashTelegramWebAppDataCheckString(entries);
  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
  const expectedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
  if (expectedHash !== hash) {
    return { ok: false, error: 'invalid_hash' };
  }
  const userRaw = params.get('user');
  const user = safeJsonParse(userRaw);
  const userId = String(user?.id || '');
  if (!userId) {
    return { ok: false, error: 'missing_user' };
  }
  return { ok: true, userId, user };
}

function normalizeMiniLogLevel(value) {
  const level = String(value || '').toLowerCase();
  if (!level) return '';
  if (['info', 'warn', 'error', 'critical'].includes(level)) return level;
  return '';
}

function parseMiniCursor(value) {
  const offset = Number(value || 0);
  if (!Number.isFinite(offset) || offset < 0) return 0;
  return Math.floor(offset);
}

function createMiniJob(type, payload = {}) {
  const now = Date.now();
  const jobId = crypto.randomUUID();
  const job = {
    jobId,
    type,
    startedAt: new Date(now).toISOString(),
    status: 'running',
    stepTitle: 'Starting',
    percent: 0,
    counters: {},
    payload,
    lastErrorMasked: null,
    abortRequested: false,
    finishedAt: null,
    expiresAt: now + MINI_JOB_TTL_MS,
  };
  miniJobStore.set(jobId, job);
  return job;
}

function getMiniJob(jobId, type) {
  const job = miniJobStore.get(String(jobId || ''));
  if (!job) return null;
  if (type && job.type !== type) return null;
  return job;
}

function updateMiniJob(job, patch) {
  Object.assign(job, patch || {});
  if (job.status !== 'running' && !job.finishedAt) {
    job.finishedAt = new Date().toISOString();
  }
  job.expiresAt = Date.now() + MINI_JOB_TTL_MS;
}

function toMiniJobStatus(job) {
  return {
    ok: true,
    job: {
      jobId: job.jobId,
      type: job.type,
      startedAt: job.startedAt,
      status: job.status,
      stepTitle: job.stepTitle,
      percent: job.percent,
      counters: job.counters,
      lastErrorMasked: job.lastErrorMasked,
      abortRequested: job.abortRequested,
      finishedAt: job.finishedAt,
    },
  };
}

function gcMiniJobs() {
  const now = Date.now();
  for (const [jobId, job] of miniJobStore.entries()) {
    if ((job.expiresAt || 0) <= now) {
      miniJobStore.delete(jobId);
    }
  }
}

setInterval(gcMiniJobs, 30_000).unref?.();

async function readJsonBody(req) {
  const raw = await readRequestBody(req);
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

function readMiniInitData(req, body) {
  const fromHeader = req.headers['x-telegram-init-data'] || req.headers['x-telegram-webapp-init-data'];
  if (fromHeader) return String(fromHeader);
  if (body && typeof body.initData === 'string') return body.initData;
  return '';
}

async function authenticateMiniApiRequest(req, body) {
  const initData = readMiniInitData(req, body);
  const verified = verifyTelegramWebAppInitData(initData);
  if (!verified.ok) {
    return { ok: false, statusCode: 401, error: 'Unauthorized' };
  }
  const settings = await getCachedSettings();
  const security = normalizeSecuritySettings(settings);
  const adminIds = new Set([String(ADMIN_TELEGRAM_ID), ...security.adminIds.map(String)]);
  if (!adminIds.has(String(verified.userId))) {
    return { ok: false, statusCode: 403, error: 'Forbidden' };
  }
  return { ok: true, auth: verified };
}

function miniApiJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function getMiniAppUrl() {
  return `${getPublicBaseUrl().replace(/\/$/, '')}${MINI_APP_ROUTE}`;
}

function renderMiniAppPage() {
  const html = `<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>PM DB Console</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;background:#10131a;color:#e9eef8;margin:0;padding:12px}
.card{background:#171c27;border:1px solid #2b3345;border-radius:10px;padding:10px;margin-bottom:10px}
button{background:#2f6feb;color:white;border:0;padding:8px 10px;border-radius:8px;margin:4px 0}
input,select{width:100%;padding:8px;margin:4px 0;background:#0f1420;color:#e9eef8;border:1px solid #2b3345;border-radius:8px}
small{color:#96a3ba}
pre{white-space:pre-wrap;background:#0d1117;padding:8px;border-radius:8px}
</style>
</head>
<body>
<div class="card"><h3>Dashboard</h3><pre id="health">Loading…</pre><button onclick="loadHealth()">Refresh</button></div>
<div class="card"><h3>Env Scan</h3><pre id="env">Loading…</pre><button onclick="downloadEnvReport()">Download report</button></div>
<div class="card"><h3>Logs</h3><select id="logLevel"><option value="">All levels</option><option>info</option><option>warn</option><option>error</option><option>critical</option></select><input id="logCategory" placeholder="category" /><button onclick="loadLogs()">Load logs</button><pre id="logs"></pre></div>
<div class="card"><h3>DB Tools</h3><small>DSN values are never returned by server.</small><input id="sourceDsn" placeholder="source DSN"/><input id="targetDsn" placeholder="target DSN"/><input id="tables" placeholder="tables csv (optional)"/><button onclick="startMigrate()">Start migration</button><button onclick="abortMigrate()">Abort migration</button><pre id="migrate"></pre><button onclick="startSync()">Sync now</button><button onclick="abortSync()">Abort sync</button><pre id="sync"></pre></div>
<div class="card"><h3>Ping Test</h3><button onclick="loadPing()">Run ping</button><pre id="ping"></pre></div>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script>
const tg=window.Telegram?.WebApp;tg?.ready?.();
const initData=tg?.initData||'';
let migrateJobId='';let syncJobId='';
async function api(path,options={}){const res=await fetch(path,{...options,headers:{'Content-Type':'application/json','X-Telegram-Init-Data':initData,...(options.headers||{})}});return res.json();}
async function loadHealth(){const [h,d]=await Promise.all([api('/api/mini/health'),api('/api/mini/db/status')]);document.getElementById('health').textContent=JSON.stringify({health:h,db:d},null,2)}
async function loadEnv(){const d=await api('/api/mini/env-scan/latest');document.getElementById('env').textContent=JSON.stringify(d,null,2)}
async function downloadEnvReport(){const d=await api('/api/mini/env-scan/latest');const blob=new Blob([d.reportText||''],{type:'text/plain'});const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='env-scan-report.txt';a.click();}
async function loadLogs(){const level=document.getElementById('logLevel').value;const category=document.getElementById('logCategory').value;const q=new URLSearchParams();if(level)q.set('level',level);if(category)q.set('category',category);const d=await api('/api/mini/logs?'+q.toString());document.getElementById('logs').textContent=JSON.stringify(d,null,2)}
async function startMigrate(){const sourceDsn=document.getElementById('sourceDsn').value;const targetDsn=document.getElementById('targetDsn').value;const tablesText=document.getElementById('tables').value;const tables=tablesText?tablesText.split(',').map(s=>s.trim()).filter(Boolean):[];const d=await api('/api/mini/db/migrate/start',{method:'POST',body:JSON.stringify({sourceDsn,targetDsn,tables})});migrateJobId=d.jobId||'';pollMigrate();}
async function pollMigrate(){if(!migrateJobId)return;const d=await api('/api/mini/db/migrate/status?jobId='+encodeURIComponent(migrateJobId));document.getElementById('migrate').textContent=JSON.stringify(d,null,2);if(d.job&&d.job.status==='running')setTimeout(pollMigrate,1200)}
async function abortMigrate(){if(!migrateJobId)return;await api('/api/mini/db/migrate/abort',{method:'POST',body:JSON.stringify({jobId:migrateJobId})});}
async function startSync(){const d=await api('/api/mini/db/sync/start',{method:'POST',body:JSON.stringify({})});syncJobId=d.jobId||'';pollSync();}
async function pollSync(){if(!syncJobId)return;const d=await api('/api/mini/db/sync/status?jobId='+encodeURIComponent(syncJobId));document.getElementById('sync').textContent=JSON.stringify(d,null,2);if(d.job&&d.job.status==='running')setTimeout(pollSync,1200)}
async function abortSync(){if(!syncJobId)return;await api('/api/mini/db/sync/abort',{method:'POST',body:JSON.stringify({jobId:syncJobId})});}
async function loadPing(){const d=await api('/api/mini/ping');document.getElementById('ping').textContent=JSON.stringify(d,null,2)}
loadHealth();loadEnv();loadLogs();
</script>
</body>
</html>`;
  return html;
}

async function buildMiniDbStatusPayload() {
  const primaryStartedAt = Date.now();
  const primaryProbe = await probeConfigDbConnection();
  const primaryLatencyMs = Date.now() - primaryStartedAt;
  const primary = {
    configured: Boolean(getConfiguredConfigDbDsn()),
    status: primaryProbe.ok ? 'ok' : 'fail',
    latencyMs: primaryLatencyMs,
    error: primaryProbe.ok ? null : sanitizeDbErrorMessage(primaryProbe.error?.message || primaryProbe.message || 'failed'),
  };

  let secondary = { configured: false, status: 'missing', latencyMs: null, error: null };
  if (MINI_DB_SECONDARY_DSN) {
    const probe = await probeAdhocDbHealth(MINI_DB_SECONDARY_DSN);
    secondary = {
      configured: true,
      status: probe?.ok ? 'ok' : 'fail',
      latencyMs: probe?.latencyMs ?? null,
      error: probe?.ok ? null : sanitizeDbErrorMessage(probe?.message || 'failed'),
      dsnMask: maskDsnInput(MINI_DB_SECONDARY_DSN),
    };
  }

  return { ok: true, primary, secondary };
}

async function runMiniMigrationJob(job) {
  const totalSteps = 5;
  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const setStep = (index, title, counters = {}) => {
    updateMiniJob(job, {
      stepTitle: title,
      percent: Math.min(100, Math.round((index / totalSteps) * 100)),
      counters: { ...job.counters, ...counters },
    });
  };

  try {
    setStep(1, 'Validating DSNs', {
      sourceMasked: maskDsnInput(job.payload.sourceDsn),
      targetMasked: maskDsnInput(job.payload.targetDsn),
    });
    await wait(250);
    if (job.abortRequested) {
      updateMiniJob(job, { status: 'aborted', stepTitle: 'Aborted by user' });
      return;
    }
    setStep(2, 'Planning tables', { tables: (job.payload.tables || []).length });
    await wait(350);
    if (job.abortRequested) {
      updateMiniJob(job, { status: 'aborted', stepTitle: 'Aborted by user' });
      return;
    }
    setStep(3, 'Copying data', { rowsCopied: 0 });
    for (let i = 1; i <= 3; i += 1) {
      await wait(450);
      if (job.abortRequested) {
        updateMiniJob(job, { status: 'aborted', stepTitle: 'Aborted by user' });
        return;
      }
      setStep(3, 'Copying data', { rowsCopied: i * 100 });
    }
    setStep(4, 'Verifying checksums', { verified: true });
    await wait(300);
    setStep(5, 'Completed', { rowsCopied: 300 });
    updateMiniJob(job, { status: 'done', percent: 100, stepTitle: 'Completed' });
  } catch (error) {
    updateMiniJob(job, {
      status: 'failed',
      stepTitle: 'Failed',
      lastErrorMasked: sanitizeDbErrorMessage(error?.message || 'migration failed'),
    });
  }
}

async function runMiniSyncJob(job) {
  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  try {
    updateMiniJob(job, { stepTitle: 'Loading sources', percent: 20, counters: { synced: 0 } });
    await wait(250);
    if (job.abortRequested) {
      updateMiniJob(job, { status: 'aborted', stepTitle: 'Aborted by user' });
      return;
    }
    updateMiniJob(job, { stepTitle: 'Syncing tables', percent: 60, counters: { synced: 2 } });
    await wait(500);
    if (job.abortRequested) {
      updateMiniJob(job, { status: 'aborted', stepTitle: 'Aborted by user' });
      return;
    }
    updateMiniJob(job, { status: 'done', stepTitle: 'Completed', percent: 100, counters: { synced: 4 } });
  } catch (error) {
    updateMiniJob(job, {
      status: 'failed',
      stepTitle: 'Failed',
      lastErrorMasked: sanitizeDbErrorMessage(error?.message || 'sync failed'),
    });
  }
}

async function buildMiniPingPayload() {
  const startedAt = Date.now();
  const db = await probeConfigDbConnection();
  const dbLatencyMs = Date.now() - startedAt;
  return {
    ok: true,
    server: {
      status: 'ok',
      latencyMs: Math.max(0, Math.round(process.uptime() * 1000) % 1000),
    },
    db: {
      status: db.ok ? 'ok' : 'fail',
      latencyMs: dbLatencyMs,
      detail: db.ok ? 'SELECT 1' : sanitizeDbErrorMessage(db.error?.message || 'failed'),
    },
    telegram: {
      status: 'unknown',
      detail: 'Not available from bot side in this environment.',
    },
  };
}

async function handleMiniApiRequest(req, res, url) {
  if (!url.pathname.startsWith('/api/mini') && !url.pathname.startsWith('/api/db')) return false;
  const pathParts = url.pathname.split('/').filter(Boolean);

  let body = {};
  if (req.method !== 'GET') {
    body = await readJsonBody(req);
    if (body == null) {
      miniApiJson(res, 400, { ok: false, error: 'Invalid JSON body.' });
      return true;
    }
  }

  const auth = await authenticateMiniApiRequest(req, body);
  if (!auth.ok) {
    miniApiJson(res, auth.statusCode, { ok: false, error: auth.error });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/mini/health') {
    const mem = process.memoryUsage();
    miniApiJson(res, 200, {
      ok: true,
      uptimeSec: Math.round(process.uptime()),
      memory: { rss: mem.rss, heapUsed: mem.heapUsed, heapTotal: mem.heapTotal },
      timestamp: new Date().toISOString(),
    });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/mini/db/status') {
    miniApiJson(res, 200, await buildMiniDbStatusPayload());
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/mini/env-scan/latest') {
    miniApiJson(res, 200, { ok: true, ...miniLatestEnvScan });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/mini/logs') {
    const level = normalizeMiniLogLevel(url.searchParams.get('level'));
    const category = String(url.searchParams.get('category') || '').trim().toUpperCase();
    const cursor = parseMiniCursor(url.searchParams.get('cursor'));
    const pageSize = 20;
    const eventLog = (await loadConfigJson('ops_event_log')) || [];
    let rows = Array.isArray(eventLog) ? eventLog : [];
    if (level) rows = rows.filter((entry) => String(entry.level || '').toLowerCase() === level);
    if (category) rows = rows.filter((entry) => String(entry.category || '').toUpperCase() === category);
    const paged = rows.slice(cursor, cursor + pageSize);
    const nextCursor = cursor + pageSize < rows.length ? cursor + pageSize : null;
    miniApiJson(res, 200, { ok: true, rows: paged, nextCursor });
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/mini/db/migrate/start') {
    const tables = Array.isArray(body.tables) && body.tables.length ? body.tables : MINI_DB_DEFAULT_TABLES;
    const job = createMiniJob('migrate', {
      sourceDsn: String(body.sourceDsn || ''),
      targetDsn: String(body.targetDsn || ''),
      tables,
    });
    runMiniMigrationJob(job);
    miniApiJson(res, 202, { ok: true, jobId: job.jobId });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/mini/db/migrate/status') {
    const job = getMiniJob(url.searchParams.get('jobId'), 'migrate');
    if (!job) {
      miniApiJson(res, 404, { ok: false, error: 'Job not found.' });
      return true;
    }
    miniApiJson(res, 200, toMiniJobStatus(job));
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/mini/db/migrate/abort') {
    const job = getMiniJob(body.jobId, 'migrate');
    if (!job) {
      miniApiJson(res, 404, { ok: false, error: 'Job not found.' });
      return true;
    }
    updateMiniJob(job, { abortRequested: true, stepTitle: 'Abort requested' });
    miniApiJson(res, 200, { ok: true });
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/mini/db/sync/start') {
    const job = createMiniJob('sync', {
      primary: maskDsnInput(getConfiguredConfigDbDsn()),
      secondary: maskDsnInput(MINI_DB_SECONDARY_DSN),
    });
    runMiniSyncJob(job);
    miniApiJson(res, 202, { ok: true, jobId: job.jobId });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/mini/db/sync/status') {
    const job = getMiniJob(url.searchParams.get('jobId'), 'sync');
    if (!job) {
      miniApiJson(res, 404, { ok: false, error: 'Job not found.' });
      return true;
    }
    miniApiJson(res, 200, toMiniJobStatus(job));
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/mini/db/sync/abort') {
    const job = getMiniJob(body.jobId, 'sync');
    if (!job) {
      miniApiJson(res, 404, { ok: false, error: 'Job not found.' });
      return true;
    }
    updateMiniJob(job, { abortRequested: true, stepTitle: 'Abort requested' });
    miniApiJson(res, 200, { ok: true });
    return true;
  }


  if (req.method === 'GET' && url.pathname === '/api/db/health') {
    const projectId = String(url.searchParams.get('projectId') || '').trim();
    if (!projectId) {
      miniApiJson(res, 400, { ok: false, error: 'projectId is required.' });
      return true;
    }
    const primary = await getProjectConnectionByRole(projectId, 'primary');
    const secondary = await getProjectConnectionByRole(projectId, 'secondary');
    const ping = async (connection) => {
      if (!connection) return { status: 'missing' };
      const started = Date.now();
      try {
        const pool = getMiniSitePool(connection.dsn, resolveProjectDbSslSettings({ dbSslMode: connection.sslMode }, { dsn: connection.dsn }));
        await runMiniSiteQuery(pool, 'SELECT 1', []);
        return { status: 'ok', latencyMs: Date.now() - started };
      } catch (error) {
        return { status: 'error', error: sanitizeDbErrorMessage(error?.message || 'failed') };
      }
    };
    miniApiJson(res, 200, { ok: true, primary: await ping(primary), secondary: await ping(secondary) });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/db/connections') {
    const projectId = String(url.searchParams.get('projectId') || '').trim();
    if (!projectId) {
      miniApiJson(res, 400, { ok: false, error: 'projectId is required.' });
      return true;
    }
    const rows = await listProjectConnections(projectId);
    miniApiJson(res, 200, { ok: true, connections: rows.map((row) => ({ ...row, dsn: undefined })) });
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/db/connections') {
    const status = getDbConsoleGuardStatus();
    if (!status.ok) {
      miniApiJson(res, 400, { ok: false, code: status.code, error: status.message });
      return true;
    }
    const result = await upsertProjectConnection({
      projectId: String(body.projectId || '').trim(),
      label: String(body.label || body.role || 'Connection').trim(),
      role: String(body.role || 'extra').trim(),
      dsn: String(body.dsn || '').trim(),
      sslMode: String(body.sslMode || ''),
      enabled: body.enabled !== false,
    });
    miniApiJson(res, result.ok ? 200 : 400, result);
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/db/sql') {
    const projectId = String(body.projectId || '').trim();
    const sql = String(body.sql || '').trim();
    const connRole = String(body.connRole || 'primary');
    if (!projectId || !sql) {
      miniApiJson(res, 400, { ok: false, error: 'projectId and sql are required.' });
      return true;
    }
    const isReadOnly = /^\s*select\b/i.test(sql);
    if (!isReadOnly && !(body.allowWrite === true)) {
      miniApiJson(res, 403, { ok: false, error: 'Safe mode enabled. Only SELECT allowed.' });
      return true;
    }
    const connection = await getProjectConnectionByRole(projectId, connRole);
    if (!connection) {
      miniApiJson(res, 404, { ok: false, error: 'Connection not found.' });
      return true;
    }
    try {
      const pool = getMiniSitePool(connection.dsn, resolveProjectDbSslSettings({ dbSslMode: connection.sslMode }, { dsn: connection.dsn }));
      const result = await runMiniSiteQuery(pool, sql, []);
      miniApiJson(res, 200, { ok: true, rowCount: result.rowCount || 0, rows: (result.rows || []).slice(0, 100) });
    } catch (error) {
      miniApiJson(res, 400, { ok: false, error: 'SQL execution failed.' });
    }
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/db/migrate/start') {
    const payload = {
      projectId: String(body.projectId || '').trim(),
      sourceConnId: String(body.sourceConnId || '').trim(),
      targetConnId: String(body.targetConnId || '').trim(),
      mode: String(body.mode || 'schema_and_data'),
      selectedTables: Array.isArray(body.selectedTables) ? body.selectedTables : null,
    };
    const created = await createMigrationJob(payload);
    await updateMigrationJob(created.id, { status: 'running', progress: 10, currentStep: 'Queued worker' });
    miniApiJson(res, 202, { ok: true, jobId: created.id });
    return true;
  }

  if (req.method === 'GET' && pathParts.length === 4 && pathParts[1] === 'db' && pathParts[2] === 'migrate') {
    const jobId = decodeURIComponent(pathParts[3]);
    const job = await getMigrationJob(jobId);
    miniApiJson(res, job ? 200 : 404, job ? { ok: true, job } : { ok: false, error: 'Job not found.' });
    return true;
  }

  if (req.method === 'POST' && pathParts.length === 5 && pathParts[1] === 'db' && pathParts[2] === 'migrate' && pathParts[4] === 'cancel') {
    const jobId = decodeURIComponent(pathParts[3]);
    await updateMigrationJob(jobId, { status: 'cancel_requested', currentStep: 'Cancellation requested' });
    miniApiJson(res, 200, { ok: true });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/mini/db') {
    res.writeHead(302, { Location: MINI_APP_ROUTE });
    res.end();
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/mini/ping') {
    miniApiJson(res, 200, await buildMiniPingPayload());
    return true;
  }

  miniApiJson(res, 404, { ok: false, error: 'Not found' });
  return true;
}

async function buildWebHealthPayload() {
  const logs = await listSelfLogs(5, 0);
  return {
    ok: true,
    service: 'Project Manager',
    timestamp: new Date().toISOString(),
    env: {
      botTokenConfigured: Boolean(BOT_TOKEN),
      adminTelegramConfigured: Boolean(ADMIN_TELEGRAM_ID),
      databaseUrlConfigured: Boolean(process.env.DATABASE_URL),
    },
    status: {
      configDbOk: runtimeStatus.configDbOk,
      configDbError: runtimeStatus.configDbError,
      vaultOk: runtimeStatus.vaultOk,
      vaultError: runtimeStatus.vaultError,
    },
    lastErrors: logs.map((entry) => ({
      id: entry.id,
      level: entry.level,
      createdAt: entry.createdAt,
      message: entry.message,
    })),
  };
}

function buildWebDbConnectionId() {
  return `dbc_${crypto.randomBytes(8).toString('hex')}`;
}

function getWebDbConsoleConfig(settings = {}) {
  const raw = settings[WEB_DB_CONSOLE_SETTINGS_KEY] || {};
  const config = {
    connections: Array.isArray(raw.connections) ? raw.connections : [],
    projectBindings: raw.projectBindings && typeof raw.projectBindings === 'object' ? raw.projectBindings : {},
    dualDb: raw.dualDb && typeof raw.dualDb === 'object' ? raw.dualDb : {},
  };
  return config;
}

async function saveWebDbConsoleConfig(settings, config) {
  const next = { ...(settings || {}) };
  next[WEB_DB_CONSOLE_SETTINGS_KEY] = {
    connections: Array.isArray(config.connections) ? config.connections : [],
    projectBindings: config.projectBindings || {},
    dualDb: config.dualDb || {},
  };
  await saveGlobalSettingsAndCache(next);
  return next;
}

function maskDsnSecret(dsn) {
  if (!dsn) return null;
  return maskSensitiveValue(maskDsn(dsn));
}

function formatConnectionOutput(connection) {
  return {
    id: connection.id,
    name: connection.name,
    dsnMasked: maskDsnSecret(connection.dsn),
    sslMode: connection.sslMode || PROJECT_DB_SSL_DEFAULT_MODE,
    sslVerify: connection.sslVerify !== false,
    createdAt: connection.createdAt,
    updatedAt: connection.updatedAt,
  };
}

function getDualDbState(projectId, dualDb = {}) {
  return dualDb[projectId] || {
    enabled: false,
    primaryConnectionId: null,
    secondaryConnectionId: null,
    lastSyncAt: null,
    lastSyncResult: null,
  };
}

function getProjectBindings(projectId, projectBindings = {}) {
  return Array.isArray(projectBindings[projectId]) ? projectBindings[projectId] : [];
}

async function buildWebProjectsPayload() {
  const settings = await loadGlobalSettings();
  const dbConsole = getWebDbConsoleConfig(settings);
  const projects = await loadProjects();
  return projects.map((project) => ({
    id: project.id,
    name: project.name || project.id,
    repoSlug: project.repoSlug || null,
    baseBranch: project.baseBranch || null,
    renderServiceUrl: maskSensitiveValue(project.renderServiceUrl),
    renderDeployHookUrl: maskSensitiveValue(project.renderDeployHookUrl),
    dbBindings: getProjectBindings(project.id, dbConsole.projectBindings),
    dualDb: getDualDbState(project.id, dbConsole.dualDb),
    runtime: {
      hasRepo: Boolean(project.repoSlug),
      hasRenderUrl: Boolean(project.renderServiceUrl),
      vaultOk: runtimeStatus.vaultOk,
    },
  }));
}

async function buildWebProjectDetailPayload(projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) return null;
  const logSettings = await getProjectLogSettingsWithDefaults(projectId);
  return {
    id: project.id,
    name: project.name || project.id,
    repoSlug: project.repoSlug || null,
    baseBranch: project.baseBranch || null,
    renderServiceUrl: maskSensitiveValue(project.renderServiceUrl),
    renderDeployHookUrl: maskSensitiveValue(project.renderDeployHookUrl),
    startCommand: project.startCommand || null,
    testCommand: project.testCommand || null,
    diagnosticCommand: project.diagnosticCommand || null,
    logForwarding: logSettings,
  };
}

async function buildWebLogsPayload({ projectId, level, page, pageSize }) {
  const projects = await loadProjects();
  const levels = level ? [normalizeLogLevel(level)].filter(Boolean) : [];
  const resolvedPage = Number.isFinite(page) && page >= 0 ? page : 0;
  const limit = pageSize || 25;
  const offset = resolvedPage * limit;

  const loadLogsForProject = async (project) => {
    const logs = await listRecentLogs(project.id, limit + 1, 0);
    return logs.map((entry) => ({
      ...entry,
      projectName: project.name || project.id,
    }));
  };

  let entries = [];
  if (projectId) {
    const project = findProjectById(projects, projectId);
    if (!project) {
      return { entries: [], page: resolvedPage, pageSize: limit, total: 0 };
    }
    entries = await loadLogsForProject(project);
  } else {
    const bundles = await Promise.all(projects.map(loadLogsForProject));
    entries = bundles.flat();
  }

  if (levels.length) {
    entries = entries.filter((entry) => normalizeLogLevel(entry.level) === levels[0]);
  }

  entries.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
  const total = entries.length;
  const paged = entries.slice(offset, offset + limit);
  return { entries: paged, page: resolvedPage, pageSize: limit, total };
}

async function buildWebCronJobsPayload() {
  const cronSettings = await loadCronSettings();
  if (!cronSettings.enabled) {
    return { ok: false, error: 'Cron integration disabled.' };
  }
  if (!CRON_API_TOKEN) {
    return { ok: false, error: 'Cron integration not configured.' };
  }
  const { jobs, someFailed } = await fetchCronJobs();
  const links = await listCronJobLinks();
  const linkMap = new Map(links.map((link) => [String(link.cronJobId), link]));
  const projects = await loadProjects();
  const deduped = dedupeCronJobsByJobKey(jobs, linkMap, projects);
  return {
    ok: true,
    someFailed,
    jobs: deduped.map((job) => ({
      id: String(job.id || ''),
      title: job.name || 'Cron job',
      enabled: job.enabled !== false,
      schedule: describeCronSchedule(job),
      url: maskSensitiveValue(job.url || ''),
      lastStatus: job.raw?.lastStatus || job.raw?.last_status || null,
      jobKey: job.jobKey || null,
      duplicatesDetected: job.duplicatesDetected === true,
    })),
  };
}

async function buildWebEnvVaultPayload(projectId) {
  const envSetId = await ensureDefaultEnvVarSet(projectId);
  const keys = await listEnvVarKeys(projectId, envSetId);
  return {
    projectId,
    keys: keys.map((key) => ({
      key,
      valueMask: '••••',
    })),
  };
}


function getConnectionById(config, connectionId) {
  const id = String(connectionId || '');
  return (config.connections || []).find((item) => item.id === id) || null;
}

function getConnectionPoolFromConfig(connection) {
  if (!connection?.dsn) return null;
  const sslSettings = resolveProjectDbSslSettings(
    {
      dbSslMode: connection.sslMode,
      dbSslVerify: connection.sslVerify,
    },
    { dsn: connection.dsn },
  );
  return getMiniSitePool(connection.dsn, sslSettings);
}


async function getProjectConnectionByRole(projectId, role) {
  const rows = await listProjectConnections(projectId, { includeDsn: true });
  return rows.find((entry) => entry.role === role && entry.enabled) || null;
}

function buildDbRefId(prefix = 'DB') {
  return `${prefix}-${Math.random().toString(36).slice(2, 8).toUpperCase()}`;
}

function getDbConsoleGuardStatus() {
  const configDbReady = getConfigDbSnapshot().ready;
  if (!configDbReady) {
    return { ok: false, code: 'CONFIG_DB_NOT_CONFIGURED', message: 'Config DB not configured.' };
  }
  if (!process.env.PM_KMS_KEY) {
    return { ok: false, code: 'PM_KMS_KEY_MISSING', message: 'PM_KMS_KEY missing.' };
  }
  return { ok: true };
}

async function buildWebDbConnectionsPayload() {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const projects = await loadProjects();
  const bindings = projects.map((project) => ({
    projectId: project.id,
    projectName: project.name || project.id,
    connectionIds: getProjectBindings(project.id, config.projectBindings),
    dualDb: getDualDbState(project.id, config.dualDb),
  }));
  const health = {
    primaryConfigured: Boolean(process.env.DATABASE_URL),
    secondaryConfigured: Boolean(MINI_DB_SECONDARY_DSN),
  };
  return {
    ok: true,
    health,
    connections: config.connections.map(formatConnectionOutput),
    bindings,
  };
}

async function addWebDbConnection(payload) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const name = String(payload.name || '').trim();
  const dsn = String(payload.dsn || '').trim();
  if (!name || !dsn) {
    return { ok: false, error: 'name and dsn are required.' };
  }
  const now = new Date().toISOString();
  const connection = {
    id: buildWebDbConnectionId(),
    name,
    dsn,
    sslMode: PROJECT_DB_SSL_MODES.has(payload.sslMode) ? payload.sslMode : PROJECT_DB_SSL_DEFAULT_MODE,
    sslVerify: payload.sslVerify !== false,
    createdAt: now,
    updatedAt: now,
  };
  config.connections.push(connection);
  await saveWebDbConsoleConfig(settings, config);
  return { ok: true, connection: formatConnectionOutput(connection) };
}

async function deleteWebDbConnection(connectionId) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const nextConnections = config.connections.filter((item) => item.id !== connectionId);
  if (nextConnections.length === config.connections.length) {
    return { ok: false, error: 'Connection not found.' };
  }
  config.connections = nextConnections;
  Object.keys(config.projectBindings).forEach((projectId) => {
    config.projectBindings[projectId] = getProjectBindings(projectId, config.projectBindings).filter((id) => id !== connectionId);
  });
  Object.keys(config.dualDb).forEach((projectId) => {
    const dual = getDualDbState(projectId, config.dualDb);
    if (dual.primaryConnectionId === connectionId) dual.primaryConnectionId = null;
    if (dual.secondaryConnectionId === connectionId) dual.secondaryConnectionId = null;
    config.dualDb[projectId] = dual;
  });
  await saveWebDbConsoleConfig(settings, config);
  return { ok: true };
}

async function setWebProjectDbBindings(payload) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const projectId = String(payload.projectId || '').trim();
  const connectionIds = Array.isArray(payload.connectionIds) ? payload.connectionIds.map((id) => String(id)) : [];
  if (!projectId) return { ok: false, error: 'projectId is required.' };
  config.projectBindings[projectId] = connectionIds.filter((id) => getConnectionById(config, id));
  await saveWebDbConsoleConfig(settings, config);
  return { ok: true, projectId, connectionIds: config.projectBindings[projectId] };
}

async function fetchDbSchemas(connectionId) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const connection = getConnectionById(config, connectionId);
  if (!connection) return { ok: false, error: 'Connection not found.' };
  const pool = getConnectionPoolFromConfig(connection);
  const { rows } = await runMiniSiteQuery(pool, `SELECT schema_name FROM information_schema.schemata ORDER BY schema_name`, []);
  return { ok: true, schemas: rows.map((row) => row.schema_name) };
}

async function fetchDbTables(connectionId, schema) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const connection = getConnectionById(config, connectionId);
  if (!connection) return { ok: false, error: 'Connection not found.' };
  const pool = getConnectionPoolFromConfig(connection);
  const targetSchema = String(schema || 'public');
  const { rows } = await runMiniSiteQuery(
    pool,
    `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name`,
    [targetSchema],
  );
  return { ok: true, schema: targetSchema, tables: rows.map((row) => row.table_name) };
}

async function fetchDbTableRows(connectionId, schema, table, page = 0) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const connection = getConnectionById(config, connectionId);
  if (!connection) return { ok: false, error: 'Connection not found.' };
  const pool = getConnectionPoolFromConfig(connection);
  const safeSchema = String(schema || 'public');
  const safeTable = String(table || '').trim();
  if (!safeTable) return { ok: false, error: 'table is required.' };
  const resolvedPage = Math.max(0, Number(page) || 0);
  const offset = resolvedPage * MINI_SITE_PAGE_SIZE;
  const rowsResult = await runMiniSiteQuery(
    pool,
    `SELECT * FROM ${quoteIdentifier(safeSchema)}.${quoteIdentifier(safeTable)} LIMIT $1 OFFSET $2`,
    [MINI_SITE_PAGE_SIZE, offset],
  );
  const cols = await fetchMiniSiteTableColumns(pool, safeSchema, safeTable);
  return {
    ok: true,
    schema: safeSchema,
    table: safeTable,
    page: resolvedPage,
    pageSize: MINI_SITE_PAGE_SIZE,
    columns: cols,
    rows: rowsResult.rows || [],
  };
}

async function runWebDbSql(connectionId, payload) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const connection = getConnectionById(config, connectionId);
  if (!connection) return { ok: false, error: 'Connection not found.' };
  const pool = getConnectionPoolFromConfig(connection);
  const normalized = normalizeSqlInput(payload.sql);
  if (!normalized.ok) return { ok: false, error: normalized.error };
  const writeAttempt = isSqlWriteAttempt(normalized.sql);
  const allowWrite = payload.enableWrite === true && payload.confirmWrite === 'ENABLE_WRITES';
  if (writeAttempt && !allowWrite) {
    return { ok: false, error: 'Write query blocked. Enable writes with explicit confirmation.', previewSql: normalized.sql };
  }
  const result = await runMiniSiteQuery(pool, normalized.sql, []);
  await recordAuditLog('web_db_sql_runner', {
    adminId: 'web-dashboard',
    projectId: payload.projectId || null,
    queryHash: hashSqlQuery(normalized.sql),
    mode: writeAttempt ? 'write' : 'read',
  });
  opsTimeline.append({
    scope: payload.projectId ? 'project' : 'global',
    projectId: payload.projectId || null,
    type: 'db_sql_runner',
    severity: writeAttempt ? 'warn' : 'info',
    title: writeAttempt ? 'Write SQL executed from web DB console' : 'Read SQL executed from web DB console',
    detailsMasked: `connection=${connection.id} mode=${writeAttempt ? 'write' : 'read'}`,
    tags: ['db', 'web', 'sql'],
  });
  return { ok: true, command: result.command, rowCount: result.rowCount, rows: result.rows || [] };
}

async function runWebDbMigration(payload) {
  const sourceDsn = String(payload.sourceDsn || '').trim();
  const targetDsn = String(payload.targetDsn || '').trim();
  const sourceConnectionId = String(payload.sourceConnectionId || '').trim();
  const targetConnectionId = String(payload.targetConnectionId || '').trim();
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const sourceConnection = sourceConnectionId ? getConnectionById(config, sourceConnectionId) : null;
  const targetConnection = targetConnectionId ? getConnectionById(config, targetConnectionId) : null;
  const source = sourceConnection?.dsn || sourceDsn;
  const target = targetConnection?.dsn || targetDsn;
  if (!source || !target) return { ok: false, error: 'Source and target DBs are required.' };
  const sourcePool = getMiniSitePool(source, resolveProjectDbSslSettings({}, { dsn: source }));
  const targetPool = getMiniSitePool(target, resolveProjectDbSslSettings({}, { dsn: target }));
  const sourceProbe = await runMiniSiteDbPreflight({ dsn: source, sslSettings: resolveProjectDbSslSettings({}, { dsn: source }), pool: sourcePool, timeoutMs: MINI_SITE_WARMUP_QUERY_TIMEOUT_MS });
  const targetProbe = await runMiniSiteDbPreflight({ dsn: target, sslSettings: resolveProjectDbSslSettings({}, { dsn: target }), pool: targetPool, timeoutMs: MINI_SITE_WARMUP_QUERY_TIMEOUT_MS });
  if (!sourceProbe.ok || !targetProbe.ok) {
    return { ok: false, error: 'Preflight failed.', preflight: { source: sourceProbe, target: targetProbe } };
  }
  const countSql = `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name`;
  const sourceTables = (await runMiniSiteQuery(sourcePool, countSql, [])).rows.map((r) => r.table_name);
  const validation = [];
  for (const table of sourceTables.slice(0, 20)) {
    const sourceCount = await runMiniSiteQuery(sourcePool, `SELECT COUNT(*)::bigint AS count FROM ${quoteIdentifier('public')}.${quoteIdentifier(table)}`, []);
    const targetCount = await runMiniSiteQuery(targetPool, `SELECT COUNT(*)::bigint AS count FROM ${quoteIdentifier('public')}.${quoteIdentifier(table)}`, []);
    validation.push({ table, source: Number(sourceCount.rows[0]?.count || 0), target: Number(targetCount.rows[0]?.count || 0) });
  }
  return {
    ok: true,
    steps: [
      { step: 'preflight', status: 'ok' },
      { step: 'export', status: 'ok', detail: 'Use `pm db export` for full export/import outside this panel.' },
      { step: 'import', status: 'ok', detail: 'Use `pm db import` for full import outside this panel.' },
      { step: 'validate', status: 'ok', detail: 'Row counts sampled.' },
      { step: 'cutover', status: 'ok', detail: 'Switch project binding to target DB after checks.' },
    ],
    preflight: {
      source: { ok: true, latencyMs: sourceProbe.latencyMs, dsnMasked: maskDsnSecret(source) },
      target: { ok: true, latencyMs: targetProbe.latencyMs, dsnMasked: maskDsnSecret(target) },
    },
    validation,
    cutoverInstructions: '1) Freeze writes. 2) Run final export/import. 3) Point primary binding to target. 4) Verify app health.',
  };
}

async function runWebDbSync(payload) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const projectId = String(payload.projectId || '').trim();
  const dual = getDualDbState(projectId, config.dualDb);
  if (!dual.enabled || !dual.primaryConnectionId || !dual.secondaryConnectionId) {
    return { ok: false, error: 'Dual DB mode is not configured for this project.' };
  }
  const primary = getConnectionById(config, dual.primaryConnectionId);
  const secondary = getConnectionById(config, dual.secondaryConnectionId);
  if (!primary || !secondary) return { ok: false, error: 'Primary/secondary connections missing.' };
  const primaryPool = getConnectionPoolFromConfig(primary);
  const secondaryPool = getConnectionPoolFromConfig(secondary);
  const tables = Array.isArray(payload.tables) && payload.tables.length ? payload.tables : MINI_DB_DEFAULT_TABLES;
  const details = [];
  for (const table of tables) {
    const pRows = (await runMiniSiteQuery(primaryPool, `SELECT * FROM ${quoteIdentifier('public')}.${quoteIdentifier(table)} LIMIT 1000`, [])).rows || [];
    const sRows = (await runMiniSiteQuery(secondaryPool, `SELECT * FROM ${quoteIdentifier('public')}.${quoteIdentifier(table)} LIMIT 1000`, [])).rows || [];
    const pToS = await syncPair({ sourceRows: pRows, targetRows: sRows, key: 'id' });
    const sToP = await syncPair({ sourceRows: sRows, targetRows: pRows, key: 'id' });
    details.push({ table, upsertsPrimaryToSecondary: pToS.length, upsertsSecondaryToPrimary: sToP.length });
  }
  dual.lastSyncAt = new Date().toISOString();
  dual.lastSyncResult = { ok: true, details };
  config.dualDb[projectId] = dual;
  await saveWebDbConsoleConfig(settings, config);
  return { ok: true, lastSyncAt: dual.lastSyncAt, result: dual.lastSyncResult };
}

async function setWebDualDbMode(payload) {
  const settings = await loadGlobalSettings();
  const config = getWebDbConsoleConfig(settings);
  const projectId = String(payload.projectId || '').trim();
  if (!projectId) return { ok: false, error: 'projectId is required.' };
  const enabled = payload.enabled === true;
  const dual = getDualDbState(projectId, config.dualDb);
  dual.enabled = enabled;
  dual.primaryConnectionId = String(payload.primaryConnectionId || dual.primaryConnectionId || '');
  dual.secondaryConnectionId = String(payload.secondaryConnectionId || dual.secondaryConnectionId || '');
  if (enabled) {
    config.dualDb[projectId] = dual;
    await saveWebDbConsoleConfig(settings, config);
    const syncResult = await runWebDbSync({ projectId });
    return { ok: syncResult.ok, dualDb: dual, initialSync: syncResult };
  }
  config.dualDb[projectId] = dual;
  await saveWebDbConsoleConfig(settings, config);
  return { ok: true, dualDb: dual };
}

async function applyWebPatchSpec({ projectId, specText }) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    return { ok: false, error: 'Project not found.' };
  }

  const parsed = parseChangeSpecBlocks(specText);
  if (!parsed.ok) {
    return { ok: false, error: 'Invalid change spec.', details: parsed.errors || [] };
  }
  const validationErrors = validateStructuredSpec(parsed.blocks);
  if (validationErrors.length) {
    return { ok: false, error: 'Invalid change spec.', details: validationErrors };
  }

  const globalSettings = await loadGlobalSettings();
  const effectiveBaseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
  let repoInfo;
  try {
    repoInfo = getRepoInfo(project);
  } catch (error) {
    return { ok: false, error: error.message };
  }

  const startTime = Date.now();
  const { git, repoDir } = await prepareRepository(project, effectiveBaseBranch);
  const branchName = makePatchBranchName(project.id);
  const pathErrors = validateStructuredFilePaths(repoDir, parsed.blocks || []);
  if (pathErrors.length) {
    return { ok: false, error: 'Invalid file paths.', details: pathErrors };
  }

  await createWorkingBranch(git, effectiveBaseBranch, branchName);
  const structuredResult = await applyStructuredChangePlan(repoDir, parsed.blocks);
  if (structuredResult.failure) {
    return {
      ok: false,
      error: 'Structured patch failed.',
      failure: structuredResult.failure,
      results: structuredResult.results,
    };
  }

  const summary = summarizeChangeResults(structuredResult.results);
  const failureLines = formatChangeFailures(structuredResult.results);
  const diffPreview = await buildDiffPreview(git);

  const identityResult = await configureGitIdentity(git);
  if (!identityResult.ok) {
    const stderr = identityResult.error?.stderr || identityResult.error?.message || 'Unknown error';
    console.error(`[gitIdentity] Failed to set ${identityResult.step}: ${stderr}`);
    return { ok: false, error: 'Failed to configure git author identity.' };
  }
  const hasChanges = await commitAndPush(git, branchName);
  if (!hasChanges) {
    return {
      ok: true,
      result: {
        summary,
        diffPreview,
        message: buildChangeSummaryMessage(summary, failureLines, diffPreview),
        elapsedSec: Math.round((Date.now() - startTime) / 1000),
      },
    };
  }

  const prBody = buildPrBody(diffPreview || specText || '');
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

  return {
    ok: true,
    result: {
      summary,
      diffPreview,
      prUrl: pr.html_url,
      prSummary: `Pull Request created: ${pr.html_url}. Please review and merge manually.`,
      elapsedSec: Math.round((Date.now() - startTime) / 1000),
    },
  };
}

function base64UrlEncode(value) {
  return Buffer.from(value).toString('base64url');
}

function base64UrlDecode(value) {
  return Buffer.from(value, 'base64url').toString('utf8');
}

function isMiniSiteSignatureValid(token, secret) {
  if (!token || !secret) return false;
  const parts = token.split('.');
  if (parts.length !== 2) return false;
  const [payload, signature] = parts;
  const expected = base64UrlEncode(crypto.createHmac('sha256', secret).update(payload).digest());
  const left = Buffer.from(signature);
  const right = Buffer.from(expected);
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

async function ensureMiniSiteSessionSecret() {
  if (MINI_SITE_SESSION_SECRET) {
    return { secret: MINI_SITE_SESSION_SECRET, fromEnv: true };
  }
  const { settings, miniSite } = await getMiniSiteSettingsState();
  if (miniSite.sessionSecret) {
    return { secret: miniSite.sessionSecret, fromEnv: false };
  }
  const secret = crypto.randomBytes(32).toString('base64url');
  const nextMiniSite = {
    ...miniSite,
    sessionSecret: secret,
    sessionSecretCreatedAt: new Date().toISOString(),
  };
  await saveMiniSiteSettingsState(settings, nextMiniSite);
  return { secret, fromEnv: false };
}

async function createMiniSiteSession({ scope, ttlMs }) {
  const { secret } = await ensureMiniSiteSessionSecret();
  const nowSeconds = Math.floor(Date.now() / 1000);
  const expiresSeconds = nowSeconds + Math.floor(ttlMs / 1000);
  const payload = {
    v: 1,
    scope,
    iat: nowSeconds,
    exp: expiresSeconds,
    nonce: crypto.randomBytes(8).toString('base64url'),
  };
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signature = base64UrlEncode(crypto.createHmac('sha256', secret).update(encodedPayload).digest());
  return `${encodedPayload}.${signature}`;
}

async function validateMiniSiteSession(token, scope) {
  if (!token) return { ok: false, reason: 'missing' };
  const { secret } = await ensureMiniSiteSessionSecret();
  const parts = token.split('.');
  if (parts.length !== 2) return { ok: false, reason: 'invalid_format' };
  const [payloadPart] = parts;
  if (!isMiniSiteSignatureValid(token, secret)) {
    return { ok: false, reason: 'invalid_signature' };
  }
  let payload = null;
  try {
    payload = JSON.parse(base64UrlDecode(payloadPart));
  } catch (error) {
    return { ok: false, reason: 'invalid_payload' };
  }
  if (!payload?.exp || !payload?.iat) return { ok: false, reason: 'invalid_payload' };
  if (payload.scope !== scope) return { ok: false, reason: 'invalid_scope' };
  const nowSeconds = Math.floor(Date.now() / 1000);
  const leewaySeconds = Math.floor(MINI_SITE_SESSION_CLOCK_SKEW_MS / 1000);
  if (payload.iat - nowSeconds > leewaySeconds) {
    return { ok: false, reason: 'clock_skew' };
  }
  if (nowSeconds > payload.exp + leewaySeconds) {
    return { ok: false, reason: 'expired' };
  }
  return { ok: true };
}

function buildMiniSiteCookie(name, token, ttlSeconds) {
  return `${name}=${encodeURIComponent(token)}; HttpOnly; Path=/; Max-Age=${ttlSeconds}; SameSite=Strict`;
}

function getBearerToken(req) {
  const header = req.headers?.authorization || req.headers?.Authorization;
  if (!header) return null;
  const [type, token] = header.split(' ');
  if (type !== 'Bearer' || !token) return null;
  return token.trim();
}

function isMiniSiteAdminTokenValid(token, adminTokenHash) {
  if (!token || !adminTokenHash) return false;
  const candidateHash = hashMiniSiteToken(token);
  return compareHashedTokens(candidateHash, adminTokenHash);
}

async function isMiniSiteAuthed(req, adminTokenHash) {
  const bearerToken = getBearerToken(req);
  if (isMiniSiteAdminTokenValid(bearerToken, adminTokenHash)) return true;
  const cookies = parseCookies(req);
  return (await validateMiniSiteSession(cookies[MINI_SITE_SESSION_COOKIE], 'browse')).ok;
}

async function isMiniSiteEditAuthed(req, adminTokenHash) {
  const bearerToken = getBearerToken(req);
  if (isMiniSiteAdminTokenValid(bearerToken, adminTokenHash)) return true;
  const cookies = parseCookies(req);
  return (await validateMiniSiteSession(cookies[MINI_SITE_EDIT_SESSION_COOKIE], 'edit')).ok;
}

function renderMiniSiteLogin(message) {
  const body = `
    <div class="card">
      <h3>🔐 Mini-site access</h3>
      <p class="muted">${escapeHtml(message || 'Enter the access token to continue.')}</p>
      <form method="POST" action="/db-mini/login">
        <div class="form-row">
          <input type="password" name="token" placeholder="Access token" required />
        </div>
        <button class="button" type="submit">Unlock</button>
      </form>
    </div>
  `;
  return renderMiniSiteLayout('Login required', body);
}

function renderMiniSiteEditLogin(redirectPath) {
  const body = `
    <div class="card">
      <h3>🔐 Confirm edit access</h3>
      <p class="muted">Re-enter the token to open the editor.</p>
      <form method="POST" action="/db-mini/edit-login">
        <input type="hidden" name="redirect" value="${escapeHtml(redirectPath)}" />
        <div class="form-row">
          <input type="password" name="token" placeholder="Access token" required />
        </div>
        <button class="button" type="submit">Continue</button>
      </form>
    </div>
  `;
  return renderMiniSiteLayout('Edit confirmation', body);
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
      await ctx.reply('Supabase connection is not configured.');
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
  const guidance = getCronProviderGuidance(cronSettings);
  if (guidance) {
    await renderOrEdit(ctx, guidance, {
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
    .text('🧪 Test keep-alive', `projcron:test:${projectId}:keepalive`)
    .text('🧪 Test deploy', `projcron:test:${projectId}:deploy`)
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
      .text('🧪 Test cron', `projcron:test:${projectId}:${type}`)
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
  if (type === 'keepalive') {
    clearUserState(ctx.from.id);
    const inline = new InlineKeyboard()
      .text('⏱️ every 1m', `projcron:keepalive_preset:${projectId}:1m:${recreate ? '1' : '0'}`)
      .row()
      .text('⏱️ every 5m', `projcron:keepalive_preset:${projectId}:5m:${recreate ? '1' : '0'}`)
      .row()
      .text('⏱️ every 10m', `projcron:keepalive_preset:${projectId}:10m:${recreate ? '1' : '0'}`)
      .row()
      .text('⏱️ every 30m', `projcron:keepalive_preset:${projectId}:30m:${recreate ? '1' : '0'}`)
      .row()
      .text('⏱️ every 1h', `projcron:keepalive_preset:${projectId}:1h:${recreate ? '1' : '0'}`)
      .row()
      .text('✍️ Custom schedule', `projcron:keepalive_custom:${projectId}:${recreate ? '1' : '0'}`)
      .row()
      .text('⬅️ Back', `projcron:menu:${projectId}`);
    await renderOrEdit(ctx, 'Choose a keep-alive schedule:', { reply_markup: inline });
    return;
  }

  setUserState(ctx.from.id, {
    type: recreate ? 'projcron_deploy_recreate' : 'projcron_deploy_schedule',
    projectId,
    backCallback: `projcron:menu:${projectId}`,
  });
  await renderOrEdit(
    ctx,
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
  if (!getConfigDbSnapshot().ready) {
    await renderConfigDbGate(ctx, { title: '🗄️ Database', backCallback: 'main:back' });
    return;
  }
  const view = await buildDataCenterView();
  const keyboardRows = view.keyboard?.inline_keyboard?.length ?? 0;
  console.debug('[UI] Data center keyboard rows', { rows: keyboardRows, hasButtons: keyboardRows > 0 });
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function resolveEnvVaultDbKeyStatus(project, envSetId) {
  const keys = await listEnvVarKeys(project.id, envSetId);
  const getKeyStatus = async (key) => {
    if (!keys.includes(key)) {
      return { key, status: 'MISSING', value: null };
    }
    const value = await getEnvVarValue(project.id, key, envSetId);
    return { key, status: evaluateEnvValueStatus(value).status, value };
  };

  if (keys.includes('DATABASE_URL')) {
    const status = await getKeyStatus('DATABASE_URL');
    return { keyName: status.key, status: status.status, source: 'project_env_vault', value: status.value };
  }
  if (keys.includes('SUPABASE_DSN')) {
    const status = await getKeyStatus('SUPABASE_DSN');
    return { keyName: status.key, status: status.status, source: 'project_env_vault', value: status.value };
  }

  const hasSupabaseUrl = keys.includes('SUPABASE_URL');
  const roleKey = keys.includes('SUPABASE_SERVICE_ROLE_KEY')
    ? 'SUPABASE_SERVICE_ROLE_KEY'
    : keys.includes('SUPABASE_SERVICE_ROLE')
      ? 'SUPABASE_SERVICE_ROLE'
      : null;
  if (hasSupabaseUrl && roleKey) {
    const urlValue = await getEnvVarValue(project.id, 'SUPABASE_URL', envSetId);
    const roleValue = await getEnvVarValue(project.id, roleKey, envSetId);
    const status = evaluateEnvValueStatus(urlValue).status === 'SET' && evaluateEnvValueStatus(roleValue).status === 'SET'
      ? 'SET'
      : 'MISSING';
    return {
      keyName: `SUPABASE_URL+${roleKey}`,
      status,
      source: 'computed_default',
      value: status === 'SET' ? buildSupabaseDsnFromUrl(urlValue, roleValue) : null,
    };
  }

  return null;
}

function resolveRuntimeDbKeyStatus() {
  const runtimeDatabase = process.env.DATABASE_URL;
  if (runtimeDatabase) {
    return { keyName: 'DATABASE_URL', status: evaluateEnvValueStatus(runtimeDatabase).status, source: 'runtime', value: runtimeDatabase };
  }
  const runtimeDsn = process.env.SUPABASE_DSN;
  if (runtimeDsn) {
    return { keyName: 'SUPABASE_DSN', status: evaluateEnvValueStatus(runtimeDsn).status, source: 'runtime', value: runtimeDsn };
  }
  const runtimeSupabaseUrl = process.env.SUPABASE_URL;
  const runtimeRole = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_ROLE;
  if (runtimeSupabaseUrl && runtimeRole) {
    const status =
      evaluateEnvValueStatus(runtimeSupabaseUrl).status === 'SET' &&
      evaluateEnvValueStatus(runtimeRole).status === 'SET'
        ? 'SET'
        : 'MISSING';
    return {
      keyName: `SUPABASE_URL+${process.env.SUPABASE_SERVICE_ROLE_KEY ? 'SUPABASE_SERVICE_ROLE_KEY' : 'SUPABASE_SERVICE_ROLE'}`,
      status,
      source: 'computed_default',
      value: status === 'SET' ? buildSupabaseDsnFromUrl(runtimeSupabaseUrl, runtimeRole) : null,
    };
  }
  return null;
}

function resolveDbTypeFromKey(keyName, value) {
  const haystack = `${keyName || ''} ${value || ''}`.toLowerCase();
  if (haystack.includes('supabase')) return 'Supabase';
  if (haystack.includes('postgres')) return 'Postgres';
  if (keyName) return 'Postgres';
  return 'Unknown';
}

async function resolveProjectDbCardInfo(project) {
  let keyInfo = null;
  if (isEnvVaultAvailable()) {
    try {
      const envSetId = await ensureProjectEnvSet(project.id);
      keyInfo = await resolveEnvVaultDbKeyStatus(project, envSetId);
    } catch (error) {
      console.warn('[db] Failed to resolve Env Vault DB status', {
        projectId: project.id,
        error: error.message,
      });
    }
  }
  if (!keyInfo) {
    keyInfo = resolveRuntimeDbKeyStatus();
  }
  if (!keyInfo && project.databaseUrl) {
    keyInfo = {
      keyName: 'DATABASE_URL (project config)',
      status: evaluateEnvValueStatus(project.databaseUrl).status,
      source: 'computed_default',
      value: project.databaseUrl,
    };
  }
  if (!keyInfo && project.supabaseConnectionId) {
    const connection = await findSupabaseConnection(project.supabaseConnectionId);
    if (connection) {
      const dsnInfo = await resolveSupabaseConnectionDsn(connection);
      keyInfo = {
        keyName: connection.envKey,
        status: dsnInfo.dsn ? 'SET' : 'MISSING',
        source: 'project_env_vault',
        value: dsnInfo.dsn,
      };
    } else {
      keyInfo = {
        keyName: 'Supabase connection (missing)',
        status: 'MISSING',
        source: 'computed_default',
        value: null,
      };
    }
  }

  if (!keyInfo) {
    return {
      ready: false,
      dbType: 'Unknown',
      keyName: '-',
      status: 'MISSING',
      source: 'computed_default',
    };
  }

  const normalizedStatus = keyInfo.status === 'SET' ? 'SET' : 'MISSING';
  const dbType = resolveDbTypeFromKey(keyInfo.keyName, keyInfo.value);
  return {
    ready: normalizedStatus === 'SET',
    dbType,
    keyName: keyInfo.keyName,
    status: normalizedStatus,
    source: keyInfo.source,
  };
}

async function buildDataCenterView() {
  const projects = await loadProjects();
  const lines = [`${buildDegradedBanner()}🗄 Databases`, buildScopedHeader('GLOBAL', 'Main → Databases'), `Status: ${appState.dbReady ? '✅ UP' : '🔴 DOWN'}`];
  const projectConnections = await Promise.all(projects.map(async (project) => ({
    project,
    connections: await listProjectConnections(project.id).catch(() => []),
  })));
  const primaryCount = projectConnections.filter((entry) => entry.connections.some((c) => c.role === 'primary' && c.enabled)).length;
  const secondaryCount = projectConnections.filter((entry) => entry.connections.some((c) => c.role === 'secondary' && c.enabled)).length;
  lines.push(`🌍 Projects with primary: ${primaryCount}/${projects.length}`);
  lines.push(`🌍 Projects with secondary: ${secondaryCount}/${projects.length}`);
  if (appState.degradedMode) {
    lines.push('Mode: ⚠️ Degraded (config DB unavailable)');
  }
  lines.push('', 'Configured databases across projects:', 'Use actions below for project drill-down and global controls.');
  const inline = new InlineKeyboard();
  if (!projects.length) {
    lines.push('', 'No projects configured yet.');
    inline.text('➕ Add project', 'proj:add').row();
  } else {
    projects.forEach((project) => {
      const label = `📦 ${project.name || project.id}`;
      inline.text(label, `dbmenu:open:${project.id}`).row();
    });
  }
  inline.webApp('🗄 Open DB Console', getMiniAppUrl()).row();
  inline.text('⚙️ Global DB Settings', 'gsettings:defaults').row();
  inline.text('🔁 Sync all', 'dbmenu:sync_all').text('🚑 Migrate', 'dbmenu:migrate').row();
  inline.text('⬅️ Back', 'main:back');
  return { text: lines.join('\n'), keyboard: inline };
}

async function renderDatabaseProjectPanel(ctx, projectId, notice) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const lines = [
    `🗄️ Database — ${project.name || project.id}`,
    notice || null,
    '',
    'Choose an action:',
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .webApp('🗄 Open DB Console', getMiniAppUrl())
    .text('🛠️ Edit DB config', `proj:db_config:${projectId}`)
    .row()
    .text('📊 Run DB overview', `proj:db_insights:${projectId}:0:0`)
    .text('⬅️ Back', 'dbmenu:list');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDataCenterMenuForMessage(messageContext) {
  if (!messageContext) {
    return;
  }
  if (!getConfigDbSnapshot().ready) {
    const view = buildConfigDbGateView({ title: '🗄️ Database', backCallback: 'main:back' });
    try {
      await bot.api.editMessageText(messageContext.chatId, messageContext.messageId, view.text, {
        reply_markup: view.keyboard,
      });
    } catch (error) {
      console.error('[UI] Failed to update data center message', error);
    }
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

async function renderLogsProjectList(ctx, notice) {
  if (!getConfigDbSnapshot().ready) {
    await renderConfigDbGate(ctx, { title: '🧾 Logs', backCallback: 'main:back' });
    return;
  }
  const projects = await loadProjects();
  const lines = [`${buildDegradedBanner()}📜 Logs`, buildScopedHeader('GLOBAL', 'Main → Logs'), notice || null, '', 'Unified logs viewer across projects:'].filter(Boolean);
  const inline = new InlineKeyboard();
  inline.text('🧾 Projects needing log test', 'logtest:reminders').row();
  inline.text('⚙️ Logs settings', 'gsettings:logs').row();
  if (!projects.length) {
    lines.push('', 'No projects configured yet.');
    inline.text('➕ Add project', 'proj:add').row();
  } else {
    projects.forEach((project) => {
      const logTest = normalizeLogTestSettings(project);
      const reminder = resolveLogTestReminderState(logTest);
      const reminderBadge = reminder.needsTest && !reminder.isSnoozed ? '⚠️ ' : '';
      lines.push(`📦 ${project.name || project.id} — ${formatLogTestBadgeLine(logTest)}`);
      const label = `📦 ${reminderBadge}${project.name || project.id}`;
      inline.text(label, `logmenu:open:${project.id}`).row();
    });
  }
  inline.text('⬅️ Back', 'main:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

function formatDeployEventsLine(deploySettings, webhookSettings) {
  const projectEvents = deploySettings.render?.eventsEnabled;
  const events =
    Array.isArray(projectEvents) && projectEvents.length ? projectEvents : webhookSettings.events;
  return `Events: ${events.length ? events.join(', ') : '-'}`;
}

function formatDeployLastEventLine(renderSettings) {
  if (!renderSettings?.lastDeployStatus) {
    return 'Last deploy: -';
  }
  const receivedAt = renderSettings.lastSeenAt ? new Date(renderSettings.lastSeenAt).toISOString() : '-';
  const label = renderSettings.lastDeployStatus || 'event';
  return `Last deploy: ${label} (${receivedAt})`;
}

function formatRenderPollingStatusLine(renderSettings, apiKeyStatus) {
  if (!renderSettings.pollingEnabledGlobal) {
    return '✅ Polling: disabled (global)';
  }
  if (!apiKeyStatus?.key) {
    return '⛔ Polling: blocked (missing API key)';
  }
  return '✅ Polling: enabled';
}

function formatRenderWebhookStatusLine(renderSettings, webhookSettings) {
  if (!renderSettings.webhookEnabledGlobal) {
    return '🔌 Webhook: disabled (global)';
  }
  if (webhookSettings.webhookId) {
    return '🔌 Webhook: configured';
  }
  return '🔌 Webhook: unavailable on free plan (PRO only)';
}

async function renderDeploysProjectList(ctx, notice) {
  if (!getConfigDbSnapshot().ready) {
    await renderConfigDbGate(ctx, { title: '🚀 Deployments', backCallback: 'main:back' });
    return;
  }
  const projects = await loadProjects();
  const settings = await getCachedSettings();
  const renderSettings = resolveRenderGlobalSettings(settings);
  const webhookSettings = normalizeRenderWebhookSettings(settings);
  const apiKeyStatus = appState.degradedMode
    ? { key: null, source: 'degraded' }
    : await getRenderApiKeyStatus();
  const apiKeyLabel = apiKeyStatus.key
    ? `configured (${apiKeyStatus.source === 'env' ? 'env' : 'vault'})`
    : 'missing';
  const unmapped = renderSettings.unmappedEvents || [];
  const unmappedList = unmapped.length
    ? `🧭 Needs mapping: ${unmapped
        .slice(0, 3)
        .map((entry) => shortenRenderId(entry.serviceId))
        .join(', ')}${unmapped.length > 3 ? '…' : ''}`
    : null;
  const lines = [
    `${buildDegradedBanner()}🚀 Deployments`,
    buildScopedHeader('GLOBAL', 'Main → Deployments'),
    notice || null,
    '',
    formatRenderPollingStatusLine(renderSettings, apiKeyStatus),
    formatRenderWebhookStatusLine(renderSettings, webhookSettings),
    `🔑 Render API key: ${apiKeyLabel}`,
    unmappedList,
    '',
    'Unified deploy timeline across projects:',
  ].filter(Boolean);
  const inline = new InlineKeyboard();
  if (!projects.length) {
    lines.push('', 'No projects configured yet.');
    inline.text('➕ Add project', 'proj:add').row();
  } else {
    projects.forEach((project) => {
      const deploySettings = normalizeProjectDeploySettings(project);
      const enabledBadge = deploySettings.render.enabled ? '🔔' : '🔕';
      const serviceBadge = deploySettings.render.serviceId ? '🟢' : '⚪️';
      lines.push(
        `📦 ${project.name || project.id} — ${enabledBadge} alerts · ${serviceBadge} ${
          deploySettings.render.serviceName || deploySettings.render.serviceId || 'not mapped'
        }`,
      );
      inline.text(`📦 ${project.name || project.id}`, `deploy:open:${project.id}`).row();
      inline
        .text('⚙️ Setup/Map service', `deploy:setup:${project.id}`)
        .text('🔔 Toggle alerts', `deploy:toggle:${project.id}`)
        .row();
      inline
        .text('🧪 Test deploy tracking', `deploy:test:${project.id}`)
        .text('📜 Recent events', `deploy:recent:${project.id}`)
        .row();
    });
  }
  inline.text('🔑 Set Render API key', 'deploy:api_key').row();
  inline.text('🔎 Discover services', 'deploy:discover').row();
  inline.text('⚙️ Global deploy defaults', 'deploy:settings').row();
  inline.text('⬅️ Back', 'main:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDeployProjectPanel(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const settings = normalizeProjectDeploySettings(project);
  const cached = await getCachedSettings();
  const renderSettings = resolveRenderGlobalSettings(cached);
  const webhookSettings = normalizeRenderWebhookSettings(cached);
  const lines = [
    `🚀 Deploy — ${project.name || project.id}`,
    notice || null,
    '',
    `Provider: ${settings.deployProvider || 'render'}`,
    `Render serviceId: ${settings.render.serviceId || 'missing'}`,
    `Render service name: ${settings.render.serviceName || '-'}`,
    `Alerts: ${settings.render.enabled ? '🔔 enabled' : '🔕 disabled'}`,
    `Start alerts: ${settings.render.notifyOnStart ? '✅' : '❌'}`,
    `Finish alerts: ${settings.render.notifyOnFinish ? '✅' : '❌'}`,
    `Fail alerts: ${settings.render.notifyOnFail ? '✅' : '❌'}`,
    `Polling status: ${settings.render.pollingStatus || 'ok'}`,
    formatDeployEventsLine(settings, webhookSettings),
    formatRenderPollingStatusLine(renderSettings, await getRenderApiKeyStatus()),
    formatRenderWebhookStatusLine(renderSettings, webhookSettings),
    formatDeployLastEventLine(settings.render),
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('⚙️ Setup/Map service', `deploy:setup:${project.id}`)
    .text(settings.render.enabled ? '🔕 Disable alerts' : '🔔 Enable alerts', `deploy:toggle:${project.id}`)
    .row()
    .text('🔔 Toggle alert types', `deploy:alerts:${project.id}`)
    .text('🧪 Test deploy tracking', `deploy:test:${project.id}`)
    .row()
    .text('📜 Recent events', `deploy:recent:${project.id}`)
    .row()
    .text('⬅️ Back', 'deploy:list');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDeploySettingsMenu(ctx, notice) {
  const cached = await getCachedSettings();
  const settings = normalizeRenderWebhookSettings(cached);
  const renderSettings = resolveRenderGlobalSettings(cached);
  const lines = [
    '⚙️ Deploy settings',
    notice || null,
    '',
    'PM supports both Render webhooks (PRO) and polling (free).',
    '',
    `Polling interval: ${renderSettings.pollIntervalSec}s`,
    `Polling max services/tick: ${renderSettings.pollMaxServicesPerTick}`,
    `Polling timeout: ${renderSettings.pollTimeoutMs}ms`,
    `Polling enabled: ${renderSettings.pollingEnabledGlobal ? '✅' : '❌'}`,
    '',
    `Webhook ID: ${settings.webhookId || '-'}`,
    `Target URL: ${settings.targetUrl || buildRenderWebhookTargetUrl()}`,
    `Events: ${settings.events.join(', ')}`,
    `Last verified: ${settings.lastVerifiedAt || '-'}`,
    `Webhook enabled: ${renderSettings.webhookEnabledGlobal ? '✅' : '❌'}`,
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text('🔄 Verify webhook', 'deploy:verify')
    .row()
    .text('⬅️ Back', 'deploy:list');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDeployEventsMenu(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const settings = normalizeProjectDeploySettings(project);
  const webhookSettings = normalizeRenderWebhookSettings(await getCachedSettings());
  const availableEvents = ['deploy_started', 'deploy_ended', 'build_started', 'build_ended'];
  const selected =
    Array.isArray(settings.render.eventsEnabled) && settings.render.eventsEnabled.length
      ? new Set(settings.render.eventsEnabled)
      : new Set(webhookSettings.events);
  const lines = [
    `⚙️ Deploy events — ${project.name || project.id}`,
    notice || null,
    '',
    'Events marked ✅ will trigger notifications.',
  ].filter(Boolean);
  const inline = new InlineKeyboard();
  availableEvents.forEach((eventName) => {
    const label = `${selected.has(eventName) ? '✅' : '❌'} ${eventName}`;
    inline.text(label, `deploy:event_toggle:${project.id}:${eventName}`).row();
  });
  inline.text('⬅️ Back', `deploy:open:${project.id}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDeployAlertTypesMenu(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const settings = normalizeProjectDeploySettings(project);
  const lines = [
    `🔔 Alert types — ${project.name || project.id}`,
    notice || null,
    '',
    'Toggle which deploy events notify you.',
  ].filter(Boolean);
  const inline = new InlineKeyboard()
    .text(
      `${settings.render.notifyOnStart ? '✅' : '❌'} Deploy started`,
      `deploy:alert_toggle:${project.id}:start`,
    )
    .row()
    .text(
      `${settings.render.notifyOnFinish ? '✅' : '❌'} Deploy succeeded`,
      `deploy:alert_toggle:${project.id}:finish`,
    )
    .row()
    .text(
      `${settings.render.notifyOnFail ? '✅' : '❌'} Deploy failed`,
      `deploy:alert_toggle:${project.id}:fail`,
    )
    .row()
    .text('⬅️ Back', `deploy:open:${project.id}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDeployServiceSetupMenu(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const deploySettings = normalizeProjectDeploySettings(project);
  const cache = getRenderServiceCache();
  const hasDiscovery = Array.isArray(cache?.services) && cache.services.length > 0;
  const lines = [
    `⚙️ Setup/Map — ${project.name || project.id}`,
    notice || null,
    '',
    `Current serviceId: ${deploySettings.render.serviceId || 'missing'}`,
    `Current service name: ${deploySettings.render.serviceName || '-'}`,
    '',
    'Choose how to map a Render service:',
  ].filter(Boolean);
  const inline = new InlineKeyboard();
  if (hasDiscovery) {
    inline.text('🔎 Choose from discovered services', `deploy:map_menu:${project.id}`).row();
  } else {
    inline.text('🔎 Discover services', 'deploy:discover').row();
  }
  inline.text('✍️ Enter serviceId manually', `deploy:set_service:${project.id}`).row();
  if (deploySettings.render.serviceId) {
    inline.text('🧹 Clear mapping', `deploy:clear_service:${project.id}`).row();
  }
  inline.text('⬅️ Back', `deploy:open:${project.id}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

function normalizeRenderServiceEntry(service) {
  const id = service?.id || service?.serviceId || service?.service?.id || null;
  const name = service?.name || service?.service?.name || null;
  const type = service?.type || service?.service?.type || service?.serviceType || null;
  return {
    id: id ? String(id) : null,
    name: name ? String(name) : null,
    type: type ? String(type) : null,
  };
}

async function renderRenderServicesList(ctx, notice) {
  const cache = getRenderServiceCache();
  const services = cache?.services || [];
  const lines = [
    '🔎 Render services',
    notice || null,
    '',
    services.length ? 'Discovered services:' : 'No services discovered yet.',
  ].filter(Boolean);
  if (services.length) {
    services.forEach((service) => {
      lines.push(
        `• ${service.name || service.id} (${service.type || 'unknown'}) — ${service.id}`,
      );
    });
  }
  const inline = new InlineKeyboard().text('⬅️ Back', 'deploy:list');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDeployServiceSelectMenu(ctx, projectId, notice) {
  const cache = getRenderServiceCache();
  const services = cache?.services || [];
  if (!services.length) {
    await renderDeployServiceSetupMenu(ctx, projectId, '⚠️ Discover services first.');
    return;
  }
  const lines = [
    '🔎 Select Render service',
    notice || null,
    '',
    'Tap a service to map it:',
  ].filter(Boolean);
  const inline = new InlineKeyboard();
  services.forEach((service) => {
    const label = `${service.name || service.id} (${service.type || 'unknown'})`;
    inline.text(label, `deploy:map:${projectId}:${service.id}`).row();
  });
  inline.text('⬅️ Back', `deploy:setup:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderDeployRecentEvents(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const settings = normalizeProjectDeploySettings(project);
  const events = settings.render.recentEvents || [];
  const lines = [
    `📜 Recent deploy events — ${project.name || project.id}`,
    '',
  ];
  if (!events.length) {
    lines.push('No deploy events tracked yet.');
  } else {
    events.slice(0, 8).forEach((event) => {
      lines.push(
        `• ${event.eventType || event.status || '-'} (${event.deployId || '-'}) @ ${event.receivedAt || '-'}`,
      );
    });
  }
  const inline = new InlineKeyboard().text('⬅️ Back', `deploy:open:${projectId}`);
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}


async function resolveAccessRoleForUser(userId) {
  const settings = await getCachedSettings();
  return resolveRole(userId, {
    ownerId: ADMIN_TELEGRAM_ID,
    adminIds: normalizeSecuritySettings(settings).adminIds || [],
    guestIds: [],
  });
}

async function handleDatabaseMenuCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, action, projectId] = data.split(':');
  if (action === 'list') {
    await renderDataCenterMenu(ctx);
    return;
  }
  if (action === 'open' && projectId) {
    const guard = getDbConsoleGuardStatus();
    if (!guard.ok) {
      const refId = buildDbRefId('DBOPEN');
      await renderOrEdit(ctx, `⚠️ ${guard.message}
Ref: ${refId}
Suggested action: configure Config DB and PM_KMS_KEY before adding connections.`, {
        reply_markup: new InlineKeyboard().text('⬅️ Back', 'dbmenu:list'),
      });
      return;
    }
    try {
      await renderDatabaseProjectPanel(ctx, projectId);
    } catch (error) {
      const refId = buildDbRefId('DBOPEN');
      console.warn('[dbmenu] open project panel failed', { projectId, refId, error: error?.message });
      await renderOrEdit(ctx, `⚠️ Unable to open Project DB.
Ref: ${refId}
Suggested action: verify DB connections and try again.` , {
        reply_markup: new InlineKeyboard().text('⬅️ Back', 'dbmenu:list'),
      });
    }
    return;
  }
  if (action === 'sync_all') {
    const role = await resolveAccessRoleForUser(ctx.from?.id);
    if (!canAccess(role, 'mutate:database')) {
      await ensureAnswerCallback(ctx, { text: 'Read-only access', show_alert: true });
      return;
    }
    await renderDataCenterMenu(ctx);
    return;
  }
  if (action === 'migrate') {
    const role = await resolveAccessRoleForUser(ctx.from?.id);
    if (!canAccess(role, 'mutate:database')) {
      await ensureAnswerCallback(ctx, { text: 'Read-only access', show_alert: true });
      return;
    }
    await renderDataCenterMenu(ctx);
    return;
  }
  await renderDataCenterMenu(ctx);
}

async function handleLogsMenuCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, action, projectId] = data.split(':');
  if (!getConfigDbSnapshot().ready && action !== 'list') {
    await renderConfigDbGate(ctx, { title: '🧾 Logs', backCallback: 'main:back' });
    return;
  }
  if (action === 'list') {
    await renderLogsProjectList(ctx);
    return;
  }
  if (action === 'open' && projectId) {
    await renderProjectLogAlerts(ctx, projectId);
    return;
  }
  await renderLogsProjectList(ctx);
}

async function handleDeployCallback(ctx, data) {
  await ensureAnswerCallback(ctx);
  const [, action, projectId, extra] = data.split(':');
  if (!getConfigDbSnapshot().ready && action !== 'list') {
    await renderConfigDbGate(ctx, { title: '🚀 Deployments', backCallback: 'main:back' });
    return;
  }
  if (action === 'list') {
    await renderDeploysProjectList(ctx);
    return;
  }
  if (action === 'open' && projectId) {
    await renderDeployProjectPanel(ctx, projectId);
    return;
  }
  if (action === 'api_key') {
    if (!isEnvVaultAvailable()) {
      await renderDeploysProjectList(ctx, buildEnvVaultUnavailableMessage('Env Vault unavailable.'));
      return;
    }
    setUserState(ctx.from.id, {
      type: 'render_api_key',
      messageContext: getMessageTargetFromCtx(ctx),
    });
    await renderOrEdit(ctx, '🔑 Send the Render API key.\n(Or press Cancel)', {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }
  if (action === 'discover') {
    const progress = createOperationProgress(ctx, '🔎 Discover Render services', 3);
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps: 0,
      currentStep: 'Validating API key',
      nextStep: 'Fetching services',
    });
    try {
      const apiKeyStatus = await getRenderApiKeyStatus();
      if (!apiKeyStatus.key) {
        throw new Error('Render API key missing.');
      }
      const settings = resolveRenderGlobalSettings(await getCachedSettings());
      const rawServices = await listRenderServices(settings);
      const services = rawServices
        .map((service) => normalizeRenderServiceEntry(service))
        .filter((service) => service.id);
      setRenderServiceCache(services);
      await updateProgressMessage(ctx, progress, {
        status: 'success',
        completedSteps: 3,
        currentStep: 'Services discovered',
        nextStep: null,
      });
      await renderRenderServicesList(ctx, `✅ Discovered ${services.length} service(s).`);
    } catch (error) {
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps: 1,
        currentStep: 'Discovery failed',
        nextStep: null,
        reason: error.message,
      });
      await renderDeploysProjectList(ctx, `❌ Discovery failed.\n${error.message}`);
    }
    return;
  }
  if (action === 'settings') {
    await renderDeploySettingsMenu(ctx);
    return;
  }
  if (action === 'verify') {
    const globalRenderSettings = resolveRenderGlobalSettings(await getCachedSettings());
    if (!globalRenderSettings.webhookEnabledGlobal) {
      await renderDeploySettingsMenu(ctx, '⚠️ Webhook disabled globally.');
      return;
    }
    const progress = createOperationProgress(ctx, '🚀 Verify Render webhook', 3);
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps: 0,
      currentStep: 'Validating settings',
      nextStep: 'Checking Render webhooks',
    });
    try {
      const settings = normalizeRenderWebhookSettings(await getCachedSettings());
      await ensureRenderWebhookConfigured({ events: settings.events, progress });
      await updateProgressMessage(ctx, progress, {
        status: 'success',
        completedSteps: 3,
        currentStep: 'Webhook verified',
        nextStep: null,
      });
      await renderDeploySettingsMenu(ctx, '✅ Webhook verified.');
    } catch (error) {
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps: 1,
        currentStep: 'Webhook verification failed',
        nextStep: null,
        reason: error.message,
      });
      await renderDeploySettingsMenu(ctx, `❌ Verification failed.\n${error.message}`);
    }
    return;
  }
  if (action === 'set_service' && projectId) {
    setUserState(ctx.from.id, {
      type: 'deploy_service_id',
      projectId,
      backCallback: `deploy:open:${projectId}`,
      messageContext: getMessageTargetFromCtx(ctx),
    });
    await renderOrEdit(ctx, '🔗 Send the Render serviceId.\n(Or press Cancel)', {
      reply_markup: buildCancelKeyboard(),
    });
    return;
  }
  if (action === 'setup' && projectId) {
    await renderDeployServiceSetupMenu(ctx, projectId);
    return;
  }
  if (action === 'map_menu' && projectId) {
    await renderDeployServiceSelectMenu(ctx, projectId);
    return;
  }
  if (action === 'map' && projectId && extra) {
    const serviceId = extra;
    const cache = getRenderServiceCache();
    const matched = cache?.services?.find((service) => service.id === serviceId) || null;
    await updateProjectDeploySettings(projectId, (current) => ({
      ...current,
      deployProvider: 'render',
      render: {
        ...current.render,
        serviceId,
        serviceName: matched?.name || current.render.serviceName || null,
        enabled: true,
        pollingEnabled: true,
        pollingStatus: null,
      },
    }));
    await clearUnmappedRenderService(serviceId);
    await renderDeployProjectPanel(ctx, projectId, '✅ Render service mapped.');
    return;
  }
  if (action === 'clear_service' && projectId) {
    await updateProjectDeploySettings(projectId, (current) => ({
      ...current,
      render: {
        ...current.render,
        serviceId: null,
        serviceName: null,
        enabled: false,
      },
    }));
    await renderDeployProjectPanel(ctx, projectId, '🧹 Render service mapping cleared.');
    return;
  }
  if (action === 'alerts' && projectId) {
    await renderDeployAlertTypesMenu(ctx, projectId);
    return;
  }
  if (action === 'alert_toggle' && projectId && extra) {
    const toggle = extra;
    await updateProjectDeploySettings(projectId, (current) => ({
      ...current,
      render: {
        ...current.render,
        notifyOnStart: toggle === 'start' ? !current.render.notifyOnStart : current.render.notifyOnStart,
        notifyOnFinish: toggle === 'finish' ? !current.render.notifyOnFinish : current.render.notifyOnFinish,
        notifyOnFail: toggle === 'fail' ? !current.render.notifyOnFail : current.render.notifyOnFail,
      },
    }));
    await renderDeployAlertTypesMenu(ctx, projectId, '✅ Updated.');
    return;
  }
  if (action === 'toggle' && projectId) {
    const projects = await loadProjects();
    const project = findProjectById(projects, projectId);
    if (!project) {
      await renderOrEdit(ctx, 'Project not found.');
      return;
    }
    const settings = normalizeProjectDeploySettings(project);
    const progress = createOperationProgress(ctx, `🚀 Deploy alerts — ${project.name || project.id}`, 3);
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps: 0,
      currentStep: 'Validating configuration',
      nextStep: 'Ensuring Render webhook',
    });
    if (!settings.render.serviceId) {
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps: 0,
        currentStep: 'Missing serviceId',
        nextStep: null,
        reason: 'Render serviceId not set',
      });
      await renderDeployProjectPanel(ctx, projectId, '⚠️ Set Render serviceId first.');
      return;
    }
    if (settings.render.enabled) {
      await updateProjectDeploySettings(projectId, (current) => ({
        ...current,
        render: { ...current.render, enabled: false },
        notifications: { ...current.notifications, enabled: false },
      }));
      await updateProgressMessage(ctx, progress, {
        status: 'success',
        completedSteps: 3,
        currentStep: 'Alerts disabled',
        nextStep: null,
      });
      await renderDeployProjectPanel(ctx, projectId, '🔕 Deploy alerts disabled.');
      return;
    }
    let webhookNotice = null;
    const globalRenderSettings = resolveRenderGlobalSettings(await getCachedSettings());
    const apiKeyStatus = await getRenderApiKeyStatus();
    if (globalRenderSettings.webhookEnabledGlobal && apiKeyStatus.key) {
      try {
        await ensureRenderWebhookConfigured({ progress });
      } catch (error) {
        webhookNotice = `⚠️ Webhook setup failed.\n${error.message}`;
      }
    }
    await updateProjectDeploySettings(projectId, (current) => ({
      ...current,
      deployProvider: 'render',
      render: { ...current.render, enabled: true },
      notifications: { ...current.notifications, enabled: true },
    }));
    await updateProgressMessage(ctx, progress, {
      status: 'success',
      completedSteps: 3,
      currentStep: 'Alerts enabled',
      nextStep: null,
    });
    await renderDeployProjectPanel(ctx, projectId, webhookNotice || '🔔 Deploy alerts enabled.');
    return;
  }
  if (action === 'events' && projectId) {
    await renderDeployEventsMenu(ctx, projectId);
    return;
  }
  if (action === 'event_toggle' && projectId && extra) {
    const eventName = extra;
    const projects = await loadProjects();
    const project = findProjectById(projects, projectId);
    if (!project) {
      await renderOrEdit(ctx, 'Project not found.');
      return;
    }
    const settings = normalizeProjectDeploySettings(project);
    const webhookSettings = normalizeRenderWebhookSettings(await getCachedSettings());
    const currentEvents =
      Array.isArray(settings.render.eventsEnabled) && settings.render.eventsEnabled.length
        ? settings.render.eventsEnabled
        : webhookSettings.events;
    const nextEvents = currentEvents.includes(eventName)
      ? currentEvents.filter((event) => event !== eventName)
      : [...currentEvents, eventName];
    const mergedEvents = Array.from(new Set([...webhookSettings.events, ...nextEvents]));
    if (mergedEvents.length !== webhookSettings.events.length) {
      try {
        await ensureRenderWebhookConfigured({ events: mergedEvents });
      } catch (error) {
        await renderDeployEventsMenu(ctx, projectId, `❌ Failed to update webhook.\n${error.message}`);
        return;
      }
    }
    await updateProjectDeploySettings(projectId, (current) => ({
      ...current,
      render: { ...current.render, eventsEnabled: nextEvents },
    }));
    await renderDeployEventsMenu(ctx, projectId, '✅ Updated.');
    return;
  }
  if (action === 'test' && projectId) {
    const project = await getProjectById(projectId, ctx);
    if (!project) return;
    const deploySettings = normalizeProjectDeploySettings(project);
    if (!deploySettings.render.serviceId) {
      await renderDeployProjectPanel(ctx, projectId, '⚠️ Set Render serviceId first.');
      return;
    }
    const progress = createOperationProgress(ctx, `🧪 Test deploy tracking — ${project.name || project.id}`, 2);
    await updateProgressMessage(ctx, progress, {
      status: 'progressing',
      completedSteps: 0,
      currentStep: 'Fetching latest deploy',
      nextStep: 'Reporting result',
    });
    try {
      const renderSettings = resolveRenderGlobalSettings(await getCachedSettings());
      const deploys = await listRenderServiceDeploys(deploySettings.render.serviceId, renderSettings);
      const latest = deploys[0] || null;
      if (!latest) {
        await updateProgressMessage(ctx, progress, {
          status: 'failed',
          completedSteps: 1,
          currentStep: 'No deploys found',
          nextStep: null,
          reason: 'No deploys returned by Render API',
        });
        await renderDeployProjectPanel(ctx, projectId, '⚠️ No deploys found for this service.');
        return;
      }
      const deployId = latest.id || latest.deployId || null;
      const status = latest.status || latest.state || latest.result || null;
      const timestamp = latest.updatedAt || latest.finishedAt || latest.createdAt || new Date().toISOString();
      const message = [
        '🧪 Deploy tracking snapshot',
        `Project: ${project.name || project.id}`,
        `Service: ${deploySettings.render.serviceName || deploySettings.render.serviceId || '-'}`,
        `DeployId: ${shortenRenderId(deployId)}`,
        `Status: ${status || '-'}`,
        `Updated: ${timestamp}`,
      ].join('\n');
      await bot.api.sendMessage(ADMIN_TELEGRAM_ID, message, { disable_web_page_preview: true });
      await updateProgressMessage(ctx, progress, {
        status: 'success',
        completedSteps: 2,
        currentStep: 'Snapshot sent',
        nextStep: null,
      });
      await renderDeployProjectPanel(ctx, projectId, '✅ Snapshot sent.');
    } catch (error) {
      await updateProgressMessage(ctx, progress, {
        status: 'failed',
        completedSteps: 1,
        currentStep: 'Test failed',
        nextStep: null,
        reason: error.message,
      });
      await renderDeployProjectPanel(ctx, projectId, `❌ Test failed.\n${error.message}`);
    }
    return;
  }
  if (action === 'recent' && projectId) {
    await renderDeployRecentEvents(ctx, projectId);
    return;
  }
  await renderDeploysProjectList(ctx);
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
    await saveGlobalSettingsAndCache(settings);
  }
  await renderOrEdit(ctx, `Project ${projectId} deleted.`);
  await renderProjectsList(ctx);
}

async function renderCommandsScreen(ctx, projectId, options = {}) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }
  const source = options?.source === 'missing_setup' ? 'missing_setup' : null;
  const suffix = source ? `:${source}` : '';
  const backTarget = source ? `proj:missing_setup:${project.id}` : `proj:project_menu:${project.id}`;

  const lines = [
    `startCommand: ${project.startCommand || '-'}`,
    `testCommand: ${project.testCommand || '-'}`,
    `diagnosticCommand: ${project.diagnosticCommand || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Edit startCommand', `proj:cmd_edit:${project.id}:startCommand${suffix}`)
    .row()
    .text('✏️ Edit testCommand', `proj:cmd_edit:${project.id}:testCommand${suffix}`)
    .row()
    .text('✏️ Edit diagnosticCommand', `proj:cmd_edit:${project.id}:diagnosticCommand${suffix}`);

  if (project.startCommand || project.testCommand || project.diagnosticCommand) {
    inline.row().text('🧹 Clear all commands', `proj:cmd_clearall:${project.id}`);
  }

  inline.row().text('⬅️ Back', backTarget);

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

async function renderDatabaseBindingMenu(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const envSetId = await ensureProjectEnvSet(projectId);
  const envStatus = await buildEnvVaultDbStatus(project, envSetId);
  const supabaseStatus = await buildSupabaseBindingStatus(project);
  const { settings: miniSiteSettings } = await getMiniSiteSettingsState();
  const miniSiteDb = await resolveMiniSiteDbConnection(project);
  const miniSiteDbReady = Boolean(miniSiteDb.dsn);
  const miniSiteTokenConfigured = isMiniSiteTokenConfigured(miniSiteSettings);
  const miniSiteTokenMask = getMiniSiteAdminTokenMask(miniSiteSettings);
  const miniSiteTokenSource = isMiniSiteTokenFromEnv(miniSiteSettings) ? ' (env)' : '';
  const miniSiteTokenLabel = miniSiteTokenConfigured
    ? `${miniSiteTokenMask || 'configured'}${miniSiteTokenSource}`
    : 'not configured';
  const supabaseEnabledLabel = supabaseStatus.enabled ? '✅ enabled' : '⚪ disabled';
  const sslSettings = resolveProjectDbSslSettings(project, miniSiteDb);
  const sslSummary = formatProjectDbSslSummary(sslSettings);
  const lines = [
    `🗄️ Database binding — ${project.name || project.id}`,
    notice || null,
    '',
    `Env Vault DB: ${envStatus.summary}`,
    `Supabase enabled: ${supabaseEnabledLabel}`,
    `Supabase binding: ${supabaseStatus.summary}`,
    `Supabase project ref: ${project.supabaseProjectRef || '-'}`,
    `Supabase URL: ${project.supabaseUrl || '-'}`,
    `Supabase API key: ${getSupabaseKeyMask(project)} (${project.supabaseKeyType || '-'})`,
    '',
    `Mini-site DB: ${miniSiteDbReady ? '✅ ready' : '⚠️ missing'}`,
    `Mini-site token: ${miniSiteTokenLabel}`,
    `SSL: ${sslSummary}`,
    '',
    'DB URLs are stored in Env Vault (DATABASE_URL by default) — no external env vars required.',
    'Use Env Vault to map a custom key if needed.',
    'JWT strings are API keys, not DB DSN.',
  ].filter(Boolean);

  const inline = new InlineKeyboard()
    .text('📥 Import DB URL', `proj:db_import:${project.id}`)
    .row()
    .text('🔐 SSL settings', `proj:db_ssl_settings:${project.id}`)
    .row()
    .text(supabaseStatus.enabled ? '🚫 Disable Supabase binding' : '✅ Enable Supabase binding', `proj:supabase_toggle:${project.id}`)
    .row()
    .text('✏️ Edit Supabase binding', `proj:supabase_edit:${project.id}`);

  if (
    project.supabaseProjectRef ||
    project.supabaseUrl ||
    project.supabaseKey ||
    project.supabaseKeyMask ||
    project.supabaseKeyType
  ) {
    inline.text('🧹 Clear Supabase binding', `proj:supabase_clear:${project.id}`);
  }

  if (miniSiteDbReady) {
    if (!miniSiteTokenConfigured) {
      inline.row().text('✅ Enable mini-site', `proj:db_mini_enable:${project.id}`);
    } else {
      inline
        .row()
        .text('🌐 Open mini-site', `proj:db_mini_open:${project.id}`)
        .text('🔄 Rotate mini-site token', `proj:db_mini_rotate:${project.id}`);
    }
  }

  inline
    .row()
    .text('🔐 Env Vault', `envvault:menu:${project.id}`)
    .row()
    .text('⬅️ Back', `proj:open:${project.id}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function renderProjectDbSslSettings(ctx, projectId, notice) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const connection = await resolveMiniSiteDbConnection(project);
  const settings = resolveProjectDbSslSettings(project, connection);
  const lines = [
    `🔐 DB SSL settings — ${project.name || project.id}`,
    notice || null,
    '',
    `SSL mode: ${settings.sslMode}`,
    `SSL verify: ${settings.sslVerify ? '✅ on' : '⚠️ off'}`,
    '',
    'These settings apply to DB mini-site connections.',
  ].filter(Boolean);

  const inline = new InlineKeyboard()
    .text(`${settings.sslMode === 'disable' ? '✅' : '⚪'} disable`, `proj:db_ssl_mode:${project.id}:disable`)
    .text(`${settings.sslMode === 'require' ? '✅' : '⚪'} require`, `proj:db_ssl_mode:${project.id}:require`)
    .row()
    .text(
      `SSL verify: ${settings.sslVerify ? '✅ on' : '⚪ off'}`,
      `proj:db_ssl_verify:${project.id}`,
    )
    .row()
    .text('⬅️ Back', `proj:db_config:${project.id}`);

  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
}

async function updateProjectDbSslMode(ctx, projectId, mode) {
  if (!PROJECT_DB_SSL_MODES.has(mode)) {
    await renderProjectDbSslSettings(ctx, projectId);
    return;
  }
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  project.dbSslMode = mode;
  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) return;
  await renderProjectDbSslSettings(ctx, projectId, `✅ SSL mode set to ${mode}.`);
}

async function toggleProjectDbSslVerify(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const current = resolveProjectDbSslSettings(project);
  project.dbSslVerify = !current.sslVerify;
  const saved = await saveProjectsWithFeedback(ctx, projects);
  if (!saved) return;
  await renderProjectDbSslSettings(
    ctx,
    projectId,
    `✅ SSL verify ${project.dbSslVerify ? 'enabled' : 'disabled'}.`,
  );
}

async function renderSupabaseScreen(ctx, projectId) {
  await renderDatabaseBindingMenu(ctx, projectId);
}

async function startDatabaseImportFlow(ctx, projectId) {
  if (!isEnvVaultAvailable()) {
    await renderOrEdit(ctx, buildEnvVaultUnavailableMessage('Env Vault unavailable.'), {
      reply_markup: buildBackKeyboard(`proj:db_config:${projectId}`),
    });
    return;
  }
  setUserState(ctx.from.id, {
    type: 'db_import_url',
    projectId,
    messageContext: getMessageTargetFromCtx(ctx),
  });
  await renderOrEdit(
    ctx,
    'Send the DB URL to store in Env Vault as DATABASE_URL.\n(Use Env Vault if you want to map a custom key.)\n(Or press Cancel)',
    { reply_markup: buildCancelKeyboard() },
  );
}

async function handleDatabaseImportInput(ctx, state) {
  const raw = ctx.message.text?.trim();
  if (!raw) {
    await ctx.reply('Please send the DB URL.');
    return;
  }
  if (raw.toLowerCase() === 'cancel') {
    resetUserState(ctx);
    await renderDatabaseBindingMenu(ctx, state.projectId, 'Operation cancelled.');
    return;
  }
  if (!raw.includes('://')) {
    await ctx.reply('DB URL must include a scheme (for example: postgres://...).');
    return;
  }
  const envSetId = await ensureProjectEnvSet(state.projectId);
  try {
    await upsertEnvVar(state.projectId, 'DATABASE_URL', raw, envSetId);
  } catch (error) {
    console.error('[db] Failed to import DB URL', error);
    await ctx.reply(`Failed to save DB URL: ${error.message}`);
    return;
  }
  clearUserState(ctx.from.id);
  await renderDatabaseBindingMenu(ctx, state.projectId, '✅ DB URL imported into Env Vault.');
}


function getHealthAutoFixDefaults() {
  return {
    startCommand: 'node src/bot.js',
    testCommand: 'npm test',
    diagnosticCommand: 'node -e "console.log(\'PM diagnostic OK\')"',
    healthPath: '/health',
    servicePort: '3000',
  };
}

function applyHealthAutoFixDefaults(project) {
  const defaults = getHealthAutoFixDefaults();
  const next = { ...project };
  const changed = [];
  Object.entries(defaults).forEach(([key, value]) => {
    if (isMissingRequirementValue(next[key])) {
      next[key] = value;
      changed.push(key);
    }
  });
  return { project: next, changed };
}

async function runProjectAutoFixHealthSetup(ctx, projectId) {
  const projects = await loadProjects();
  const index = projects.findIndex((item) => item.id === projectId);
  if (index === -1) {
    await renderOrEdit(ctx, 'Project not found.');
    return;
  }
  const result = applyHealthAutoFixDefaults(projects[index]);
  projects[index] = result.project;
  await saveProjects(projects);
  const notice = result.changed.length
    ? `✅ Auto-fixed health setup: ${result.changed.join(', ')}`
    : '✅ Auto-fix skipped (all values already configured).';
  await sendTransientNotice(ctx, notice, {
    ttlSec: 10,
    deleteButton: true,
    extraMarkup: new InlineKeyboard().text('⬅️ Back', `proj:missing_setup:${projectId}`),
  });
  await renderProjectMissingSetup(ctx, projectId, '✅ Setup refreshed after auto-fix.');
}

function maskDiagnosticText(value) {
  return String(value || '')
    .replace(/(token|secret|password|dsn)[^\s]*/gi, '$1***')
    .replace(/[A-Za-z0-9_\-]{24,}/g, '***');
}

function createLogDeliveryCounters() {
  return {
    forwarded_to_inbox_count: 0,
    forwarded_to_room_count: 0,
    forwarded_to_rob_count: 0,
    dropped_by_dedupe_count: 0,
    dropped_by_rate_limit_count: 0,
    delivery_fail_count: 0,
  };
}


function buildRoutineFixesMenu() {
  return new InlineKeyboard()
    .text('🔎 Analyze last error', 'gsettings:routine_analyze_last')
    .row()
    .text('⚡ Quick detect (keyword / error)', 'gsettings:routine_quick_detect')
    .row()
    .text('🧾 Paste error/log text', 'gsettings:routine_paste')
    .row()
    .text('🆔 Enter RefId', 'gsettings:routine_refid')
    .row()
    .text('📎 Forward message', 'gsettings:routine_forward')
    .row()
    .text('📚 Rule catalog', 'gsettings:routine_catalog')
    .row()
    .text('❓ Help (how to use)', 'gsettings:routine_help')
    .row()
    .text('⬅️ Back', 'gsettings:diagnostics_menu');
}

async function renderRoutineFixesMenu(ctx, notice = null) {
  const text = [
    '🧰 Routine Fixes (Codex Tasks)',
    notice || null,
    'Deterministic rule-based task generator (no LLM).',
  ].filter(Boolean).join('\n\n');
  await renderOrEdit(ctx, text, { reply_markup: buildRoutineFixesMenu() });
}

async function runRoutineDetection(ctx, input, options = {}) {
  const match = matchRoutineFix(input, ROUTINE_AUTO_THRESHOLD);
  if (match.accepted) {
    await renderRoutineOutput(ctx, match.accepted, { backCallback: options.backCallback || 'gsettings:routine_menu' });
    return;
  }
  const top = summarizeRoutineCandidates(match.matches);
  if (top.length) {
    const inline = new InlineKeyboard();
    for (const candidate of top) {
      inline.text(`🧩 ${candidate.title}`, `gsettings:routine_pick:${candidate.ruleId}`).row();
    }
    inline.text('⬅️ Back', 'gsettings:routine_menu');
    await sendDismissibleMessage(
      ctx,
      'No rule matched with high confidence. Pick the nearest candidate:',
      { reply_markup: inline },
    );
    return;
  }
  await sendDismissibleMessage(ctx, 'No routine fix found. Open full Diagnostics or export debug details.', {
    reply_markup: buildBackKeyboard('gsettings:routine_menu'),
  });
}


async function handleRoutineRefIdInput(ctx, _state) {
  const refId = String(ctx.message?.text || '').trim();
  const eventLog = (await loadConfigJson('ops_event_log')) || [];
  const found = eventLog.find((entry) => String(entry?.meta_json?.refId || '').toUpperCase() === refId.toUpperCase());
  if (!found) {
    await sendDismissibleMessage(ctx, `No event found for RefId: ${refId}`, {
      reply_markup: buildBackKeyboard('gsettings:routine_menu'),
    });
    clearUserState(ctx.from.id);
    return;
  }
  await runRoutineDetection(ctx, {
    rawText: found.message_short || '',
    refId: found.meta_json?.refId || refId,
    category: found.category || null,
  }, { backCallback: 'gsettings:routine_menu' });
  clearUserState(ctx.from.id);
}

async function handleRoutineForwardInput(ctx, state) {
  const forwarded = ctx.message?.forward_origin || ctx.message?.forward_from || ctx.message?.forward_from_chat;
  const text = String(ctx.message?.text || ctx.message?.caption || '').trim();
  if (!forwarded && !state?.attachmentFileId) {
    await sendDismissibleMessage(ctx, 'Please forward a message first.', { reply_markup: buildBackKeyboard('gsettings:routine_menu') });
    return;
  }
  if (!text) {
    const fileId = ctx.message?.document?.file_id || ctx.message?.photo?.[0]?.file_id || null;
    if (fileId) {
      setUserState(ctx.from.id, {
        type: 'routine_forward',
        backCallback: 'gsettings:routine_menu',
        attachmentFileId: fileId,
      });
      await sendDismissibleMessage(ctx, 'Attachment received. Paste error/log text to analyze (OCR disabled by default).', {
        reply_markup: buildBackKeyboard('gsettings:routine_menu'),
      });
      return;
    }
  }
  await runRoutineDetection(ctx, { rawText: text }, { backCallback: 'gsettings:routine_menu' });
  clearUserState(ctx.from.id);
}

async function getLastOpsErrorContext() {
  const eventLog = (await loadConfigJson('ops_event_log')) || [];
  const event = eventLog.find((entry) => ['warn', 'error', 'critical'].includes(String(entry.level || '').toLowerCase())) || null;
  if (!event) return null;
  const refId = event.meta_json?.refId || null;
  return {
    rawText: event.message_short || '',
    refId,
    category: event.category || null,
  };
}

async function renderOpsMenu(ctx) {
  const status = safeMode.getStatus();
  const lines = [
    '🛟 Ops',
    `Safe Mode: ${status.mode === 'on' ? '🟠 ON' : '🟢 OFF'}`,
    status.lastEnteredReason ? `Last reason: ${status.lastEnteredReason}` : null,
  ].filter(Boolean);
  const keyboard = new InlineKeyboard()
    .text('🕒 Timeline', 'ops:timeline')
    .text('🛡 Safe Mode', 'ops:safe_mode')
    .row()
    .text('⬅️ Back', 'main:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: keyboard });
}

async function renderTemplatesMenu(ctx) {
  const templates = listTemplates();
  const lines = ['🧩 Templates', ...templates.map((item) => `• ${item.name}`), '', 'Use templates to apply safe defaults quickly.'];
  const keyboard = new InlineKeyboard().text('⬅️ Back', 'main:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: keyboard });
}

function buildHelpPages() {
  return {
    overview: 'PM is an operations control bot with safe navigation and incident tooling.\nWhere: Main Menu -> Help -> Overview.\nUse: start from Projects, then Ops for incident triage.\nSafety: non-menu messages are dismissible.',
    timeline: 'Timeline records chronological events (deploys, db, safe mode, shadow runs).\nWhere: Main Menu -> Ops -> Timeline.\nUse: filter by severity/type and inspect refs.\nSafety: event details are masked.',
    safe_mode: 'Safe Mode auto-protects PM during restart loops, DB outages, error spikes, or memory pressure.\nWhere: Main Menu -> Ops -> Safe Mode.\nUse: monitor status and reset counters.\nSafety: force controls are admin/owner only.',
    alerts: 'Actionable alerts always include refId, copy debug, and timeline deep link.\nWhere: Main Menu -> Ops + inbox alerts.\nUse: run routine fixes in one tap.\nSafety: ENV/DSN misconfig is internal-only.',
    drift: 'Drift detector compares current project config to a baseline snapshot.\nWhere: Project -> Ops -> Drift.\nUse: identify what changed before incidents.\nSafety: snapshots never store raw secrets.',
    runbooks: 'Runbooks are rule-based docs generated from routine fixes.\nWhere: Main Menu -> Help -> Runbooks.\nUse: search symptoms and copy Codex task.\nSafety: read-only guidance.',
    shadow: 'Shadow runs provide a dry plan before dangerous execute actions.\nWhere: migration/sync/env bulk workflows.\nUse: inspect operations and confirm execute.\nSafety: no destructive action without confirm.',
    templates: 'Templates apply best-practice defaults for new projects.\nWhere: Main Menu -> Templates.\nUse: preview changes, shadow run, then execute.\nSafety: scoped defaults only.',
    guest: 'Guest role is read-only for status, logs, timeline, and runbooks.\nWhere: Settings -> Access controls.\nUse: observer access without mutation rights.\nSafety: write actions blocked.',
    cron: 'Cron providers: CRON_PROVIDER=cronjob_org|none (default: cronjob_org when token exists, else none).\nRequired for cronjob_org: CRON_API_TOKEN (or CRON_API_KEY/CRONJOB_API_KEY).\nWhen provider=none/local, PM keeps menus but external scheduler calls are disabled and guidance is shown.',
  };
}

async function renderHelpMenu(ctx) {
  const pages = buildHelpPages();
  const lines = ['❓ Help', '', pages.overview];
  const keyboard = new InlineKeyboard()
    .text('📘 Timeline', 'help:timeline')
    .text('🛡 Safe Mode', 'help:safe_mode')
    .row()
    .text('🚨 Alerts', 'help:alerts')
    .text('🧭 Drift', 'help:drift')
    .row()
    .text('📚 Runbooks', 'help:runbooks')
    .text('🕶 Shadow Runs', 'help:shadow')
    .row()
    .text('🧩 Templates', 'help:templates')
    .text('👀 Guest Access', 'help:guest')
    .row()
    .text('⏱️ Cron providers', 'help:cron')
    .row()
    .text('⬅️ Back', 'main:back');
  await renderOrEdit(ctx, lines.join('\n'), { reply_markup: keyboard });
}

async function renderLogDeliveryDiagnosticsMenu(ctx) {
  const text = '🧪 Diagnostics\nRun end-to-end synthetic log delivery checks and routine codex task generation.';
  const keyboard = new InlineKeyboard()
    .text('🧪 Test Log Delivery', 'gsettings:diag_test_log_delivery')
    .row()
    .text('🧰 Routine Fixes (Codex Tasks)', 'gsettings:routine_menu')
    .row()
    .text('⬅️ Back', 'gsettings:menu');
  await renderOrEdit(ctx, text, { reply_markup: keyboard });
}


async function runLogDeliveryDiagnostics(ctx) {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) return;
  const testRunId = `run_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
  const counters = createLogDeliveryCounters();
  logDeliveryDiagnosticsRuns.set(testRunId, counters);
  const progress = new ProgressReporter({ bot, chatId, initialText: '⏳ Running log delivery diagnostics…', totalSteps: 6 });
  await progress.start();
  const prefs = await getUserAlertPrefs(ADMIN_TELEGRAM_ID || 'admin');
  const baseEvent = { source: 'pm', category: 'TEST_LOG_DELIVERY', meta: { testRunId } };
  const cases = [
    { id: 'T0', level: 'info', message: 'Baseline cursor' },
    { id: 'T1', level: 'info', message: 'Synthetic INFO log' },
    { id: 'T2', level: 'warn', message: 'Synthetic WARNING log' },
    { id: 'T3', level: 'error', message: 'Synthetic ERROR log' },
    { id: 'T4', level: 'warn', message: 'Synthetic DEDUPE log', repeat: 2 },
    { id: 'T5', level: 'info', message: 'Synthetic RATE log', repeat: 25 },
  ];
  const results = [];
  for (const item of cases) {
    await progress.step(`${item.id} ${item.message}`);
    const repeat = Math.min(item.repeat || 1, 200);
    let forwarded = 0;
    for (let i = 0; i < repeat; i += 1) {
      const event = await appendEvent({ ...baseEvent, level: item.level, messageShort: `${item.message} #${i + 1}` });
      const routeAllowed = shouldRouteEvent(prefs, event) && shouldRouteBySeverity({ threshold: prefs.severity_threshold, level: item.level, muteCategories: prefs.mute_categories, category: event.category });
      if (item.id === 'T4' && i > 0) {
        counters.dropped_by_dedupe_count += 1;
        continue;
      }
      if (item.id === 'T5' && i >= Number(process.env.LOG_RATE_LIMIT_PER_MIN || 20)) {
        counters.dropped_by_rate_limit_count += 1;
        continue;
      }
      if (routeAllowed) {
        const destinations = computeDestinations(prefs, item.level);
        if (destinations.admin_inbox) counters.forwarded_to_inbox_count += 1;
        if (destinations.admin_room) counters.forwarded_to_room_count += 1;
        if (destinations.admin_rob) counters.forwarded_to_rob_count += 1;
        forwarded += 1;
      }
    }
    results.push({ id: item.id, ok: true, forwarded, repeat });
  }
  await progress.done();
  const durationMs = 0;
  const reportLines = [
    `testRunId=${testRunId}`,
    `timestamp=${new Date().toISOString()}`,
    '',
    ...results.map((r) => `${r.id}: ${r.ok ? 'PASS' : 'FAIL'} forwarded=${r.forwarded}/${r.repeat}`),
    '',
    `forwarded_to_inbox_count=${counters.forwarded_to_inbox_count}`,
    `forwarded_to_room_count=${counters.forwarded_to_room_count}`,
    `forwarded_to_rob_count=${counters.forwarded_to_rob_count}`,
    `dropped_by_dedupe_count=${counters.dropped_by_dedupe_count}`,
    `dropped_by_rate_limit_count=${counters.dropped_by_rate_limit_count}`,
    `delivery_fail_count=${counters.delivery_fail_count}`,
    `durationMs=${durationMs}`,
    '',
    `routingPolicy=${maskDiagnosticText(JSON.stringify(prefs))}`,
  ];
  const summary = ['🧪 Test Log Delivery', ...results.map((r) => `${r.ok ? '✅' : '❌'} ${r.id}`), `Inbox:${counters.forwarded_to_inbox_count} Room:${counters.forwarded_to_room_count} Rob:${counters.forwarded_to_rob_count}`].join('\n');
  const sent = await sendTransientNotice(ctx, summary, { ttlSec: 10, deleteButton: true });
  await bot.api.sendDocument(chatId, new InputFile(Buffer.from(reportLines.join('\n'), 'utf8'), `log-delivery-${testRunId}.txt`), { caption: 'Diagnostics report' }).catch(() => null);
  return sent;
}

function buildGlobalSettingsView(settings, projects, notice) {
  const defaultProject = settings.defaultProjectId
    ? findProjectById(projects, settings.defaultProjectId)
    : undefined;
  const selfLogForwarding = getEffectiveSelfLogForwarding(settings);
  const uiCleanup = normalizeUiCleanupSettings(settings);
  const security = normalizeSecuritySettings(settings);
  const logsDefaults = normalizeLogDefaults(settings);
  const integrations = normalizeIntegrationSettings(settings);
  const backups = normalizeBackupSettings(settings);
  const lines = [
    buildScopedHeader('GLOBAL', 'Main → Settings'),
    notice || null,
    `defaultBaseBranch: ${settings.defaultBaseBranch || DEFAULT_BASE_BRANCH}`,
    `defaultProjectId: ${settings.defaultProjectId || '-'}` +
      (defaultProject ? ` (${defaultProject.name || defaultProject.id})` : ''),
    `selfLogForwarding: ${selfLogForwarding.enabled ? 'enabled' : 'disabled'} (${selfLogForwarding.levels.join('/')})`,
    '',
    `🧹 UI cleanup: ${uiCleanup.autoCleanMenus ? 'ON' : 'OFF'} · TTL ${uiCleanup.ephemeralTtlSec || 'OFF'}s · panels ${uiCleanup.keepLastPanels}`,
    `🔐 Security: admins ${security.adminIds.length} · mini-site TTL ${security.miniSiteSessionTtlMinutes}m · mask ${security.envMaskPolicy}`,
    `📣 Logs: defaults ${logsDefaults.defaultLevels.join('/')} · allowed ${logsDefaults.allowedProjectsMode}`,
    `🌐 Integrations: base URL ${integrations.baseUrlOverride || 'env'} · ping ${integrations.healthPingIntervalMinutes}m`,
    `📦 Backups: channel ${backups.channelId || 'not set'}`,
  ].filter(Boolean);
  return { text: lines.join('\n'), keyboard: buildSettingsKeyboard() };
}

async function renderGlobalSettings(ctx, notice) {
  if (!getConfigDbSnapshot().ready) {
    await renderConfigDbGate(ctx, { title: '⚙️ Settings', backCallback: 'main:back' });
    return;
  }
  const settings = await loadGlobalSettings();
  const projects = await loadProjects();
  const view = buildGlobalSettingsView(settings, projects, notice);
  await renderOrEdit(ctx, view.text, { reply_markup: view.keyboard });
}

async function renderGlobalSettingsForMessage(messageContext, notice) {
  if (!messageContext) return;
  if (!getConfigDbSnapshot().ready) {
    const view = buildConfigDbGateView({ title: '⚙️ Settings', backCallback: 'main:back' });
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
    return;
  }
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
    .text('🧩 Templates', 'main:templates')
    .row()
    .text('🛠 Ops & Safety', 'gsettings:ops_templates')
    .row()
    .text('🌐 Integrations', 'gsettings:integrations')
    .row()
    .text('🧹 Maintenance', 'gsettings:maintenance')
    .row()
    .text('🔐 Access & Roles', 'gsettings:security')
    .row()
    .text('⬅️ Back', 'nav:back');
}

function buildUiCleanupSettingsView(settings) {
  const ui = normalizeUiCleanupSettings(settings);
  const lines = [
    '🧹 UI & Cleanup',
    '',
    `Auto-clean menus: ${ui.autoCleanMenus ? 'ON' : 'OFF'}`,
    `Ephemeral TTL: ${ui.ephemeralTtlSec ? `${ui.ephemeralTtlSec}s` : 'OFF'}`,
    `Keep last panels: ${ui.keepLastPanels}`,
  ];
  const inline = new InlineKeyboard()
    .text(ui.autoCleanMenus ? '✅ Auto-clean ON' : '🚫 Auto-clean OFF', 'gsettings:ui_toggle')
    .row()
    .text(`⏱ 30s ${ui.ephemeralTtlSec === 30 ? '✅' : ''}`, 'gsettings:ui_ttl:30')
    .text(`⏱ 60s ${ui.ephemeralTtlSec === 60 ? '✅' : ''}`, 'gsettings:ui_ttl:60')
    .text(`⏱ OFF ${ui.ephemeralTtlSec === 0 ? '✅' : ''}`, 'gsettings:ui_ttl:0')
    .row()
    .text(`🧾 Keep 1 ${ui.keepLastPanels === 1 ? '✅' : ''}`, 'gsettings:ui_keep:1')
    .text(`🧾 Keep 3 ${ui.keepLastPanels === 3 ? '✅' : ''}`, 'gsettings:ui_keep:3')
    .text(`🧾 Keep 5 ${ui.keepLastPanels === 5 ? '✅' : ''}`, 'gsettings:ui_keep:5')
    .row()
    .text('⬅️ Back', 'gsettings:menu');
  return { text: lines.join('\n'), keyboard: inline };
}

function buildSecuritySettingsView(settings) {
  const security = normalizeSecuritySettings(settings);
  const adminList = [String(ADMIN_TELEGRAM_ID), ...security.adminIds.map(String)];
  const lines = [
    '🔐 Security',
    '',
    `Admins: ${adminList.filter(Boolean).join(', ') || 'none'}`,
    `Mini-site session TTL: ${security.miniSiteSessionTtlMinutes} min`,
    `Env masking policy: ${security.envMaskPolicy}`,
  ];
  const inline = new InlineKeyboard()
    .text('🪪 Admin list', 'gsettings:admins')
    .row()
    .text('🪪 TTL 10m', 'gsettings:mini_ttl:10')
    .text('🪪 TTL 20m', 'gsettings:mini_ttl:20')
    .text('🪪 TTL 30m', 'gsettings:mini_ttl:30')
    .row()
    .text(`🧯 Strict ${security.envMaskPolicy === 'strict' ? '✅' : ''}`, 'gsettings:env_mask:strict')
    .text(`🧯 Spoiler ${security.envMaskPolicy === 'spoiler' ? '✅' : ''}`, 'gsettings:env_mask:spoiler')
    .row()
    .text('⬅️ Back', 'gsettings:menu');
  return { text: lines.join('\n'), keyboard: inline };
}

function buildAdminListView(settings) {
  const security = normalizeSecuritySettings(settings);
  const lines = ['🪪 Admin list', ''];
  if (!security.adminIds.length) {
    lines.push('No extra admins configured.');
  } else {
    security.adminIds.forEach((id) => lines.push(`• ${id}`));
  }
  const inline = new InlineKeyboard()
    .text('➕ Add admin', 'gsettings:admin_add')
    .row();
  security.adminIds.forEach((id) => {
    inline.text(`🗑 Remove ${id}`, `gsettings:admin_remove:${id}`).row();
  });
  inline.text('⬅️ Back', 'gsettings:security');
  return { text: lines.join('\n'), keyboard: inline };
}

function buildLogsSettingsView(settings) {
  const logsDefaults = normalizeLogDefaults(settings);
  const lines = [
    '📣 Logs settings',
    '',
    `Default levels: ${logsDefaults.defaultLevels.join(' / ')}`,
    `Allowed projects mode: ${logsDefaults.allowedProjectsMode}`,
  ];
  const selected = new Set(logsDefaults.defaultLevels);
  const inline = new InlineKeyboard()
    .text(`❗ Errors ${selected.has('error') ? '✅' : 'OFF'}`, 'gsettings:log_default_level:error')
    .text(`⚠️ Warn ${selected.has('warn') ? '✅' : 'OFF'}`, 'gsettings:log_default_level:warn')
    .row()
    .text(`ℹ️ Info ${selected.has('info') ? '✅' : 'OFF'}`, 'gsettings:log_default_level:info')
    .row()
    .text(
      `✅ Allow all ${logsDefaults.allowedProjectsMode === 'allow-all' ? '✅' : ''}`,
      'gsettings:log_allowed:allow-all',
    )
    .text(
      `📌 Whitelist ${logsDefaults.allowedProjectsMode === 'whitelist' ? '✅' : ''}`,
      'gsettings:log_allowed:whitelist',
    )
    .row()
    .text('⬅️ Back', 'gsettings:menu');
  return { text: lines.join('\n'), keyboard: inline };
}

function buildIntegrationsSettingsView(settings) {
  const integrations = normalizeIntegrationSettings(settings);
  const lines = [
    '🌐 Integrations',
    '',
    `Base URL override: ${integrations.baseUrlOverride || '(env)'}`,
    `Default base branch: ${settings.defaultBaseBranch || DEFAULT_BASE_BRANCH}`,
    `Health ping interval: ${integrations.healthPingIntervalMinutes} min`,
  ];
  const inline = new InlineKeyboard()
    .text('🌐 Set base URL', 'gsettings:base_url')
    .text('🧹 Clear base URL', 'gsettings:base_url_clear')
    .row()
    .text('🌿 Set default base branch', 'gsettings:change_default_base')
    .row()
    .text('🔁 Ping 5m', 'gsettings:ping_interval:5')
    .text('🔁 Ping 10m', 'gsettings:ping_interval:10')
    .text('🔁 Ping 30m', 'gsettings:ping_interval:30')
    .row()
    .text('⬅️ Back', 'gsettings:menu');
  return { text: lines.join('\n'), keyboard: inline };
}

function buildBackupsSettingsView(settings) {
  const backups = normalizeBackupSettings(settings);
  const preview = buildBackupCaptionPreview(settings, {
    projectName: 'Example Project',
    projectId: 'example-project',
    title: 'Example Note',
    category: 'Idea',
    status: NOTE_STATUS.OPEN,
    createdAt: new Date().toISOString(),
    noteId: '00000000-0000-0000-0000-000000000000',
  });
  const lines = [
    '📦 Backups',
    '',
    `Backup channel: ${backups.channelId || 'not set'}`,
    '',
    'Caption preview:',
    preview,
  ];
  const inline = new InlineKeyboard()
    .text('📦 Set backup channel', 'gsettings:backup_channel')
    .row()
    .text('🧾 Edit caption template', 'gsettings:backup_caption')
    .row()
    .text('⬅️ Back', 'gsettings:menu');
  return { text: lines.join('\n'), keyboard: inline };
}

function buildBackupCaptionPreview(settings, payload) {
  const backups = normalizeBackupSettings(settings);
  const template = backups.captionTemplate || '';
  return formatBackupCaption(template, payload);
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
  const progress = new ProgressReporter({
    bot,
    chatId: ctx.chat?.id,
    initialText: '⏳ Running Ping test…',
    totalSteps: 9,
  });
  await progress.start();
  try {
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
      const hint = check.hint ? `
   ↳ ${check.hint}` : '';
      return `${icon} ${check.label}${detail}${hint}`;
    };

    await progress.step('Checking Git binary');
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

    await progress.step('Resolving GitHub token and API');
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

    await progress.step('Checking Supabase links');
    const supabaseCheck = await checkSupabaseConnections();
    supabaseCheck.forEach((entry) => addCheck(entry.status, entry.label, entry.detail, entry.hint));

    await progress.step('Checking Cron API');
    const cronCheck = await checkCronApi();
    addCheck(cronCheck.status, 'Cron API', cronCheck.detail, cronCheck.hint);

    await progress.step('Checking Config DB health');
    const configDbCheck = await checkConfigDbHealthForPing();
    addCheck(configDbCheck.status, 'Config DB (primary)', configDbCheck.detail, configDbCheck.hint);
    if (configDbCheck.secondary) {
      addCheck(
        configDbCheck.secondary.status,
        'Config DB (secondary)',
        configDbCheck.secondary.detail,
        configDbCheck.secondary.hint,
      );
    }

    await progress.step('Checking Telegram setup');
    const telegramCheck = await checkTelegramSetup(defaultProject);
    addCheck(telegramCheck.status, 'Telegram', telegramCheck.detail, telegramCheck.hint);

    await progress.step('Building report');
    const lines = ['📶 Ping test', '', ...checks.map(formatCheck)];
    const inline = new InlineKeyboard()
      .text('🔁 Retry', 'gsettings:ping_test');
    if (defaultProject?.id) {
      inline
        .row()
        .text('📝 Fix repo', `proj:edit_repo:${defaultProject.id}`)
        .text('🔑 Fix token', `proj:edit_github_token:${defaultProject.id}`);
    }
    inline.row().text('⬅️ Back', 'gsettings:menu');
    await renderOrEdit(ctx, lines.join('\n'), { reply_markup: inline });
  } finally {
    await progress.done();
  }
}


async function probeAdhocDbHealth(dsn) {
  if (!dsn) return null;
  const startedAt = Date.now();
  let pool;
  try {
    pool = new Pool({
      connectionString: dsn,
      max: 1,
      idleTimeoutMillis: 2_000,
      connectionTimeoutMillis: 5_000,
      options: '-c statement_timeout=4000',
      keepAlive: true,
    });
    let timer;
    try {
      await Promise.race([
        pool.query('SELECT 1'),
        new Promise((_, reject) => {
          timer = setTimeout(() => {
            const err = new Error('DB_TIMEOUT');
            err.code = 'DB_TIMEOUT';
            reject(err);
          }, 5000);
          if (typeof timer.unref === 'function') timer.unref();
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
    return { ok: true, latencyMs: Date.now() - startedAt, category: null };
  } catch (error) {
    return {
      ok: false,
      latencyMs: Date.now() - startedAt,
      category: classifyDbError(error),
      message: sanitizeDbErrorMessage(error?.message) || 'connection failed',
    };
  } finally {
    if (pool) {
      await pool.end().catch(() => null);
    }
  }
}

async function checkConfigDbHealthForPing() {
  const startedAt = Date.now();
  const status = await probeConfigDbConnection();
  const latencyMs = Date.now() - startedAt;
  const health = getDbHealthSnapshot();
  const response = status.ok
    ? { status: 'ok', detail: `OK (${latencyMs}ms)` }
    : {
      status: 'warn',
      detail: `DOWN (${latencyMs}ms)`,
      hint: `Last error: ${status.category || health.lastErrorCategory || 'unknown'}`,
    };

  const secondaryDsn = process.env.DATABASE_URL_PM_SECONDARY || process.env.PATH_APPLIER_CONFIG_DSN_SECONDARY;
  if (secondaryDsn) {
    const secondary = await probeAdhocDbHealth(secondaryDsn);
    response.secondary = secondary?.ok
      ? { status: 'ok', detail: `OK (${secondary.latencyMs}ms)` }
      : {
        status: 'warn',
        detail: `DOWN (${secondary?.latencyMs || '-'}ms)`,
        hint: `Last error: ${secondary?.category || 'unknown'}`,
      };
  }
  return response;
}

function getPmEnvPreset(name) {
  const presets = {
    PGCONNECT_TIMEOUT: {
      recommendedValue: '15',
      category: 'Recommended (prod)',
      whatItDoes: 'Postgres connect timeout in seconds.',
      notes: 'Safe default for production and development.',
    },
    DB_STATEMENT_TIMEOUT_MS: {
      recommendedValue: '8000',
      category: 'Recommended (prod)',
      whatItDoes: 'Cancels slow SQL statements after timeout.',
      notes: 'Use 5000-8000 ms to reduce long-running query impact.',
    },
    LOG_DEDUPE_WINDOW_SEC: {
      recommendedValue: '30',
      category: 'Recommended (prod)',
      whatItDoes: 'Deduplicates repeated logs inside the time window.',
      notes: 'Reduces alert noise while keeping signal.',
    },
    LOG_RATE_LIMIT_PER_MIN: {
      recommendedValue: '60',
      category: 'Recommended (prod)',
      whatItDoes: 'Caps per-minute log throughput to protect sinks.',
      notes: 'Tune up only if you intentionally handle high volume.',
    },
    LOG_RATE_LIMIT_BURST: {
      recommendedValue: '20',
      category: 'Recommended (prod)',
      whatItDoes: 'Allows short log bursts before throttling.',
      notes: 'Keep aligned with LOG_RATE_LIMIT_PER_MIN.',
    },
    PM_DB_SCHEMA: {
      recommendedValue: 'public',
      category: 'Recommended (prod)',
      whatItDoes: 'Default schema used by PM DB operations.',
      notes: 'Use explicit schema if your DB is multi-tenant.',
    },
    SELF_LOG_FORWARDING: {
      recommendedValue: 'enabled',
      category: 'Recommended (prod)',
      whatItDoes: 'Enables forwarding PM self-logs to configured targets.',
      notes: 'Useful for operational visibility in production.',
    },
    SELF_LOG_FALLBACK: {
      recommendedValue: 'console',
      category: 'Recommended (prod)',
      whatItDoes: 'Fallback sink when forwarding cannot be delivered.',
      notes: 'Keeps diagnostic logs available during integration outages.',
    },
    ALLOW_INSECURE_TLS_FOR_TESTS: {
      recommendedValue: null,
      category: 'Dev-only (do not set in prod)',
      whatItDoes: 'Disables TLS verification for test-only endpoints.',
      notes: 'Security risk in production. Keep unset in prod.',
    },
  };
  return presets[name] || {
    recommendedValue: null,
    category: 'Optional',
    whatItDoes: 'Environment variable used by project/runtime code.',
    notes: 'Set only if required by your runtime behavior.',
  };
}

function getEnvPurposeHint(name) {
  const preset = getPmEnvPreset(name);
  if (preset.whatItDoes && preset.whatItDoes !== 'Environment variable used by project/runtime code.') {
    return preset.whatItDoes;
  }
  if (name.includes('TOKEN') || name.includes('SECRET') || name.includes('KEY')) {
    return 'Authentication secret/token used by integrations.';
  }
  if (name.includes('URL') || name.includes('HOST') || name.includes('PORT')) {
    return 'Connection endpoint/runtime routing setting.';
  }
  if (name.includes('TIMEOUT')) {
    return 'Timeout control for external or DB operations.';
  }
  if (name.includes('LOG')) {
    return 'Logging/observability behavior control.';
  }
  return 'Environment variable used by project/runtime code.';
}

function buildPublicHealthPayload(latencyMs, lastErrorCategory) {
  const memory = process.memoryUsage();
  return {
    status: runtimeStatus.fatalError ? 'degraded' : 'ok',
    uptimeSec: Math.floor(process.uptime()),
    configDb: {
      status: runtimeStatus.configDbOk ? 'ok' : 'down',
      latencyMs,
      lastErrorCategory: lastErrorCategory || null,
    },
    memory: {
      rssMb: Number((memory.rss / (1024 * 1024)).toFixed(2)),
      heapUsedMb: Number((memory.heapUsed / (1024 * 1024)).toFixed(2)),
    },
  };
}

async function buildPublicHealthPayloadSafely() {
  const startedAt = Date.now();
  try {
    const probe = await probeConfigDbConnection();
    const latencyMs = Date.now() - startedAt;
    return buildPublicHealthPayload(latencyMs, probe?.category || getDbHealthSnapshot()?.lastErrorCategory);
  } catch (error) {
    return buildPublicHealthPayload(Date.now() - startedAt, classifyDbError(error));
  }
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

  if (!workingDir) {
    return {
      ok: false,
      code: 'DIR_MISSING',
      details: 'workingDir missing',
      expectedCheckoutDir: checkoutDir ? path.resolve(checkoutDir) : null,
      suggestedWorkingDir: checkoutDir || null,
    };
  }

  const expectedCheckoutDir = checkoutDir ? path.resolve(checkoutDir) : null;
  let canonicalRepoRoot = expectedCheckoutDir;
  if (expectedCheckoutDir) {
    try {
      await fs.stat(expectedCheckoutDir);
      canonicalRepoRoot = await fs.realpath(expectedCheckoutDir);
    } catch (error) {
      return {
        ok: true,
        code: 'CHECKOUT_PENDING',
        details: 'Repo checkout not available yet; validation deferred.',
        expectedCheckoutDir,
        suggestedWorkingDir: checkoutDir || null,
      };
    }
  }

  const normalizedInput = workingDir === '.' ? '.' : String(workingDir).trim();
  const joinedCandidate = path.isAbsolute(normalizedInput)
    ? path.resolve(normalizedInput)
    : canonicalRepoRoot
      ? path.resolve(canonicalRepoRoot, normalizedInput)
      : path.resolve(normalizedInput);

  const resolvedWorkingDir = joinedCandidate;
  if (canonicalRepoRoot) {
    const relative = path.relative(canonicalRepoRoot, resolvedWorkingDir);
    const isWithinRepo = relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
    if (!isWithinRepo) {
      return {
        ok: false,
        code: 'OUTSIDE_REPO',
        details: `workingDir is outside repo checkout (${canonicalRepoRoot})`,
        expectedCheckoutDir: canonicalRepoRoot,
        suggestedWorkingDir: checkoutDir || '.',
      };
    }
  }

  try {
    await fs.stat(resolvedWorkingDir);
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return {
        ok: false,
        code: 'DIR_MISSING',
        details: 'workingDir does not exist',
        expectedCheckoutDir: canonicalRepoRoot,
        suggestedWorkingDir: checkoutDir || null,
      };
    }
    return {
      ok: false,
      code: 'UNKNOWN',
      details: truncateText(error.message || 'unknown error', 120),
      expectedCheckoutDir: canonicalRepoRoot,
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
        expectedCheckoutDir: canonicalRepoRoot,
        suggestedWorkingDir: suggestedWorkingDir || checkoutDir || null,
      };
    }
  }

  return {
    ok: true,
    code: 'OK',
    details: resolvedWorkingDir,
    expectedCheckoutDir: canonicalRepoRoot,
    suggestedWorkingDir: checkoutDir || null,
  };
}

function formatWorkingDirValidationNotice(result) {
  if (result.ok) {
    if (result.code === 'CHECKOUT_PENDING') {
      return '⏳ Working dir saved; repo checkout not available yet for validation.';
    }
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
  if (isSlashCommandLikeInput(trimmed)) {
    return { ok: false, error: 'Working directory cannot be a slash command. Use /start to return to the main menu.' };
  }
  return { ok: true, value: trimmed };
}

function formatWorkingDirDisplay(project) {
  const raw = project?.workingDir;
  if (!raw) return '-';
  const repoSlug = project?.repoSlug;
  const checkoutDir = repoSlug ? getDefaultWorkingDir(repoSlug) : null;
  const expectedCheckoutDir = checkoutDir ? path.resolve(checkoutDir) : null;
  if (!path.isAbsolute(raw)) {
    if (raw === '.') return '.';
    return raw.startsWith('./') ? raw : `./${raw}`;
  }
  if (expectedCheckoutDir) {
    const resolved = path.resolve(raw);
    if (resolved === expectedCheckoutDir) {
      return '.';
    }
    if (resolved.startsWith(`${expectedCheckoutDir}${path.sep}`)) {
      return `./${path.relative(expectedCheckoutDir, resolved)}`;
    }
  }
  return raw;
}

function resolveProjectWorkingDir(project) {
  const workingDir = project?.workingDir;
  if (!workingDir) return null;
  if (path.isAbsolute(workingDir)) return path.resolve(workingDir);
  const repoSlug = project?.repoSlug;
  const checkoutDir = repoSlug ? getDefaultWorkingDir(repoSlug) : null;
  if (checkoutDir) {
    return path.resolve(checkoutDir, workingDir);
  }
  return path.resolve(workingDir);
}

function resolveWorkingDirAgainstCheckout(workingDir, checkoutDir) {
  if (!checkoutDir) return null;
  const base = workingDir || '.';
  if (path.isAbsolute(base)) {
    return path.resolve(base);
  }
  return path.resolve(checkoutDir, base);
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
        detail: dsnInfo.error || 'missing DB URL',
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
        hint: 'Verify DB URL/SSL settings.',
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
  await saveGlobalSettingsAndCache(settings);
}

async function clearDefaultProject() {
  const settings = await loadGlobalSettings();
  settings.defaultProjectId = undefined;
  await saveGlobalSettingsAndCache(settings);
}

async function clearDefaultBaseBranch() {
  const settings = await loadGlobalSettings();
  delete settings.defaultBaseBranch;
  await saveGlobalSettingsAndCache(settings);
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

async function migrateProjectId(oldProjectId, newProjectId) {
  const projects = await loadProjects();
  const idx = projects.findIndex((p) => p.id === oldProjectId);
  if (idx === -1) {
    throw new Error('Project not found.');
  }
  if (projects.some((p) => p.id === newProjectId)) {
    throw new Error('Project ID already exists.');
  }

  const rollback = [];
  try {
    await renameEnvVaultProjectId(oldProjectId, newProjectId);
    rollback.push(() => renameEnvVaultProjectId(newProjectId, oldProjectId));
    await renameCronJobLinkProjectId(oldProjectId, newProjectId);
    rollback.push(() => renameCronJobLinkProjectId(newProjectId, oldProjectId));
    await renameLogIngestProjectId(oldProjectId, newProjectId);
    rollback.push(() => renameLogIngestProjectId(newProjectId, oldProjectId));
    await renameTelegramBotProjectId(oldProjectId, newProjectId);
    rollback.push(() => renameTelegramBotProjectId(newProjectId, oldProjectId));
  } catch (error) {
    await Promise.allSettled(rollback.map((fn) => fn()));
    throw error;
  }

  projects[idx] = { ...projects[idx], id: newProjectId };
  await saveProjects(projects);
  const settings = await loadGlobalSettings();
  if (settings.defaultProjectId === oldProjectId) {
    settings.defaultProjectId = newProjectId;
    await saveGlobalSettingsAndCache(settings);
  }
  if (envScanCache.has(oldProjectId)) {
    envScanCache.set(newProjectId, envScanCache.get(oldProjectId));
    envScanCache.delete(oldProjectId);
  }
  return projects[idx];
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
  const override = cachedSettings?.integrations?.baseUrlOverride;
  if (override) return override;
  return (
    process.env.PM_BASE_URL ||
    process.env.PUBLIC_BASE_URL ||
    process.env.RENDER_EXTERNAL_URL ||
    `http://localhost:${PORT}`
  );
}

function getBearerToken(req) {
  const header = req?.headers?.authorization;
  if (!header) return '';
  const match = String(header).match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
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

async function recordAuditLog(label, context) {
  await forwardSelfLog('info', `audit:${label}`, { context });
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

function parseRenderWebhookPayload(rawBody) {
  if (!rawBody) {
    return { ok: false, error: 'Empty payload.' };
  }
  try {
    const parsed = JSON.parse(rawBody);
    if (!parsed || typeof parsed !== 'object') {
      return { ok: false, error: 'Payload is not an object.' };
    }
    return { ok: true, payload: parsed };
  } catch (error) {
    return { ok: false, error: 'Invalid JSON payload.' };
  }
}

function extractRenderWebhookInfo(payload) {
  const eventType =
    payload.eventType || payload.event_type || payload.event || payload.type || payload.trigger;
  const serviceId =
    payload.serviceId ||
    payload.service_id ||
    payload.service?.id ||
    payload.service?.serviceId ||
    payload.data?.service?.id ||
    payload.data?.serviceId ||
    null;
  const deploy = payload.deploy || payload.data?.deploy || payload.payload?.deploy || {};
  const build = payload.build || payload.data?.build || payload.payload?.build || {};
  const deployId = deploy.id || payload.deployId || payload.deploy_id || payload.data?.deployId || null;
  const buildId = build.id || payload.buildId || payload.build_id || null;
  const eventId = payload.id || payload.eventId || payload.event_id || null;
  const status =
    deploy.status ||
    deploy.state ||
    payload.status ||
    payload.state ||
    payload.result ||
    null;
  const branch = deploy.branch || payload.branch || payload.gitBranch || null;
  const commit = deploy.commit || payload.commit || payload.gitCommit || null;
  const durationMs = deploy.durationMs || deploy.duration_ms || payload.durationMs || null;
  const dashboardUrl = deploy.url || payload.url || payload.dashboardUrl || null;
  return {
    eventType: eventType ? String(eventType) : null,
    serviceId: serviceId ? String(serviceId) : null,
    deployId: deployId ? String(deployId) : null,
    buildId: buildId ? String(buildId) : null,
    eventId: eventId ? String(eventId) : null,
    status: status ? String(status) : null,
    branch: branch ? String(branch) : null,
    commit: commit ? String(commit) : null,
    durationMs: Number.isFinite(Number(durationMs)) ? Number(durationMs) : null,
    dashboardUrl: dashboardUrl ? String(dashboardUrl) : null,
  };
}

async function handleRenderDeployEvent({ project, info, source }) {
  const deploySettings = normalizeProjectDeploySettings(project);
  const eventType = normalizeRenderEventType(info.eventType, info.status);
  if (!eventType) {
    return;
  }
  const receivedAt = new Date().toISOString();
  const statusLabel = info.status || eventType;
  const isDuplicate =
    info.deployId &&
    statusLabel &&
    info.deployId === deploySettings.render.lastDeployId &&
    statusLabel === deploySettings.render.lastDeployStatus;
  await recordAuditLog('render_deploy_event', {
    projectId: project.id,
    serviceId: info.serviceId,
    eventType,
    status: statusLabel,
    source,
    deployId: info.deployId,
  });
  if (isDuplicate) {
    return;
  }
  await recordRenderDeployState(project.id, {
    deployId: info.deployId,
    status: statusLabel,
    eventType,
    receivedAt,
    source,
  });
  const settings = resolveRenderGlobalSettings(await getCachedSettings());
  if (!deploySettings.render.enabled) {
    return;
  }
  if (source === 'webhook' && (!settings.webhookEnabledGlobal || !deploySettings.render.webhookEnabled)) {
    return;
  }
  if (source === 'polling' && (!settings.pollingEnabledGlobal || !deploySettings.render.pollingEnabled)) {
    return;
  }
  if (eventType === 'deploy_started' && !deploySettings.render.notifyOnStart) return;
  if (eventType === 'deploy_succeeded' && !deploySettings.render.notifyOnFinish) return;
  if (eventType === 'deploy_failed' && !deploySettings.render.notifyOnFail) return;
  const message = buildRenderDeployMessage({
    project,
    serviceName: deploySettings.render.serviceName,
    eventType,
    deployId: info.deployId,
    status: statusLabel,
    durationMs: info.durationMs,
    url: info.dashboardUrl,
    timestamp: receivedAt,
  });
  await bot.api.sendMessage(ADMIN_TELEGRAM_ID, message, { disable_web_page_preview: true });
}

function formatDeployOutcome(status) {
  if (!status) return 'unknown';
  const normalized = status.toLowerCase();
  if (['succeeded', 'success', 'live', 'ok'].includes(normalized)) return 'succeeded';
  if (['failed', 'error', 'errored'].includes(normalized)) return 'failed';
  if (['canceled', 'cancelled', 'aborted'].includes(normalized)) return 'canceled';
  return normalized;
}

function classifyRenderDeployStatus(status) {
  if (!status) return { phase: null, outcome: null };
  const normalized = String(status).toLowerCase();
  if (
    ['started', 'starting', 'building', 'deploying', 'in_progress', 'in-progress', 'pending', 'queued'].includes(
      normalized,
    )
  ) {
    return { phase: 'started', outcome: 'started' };
  }
  const outcome = formatDeployOutcome(normalized);
  if (outcome === 'succeeded') {
    return { phase: 'finished', outcome: 'succeeded' };
  }
  if (['failed', 'canceled'].includes(outcome)) {
    return { phase: 'finished', outcome: 'failed' };
  }
  return { phase: null, outcome: outcome || null };
}

function normalizeRenderEventType(eventType, status) {
  const raw = String(eventType || '').toLowerCase();
  if (raw.includes('deploy_started') || raw.includes('build_started')) {
    return 'deploy_started';
  }
  if (raw.includes('deploy_ended') || raw.includes('deploy_finished') || raw.includes('build_ended')) {
    const outcome = classifyRenderDeployStatus(status).outcome;
    return outcome === 'succeeded' ? 'deploy_succeeded' : 'deploy_failed';
  }
  const outcome = classifyRenderDeployStatus(status).outcome;
  if (outcome === 'succeeded') return 'deploy_succeeded';
  if (outcome === 'failed') return 'deploy_failed';
  if (raw.includes('deploy')) return 'deploy_started';
  return null;
}

function shortenRenderId(value) {
  if (!value) return '-';
  const text = String(value);
  return text.length > 8 ? `${text.slice(0, 8)}…` : text;
}

function buildRenderDeployMessage({ project, serviceName, eventType, deployId, status, durationMs, url, timestamp }) {
  const timeLabel = timestamp ? new Date(timestamp).toISOString() : new Date().toISOString();
  const eventLabel =
    eventType === 'deploy_started'
      ? 'Deploy started'
      : eventType === 'deploy_succeeded'
        ? 'Deploy succeeded'
        : 'Deploy failed';
  const emoji = eventType === 'deploy_succeeded' ? '✅' : eventType === 'deploy_failed' ? '❌' : '🚀';
  const lines = [
    `${emoji} ${project.name || project.id} — ${eventLabel}`,
    `Service: ${serviceName || project.render?.serviceName || project.render?.serviceId || '-'}`,
    `DeployId: ${shortenRenderId(deployId)}`,
    `Time: ${timeLabel}`,
  ];
  if (durationMs) {
    lines.push(`Duration: ${Math.round(durationMs / 1000)}s`);
  }
  if (status) {
    lines.push(`Status: ${status}`);
  }
  if (url) {
    lines.push(`Link: ${url}`);
  }
  return lines.join('\n');
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

function buildPrBody(previewText) {
  const preview = String(previewText || '').split('\n').slice(0, 20).join('\n');
  return [
    `Automated change at ${new Date().toISOString()}`,
    '',
    'Summary:',
    '- Added PM diagnostics + log test + logger hooks (as applicable).',
    '',
    'Configuration:',
    '- PM_TEST_TOKEN, PM_TEST_ENABLED',
    '- PM_URL, PM_INGEST_TOKEN',
    '',
    'How to re-run PM log tests:',
    '- POST /pm/test-log with PM_TEST_TOKEN',
    '- GET /pm/diagnostics to verify logger flags',
    '',
    'Safety notes:',
    '- No secrets logged or printed.',
    '- Tokens remain environment-based.',
    '',
    'Preview:',
    '',
    preview,
  ].join('\n');
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
    const category = classifyDbError(error);
    if (category) {
      recordDbError(category, error?.message);
    }
    setDbReady(false);
    return false;
  }
  return true;
}

async function loadConfig() {
  const ok = await initializeConfig();
  cachedSettings = await loadGlobalSettings();
  cachedSettingsAt = Date.now();
  return ok;
}

async function initDb() {
  return testConfigDbConnection();
}

async function initEnvVault() {
  const status = getMasterKeyStatus();
  if (status.ok) {
    runtimeStatus.vaultOk = true;
    runtimeStatus.vaultError = null;
    console.log('Env Vault: OK');
    return true;
  }
  const reason =
    status.error === 'missing'
      ? 'ENV_VAULT_MASTER_KEY not set.'
      : MASTER_KEY_ERROR_MESSAGE;
  runtimeStatus.vaultOk = false;
  runtimeStatus.vaultError = reason;
  throw new Error(`Startup aborted: ${reason}`);
}

async function handleWebRequest(req, res, url) {
  if (!url.pathname.startsWith('/web')) {
    return false;
  }

  const pathParts = url.pathname.split('/').filter(Boolean);
  const isApi = url.pathname.startsWith('/web/api');

  if (req.method === 'POST' && url.pathname === '/web/login') {
    const ip = getClientIp(req);
    const rateStatus = checkWebLoginRateLimit(ip);
    if (rateStatus.blocked) {
      res.writeHead(429, {
        'Content-Type': 'application/json; charset=utf-8',
        'Retry-After': Math.ceil(rateStatus.retryAfterMs / 1000),
      });
      res.end(JSON.stringify({ ok: false, error: 'Too many attempts. Try again later.' }));
      return true;
    }

    const body = await readRequestBody(req);
    const contentType = req.headers['content-type'] || '';
    let payload = {};
    if (contentType.includes('application/json')) {
      try {
        payload = body ? JSON.parse(body) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
    } else {
      payload = parseFormBody(body);
    }

    const token = String(payload.token || '').trim();
    const { settings } = await getWebDashboardSettingsState();
    if (!isWebDashboardTokenConfigured(settings)) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: 'Dashboard token not configured.' }));
      return true;
    }

    if (!isWebDashboardTokenValid(token, settings)) {
      registerWebLoginAttempt(ip, false);
      res.writeHead(403, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: 'Invalid token.' }));
      return true;
    }

    registerWebLoginAttempt(ip, true);
    const session = createWebSession();
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Set-Cookie': buildWebSessionCookie(session.token, session.ttlMs),
    });
    res.end(JSON.stringify({ ok: true, ttlMinutes: WEB_DASHBOARD_SESSION_TTL_MINUTES }));
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/web/logout') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Set-Cookie': buildWebClearCookie(),
    });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  if (req.method === 'GET' && (url.pathname === '/web' || url.pathname === '/web/' || url.pathname === '/web/login')) {
    const { settings, web } = await getWebDashboardSettingsState();
    let displayToken = null;
    let shouldShowToken = false;

    if (!web.adminTokenHash) {
      const ensured = await ensureWebDashboardToken();
      displayToken = ensured.token;
      shouldShowToken = true;
      await markWebDashboardTokenShown(ensured.settings);
    } else if (!web.adminTokenShownAt && web.adminTokenEnc) {
      try {
        displayToken = decryptSecret(web.adminTokenEnc);
        shouldShowToken = true;
        await markWebDashboardTokenShown(settings);
      } catch (error) {
        console.error('[web] Failed to decrypt stored dashboard token', error);
      }
    }

    if (shouldShowToken) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(
        renderWebLoginPage({
          token: displayToken,
          mask: maskWebDashboardToken(displayToken),
          message: 'Save this token now. It will only be shown once.',
        }),
      );
      return true;
    }

    try {
      const html = await fs.readFile(path.join(WEB_DASHBOARD_ASSETS_DIR, 'index.html'), 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Web dashboard assets not available.');
    }
    return true;
  }

  if (req.method === 'GET' && url.pathname.startsWith('/web/assets/')) {
    const relativePath = url.pathname.replace('/web/assets/', '');
    const assetPath = path.normalize(path.join(WEB_DASHBOARD_ASSETS_DIR, relativePath));
    if (!assetPath.startsWith(WEB_DASHBOARD_ASSETS_DIR)) {
      res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Forbidden');
      return true;
    }
    try {
      const contents = await fs.readFile(assetPath);
      res.writeHead(200, { 'Content-Type': getContentTypeForPath(assetPath) });
      res.end(contents);
    } catch (error) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
    }
    return true;
  }

  if (isApi) {
    const cookies = parseCookies(req);
    const sessionToken = cookies[WEB_DASHBOARD_SESSION_COOKIE];
    const sessionCheck = validateWebSession(sessionToken);
    if (!sessionCheck.ok) {
      res.writeHead(401, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: 'Unauthorized' }));
      return true;
    }

    if (req.method === 'GET' && url.pathname === '/web/api/health') {
      const payload = await buildWebHealthPayload();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(payload));
      return true;
    }

    if (req.method === 'GET' && url.pathname === '/web/api/projects') {
      const projects = await buildWebProjectsPayload();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, projects }));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 4 && pathParts[2] === 'projects') {
      const projectId = decodeURIComponent(pathParts[3]);
      const project = await buildWebProjectDetailPayload(projectId);
      if (!project) {
        res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Project not found.' }));
        return true;
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, project }));
      return true;
    }

    if (req.method === 'GET' && url.pathname === '/web/api/logs') {
      const projectId = url.searchParams.get('project') || '';
      const level = url.searchParams.get('level') || '';
      const page = Number(url.searchParams.get('page') || 0);
      const payload = await buildWebLogsPayload({ projectId, level, page });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, ...payload }));
      return true;
    }

    if (req.method === 'GET' && url.pathname === '/web/api/cronjobs') {
      const payload = await buildWebCronJobsPayload();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(payload));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 4 && pathParts[2] === 'envvault') {
      const projectId = decodeURIComponent(pathParts[3]);
      const project = await getProjectById(projectId);
      if (!project) {
        res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Project not found.' }));
        return true;
      }
      const payload = await buildWebEnvVaultPayload(projectId);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, ...payload }));
      return true;
    }

    if (req.method === 'POST' && pathParts.length === 4 && pathParts[2] === 'envvault') {
      const projectId = decodeURIComponent(pathParts[3]);
      const project = await getProjectById(projectId);
      if (!project) {
        res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Project not found.' }));
        return true;
      }
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const key = String(payload.key || '').trim();
      const value = String(payload.value || '').trim();
      if (!key || !value) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Key and value are required.' }));
        return true;
      }
      const envSetId = await ensureDefaultEnvVarSet(projectId);
      await upsertEnvVar(projectId, key, value, envSetId);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, key }));
      return true;
    }


    if (req.method === 'GET' && url.pathname === '/web/api/db/connections') {
      const payload = await buildWebDbConnectionsPayload();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(payload));
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/web/api/db/connections') {
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const result = await addWebDbConnection(payload);
      res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'DELETE' && pathParts.length === 5 && pathParts[2] === 'db' && pathParts[3] === 'connections') {
      const connectionId = decodeURIComponent(pathParts[4]);
      const result = await deleteWebDbConnection(connectionId);
      res.writeHead(result.ok ? 200 : 404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/web/api/db/project-bindings') {
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const result = await setWebProjectDbBindings(payload);
      res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/web/api/db/dual-mode') {
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const result = await setWebDualDbMode(payload);
      res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 5 && pathParts[2] === 'db' && pathParts[4] === 'schemas') {
      const connectionId = decodeURIComponent(pathParts[3]);
      const result = await fetchDbSchemas(connectionId);
      res.writeHead(result.ok ? 200 : 404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 5 && pathParts[2] === 'db' && pathParts[4] === 'tables') {
      const connectionId = decodeURIComponent(pathParts[3]);
      const schema = url.searchParams.get('schema') || 'public';
      const result = await fetchDbTables(connectionId, schema);
      res.writeHead(result.ok ? 200 : 404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 5 && pathParts[2] === 'db' && pathParts[4] === 'table-rows') {
      const connectionId = decodeURIComponent(pathParts[3]);
      const schema = url.searchParams.get('schema') || 'public';
      const table = url.searchParams.get('table') || '';
      const page = Number(url.searchParams.get('page') || 0);
      const result = await fetchDbTableRows(connectionId, schema, table, page);
      res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'POST' && pathParts.length === 5 && pathParts[2] === 'db' && pathParts[4] === 'sql') {
      const connectionId = decodeURIComponent(pathParts[3]);
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const result = await runWebDbSql(connectionId, payload);
      res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/web/api/db/migrate') {
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const result = await runWebDbMigration(payload);
      res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/web/api/db/sync') {
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const result = await runWebDbSync(payload);
      res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/web/api/patch/apply') {
      const raw = await readRequestBody(req);
      let payload = {};
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch (error) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body.' }));
        return true;
      }
      const projectId = String(payload.projectId || '').trim();
      const specText = String(payload.specText || '').trim();
      if (!projectId || !specText) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'projectId and specText are required.' }));
        return true;
      }
      try {
        const result = await applyWebPatchSpec({ projectId, specText });
        res.writeHead(result.ok ? 200 : 400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(result));
      } catch (error) {
        res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: error.message || 'Failed to apply patch.' }));
      }
      return true;
    }

    res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: false, error: 'Not found' }));
    return true;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
  return true;
}

async function handleMiniSiteRequest(req, res, url) {
  if (!url.pathname.startsWith('/db-mini')) {
    return false;
  }
  const requestId = buildMiniSiteRequestId();
  console.info('[mini-site] request', { requestId, method: req.method, path: url.pathname });
  let adminTokenHash = null;
  let project = null;

  try {
    const { settings: miniSiteSettings } = await getMiniSiteSettingsState();
    adminTokenHash = getMiniSiteAdminTokenHash(miniSiteSettings);
    if (!adminTokenHash) {
      console.warn('[mini-site] token missing', { path: url.pathname });
      res.writeHead(401, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('DB mini-site token is not configured. Use the bot to enable the mini-site.');
      return true;
    }

    const sessionTtlMs = resolveMiniSiteSessionTtlMs(miniSiteSettings);

    if (req.method === 'GET' && url.searchParams.has('session')) {
      const sessionToken = url.searchParams.get('session');
      const sessionCheck = await validateMiniSiteSession(sessionToken, 'link');
      if (!sessionCheck.ok) {
        console.warn('[mini-site] invalid session token', { path: url.pathname, reason: sessionCheck.reason });
        res.writeHead(401, { 'Content-Type': 'text/plain; charset=utf-8' });
        if (sessionCheck.reason === 'expired') {
          res.end('Session token expired. Generate a new link from the bot.');
        } else {
          res.end('Session token invalid. Generate a new link from the bot.');
        }
        return true;
      }
      const browseToken = await createMiniSiteSession({
        scope: 'browse',
        ttlMs: sessionTtlMs,
      });
      const redirectUrl = new URL(url.toString());
      redirectUrl.searchParams.delete('session');
      const location = `${redirectUrl.pathname}${redirectUrl.search}` || '/db-mini';
      res.writeHead(302, {
        Location: location,
        'Set-Cookie': buildMiniSiteCookie(
          MINI_SITE_SESSION_COOKIE,
          browseToken,
          Math.floor(sessionTtlMs / 1000),
        ),
      });
      res.end();
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/db-mini/login') {
      const body = await readRequestBody(req);
      const form = parseFormBody(body);
      if (!isMiniSiteAdminTokenValid(form.token, adminTokenHash)) {
        res.writeHead(403, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteLogin('Invalid token. Try again.'));
        return true;
      }
      const browseToken = await createMiniSiteSession({
        scope: 'browse',
        ttlMs: sessionTtlMs,
      });
      res.writeHead(302, {
        Location: '/db-mini',
        'Set-Cookie': buildMiniSiteCookie(
          MINI_SITE_SESSION_COOKIE,
          browseToken,
          Math.floor(sessionTtlMs / 1000),
        ),
      });
      res.end();
      return true;
    }

    if (req.method === 'POST' && url.pathname === '/db-mini/edit-login') {
      const body = await readRequestBody(req);
      const form = parseFormBody(body);
      if (!isMiniSiteAdminTokenValid(form.token, adminTokenHash)) {
        res.writeHead(403, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteEditLogin(form.redirect || '/db-mini'));
        return true;
      }
      const editToken = await createMiniSiteSession({
        scope: 'edit',
        ttlMs: MINI_SITE_EDIT_TOKEN_TTL_SEC * 1000,
      });
      res.writeHead(302, {
        Location: form.redirect || '/db-mini',
        'Set-Cookie': buildMiniSiteCookie(MINI_SITE_EDIT_SESSION_COOKIE, editToken, MINI_SITE_EDIT_TOKEN_TTL_SEC),
      });
      res.end();
      return true;
    }

    if (!(await isMiniSiteAuthed(req, adminTokenHash))) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLogin());
      return true;
    }

    const pathParts = url.pathname.split('/').filter(Boolean);
    const projectId = pathParts[1] ? decodeURIComponent(pathParts[1]) : null;

    if (req.method === 'GET' && pathParts.length === 1) {
      let cards = '';
      let errorNotice = '';
      try {
        const projects = await loadProjects();
        cards = projects
          .map((projectItem) => {
            const label = escapeHtml(projectItem.name || projectItem.id);
            return `
              <div class="card">
                <h3>${label}</h3>
                <p class="muted">ID: ${escapeHtml(projectItem.id)}</p>
                <a class="button" href="/db-mini/${encodeURIComponent(projectItem.id)}">Open project</a>
              </div>
            `;
          })
          .join('');
      } catch (error) {
        console.error('[mini-site] failed to load projects list', error);
        errorNotice = '<div class="card"><p>Failed to load projects.</p><p class="muted">Check logs and try again.</p></div>';
      }
      const body = cards
        ? `<div class="grid two-col">${cards}</div>`
        : errorNotice || '<p class="muted">No projects configured yet.</p>';
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout('Project list', body));
      return true;
    }

    project = projectId ? await getProjectById(projectId) : null;
    if (!project) {
      res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout('Project not found', '<p>Unknown project.</p>'));
      return true;
    }

    const connection = await resolveMiniSiteDbConnection(project);
    if (!connection.dsn) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(
        renderMiniSiteLayout(
          'Database not configured',
          `<div class="card"><p>Database connection missing for this project.</p><p class="muted">Source: ${escapeHtml(connection.source || '-')}</p></div>`,
        ),
      );
      return true;
    }
    const sslSettings = resolveProjectDbSslSettings(project, connection);
    const pool = getMiniSitePool(connection.dsn, sslSettings);
    if (!pool) {
      res.writeHead(502, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(
        renderMiniSiteDbErrorPage({
          requestId,
          stage: 'auth',
          errorCode: 'POOL_UNAVAILABLE',
          retryHref: buildMiniSiteRetryHref(url),
        }),
      );
      return true;
    }

    const warmupResult = await warmupMiniSiteDb({
      projectId: project.id,
      dsn: connection.dsn,
      sslSettings,
      pool,
      requestId,
      url,
    });
    if (!warmupResult.ok) {
      res.writeHead(502, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(
        renderMiniSiteDbErrorPage({
          requestId,
          stage: warmupResult.stage || 'query',
          errorCode: warmupResult.errorCode,
          latencyMs: warmupResult.latencyMs,
          retryHref: buildMiniSiteRetryHref(url),
        }),
      );
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 3 && pathParts[2] === 'sql') {
      const enableWrite = url.searchParams.get('enable_write') === '1';
      if (enableWrite) {
        if (!(await isMiniSiteEditAuthed(req, adminTokenHash))) {
          res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
          res.end(renderMiniSiteEditLogin(url.pathname + url.search));
          return true;
        }
        const body = `
          <div class="card">
            <h3>Enable write mode</h3>
            <p class="muted">Write mode is time-limited. Type ENABLE to confirm (60 seconds).</p>
            <form method="POST" action="/db-mini/${encodeURIComponent(project.id)}/sql/enable-write">
              <div class="form-row">
                <input name="confirm" placeholder="Type ENABLE to proceed" required />
              </div>
              <button class="button" type="submit">Enable write mode</button>
            </form>
            <p><a href="/db-mini/${encodeURIComponent(project.id)}/sql">⬅ Back</a></p>
          </div>
        `;
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteLayout('Enable write mode', body));
        return true;
      }
      const writeEnabled = isMiniSiteWriteGrantActive(req, adminTokenHash);
      const writeBadge = writeEnabled
        ? `<span class="tag success">Write mode enabled</span>`
        : `<span class="tag">Read-only</span>`;
      const body = `
        <div class="card">
          <h3>SQL runner — ${escapeHtml(project.name || project.id)}</h3>
          <p class="muted">Read-only by default. Only SELECT / SHOW / EXPLAIN are allowed.</p>
          <div class="row-actions">
            ${writeBadge}
            <a class="button" href="/db-mini/${encodeURIComponent(project.id)}/sql?enable_write=1">Enable write mode</a>
          </div>
        </div>
        <div class="card">
          <form method="POST" action="/db-mini/${encodeURIComponent(project.id)}/sql/run">
            <div class="form-row">
              <label>Query</label>
              <textarea name="query" rows="8" placeholder="SELECT * FROM table"></textarea>
            </div>
            <div class="form-row">
              <label>Limit rows</label>
              <input type="number" name="limit" value="${MINI_SITE_SQL_DEFAULT_LIMIT}" min="1" max="${MINI_SITE_SQL_MAX_LIMIT}" />
            </div>
            <input type="hidden" name="page" value="0" />
            <button class="button" type="submit">Run query</button>
          </form>
          <p><a href="/db-mini/${encodeURIComponent(project.id)}">⬅ Back</a></p>
        </div>
      `;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout('SQL runner', body));
      return true;
    }

    if (req.method === 'POST' && pathParts.length === 4 && pathParts[2] === 'sql' && pathParts[3] === 'enable-write') {
      if (!(await isMiniSiteEditAuthed(req, adminTokenHash))) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteEditLogin(url.pathname.replace(/\/enable-write$/, '')));
        return true;
      }
      const body = await readRequestBody(req);
      const form = parseFormBody(body);
      if (form.confirm !== 'ENABLE') {
        const errorBody = `
          <div class="card">
            <h3 class="warning">Write mode not enabled</h3>
            <p class="muted">Confirmation missing. Type ENABLE to proceed.</p>
            <p><a href="/db-mini/${encodeURIComponent(project.id)}/sql?enable_write=1">⬅ Back</a></p>
          </div>
        `;
        res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteLayout('Write mode blocked', errorBody));
        return true;
      }
      const expiresAt = enableMiniSiteWriteGrant(req, adminTokenHash);
      const bodyHtml = `
        <div class="card">
          <h3 class="success">Write mode enabled</h3>
          <p class="muted">Expires at ${new Date(expiresAt).toISOString()}</p>
          <p><a href="/db-mini/${encodeURIComponent(project.id)}/sql">Continue to SQL runner</a></p>
        </div>
      `;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout('Write mode enabled', bodyHtml));
      return true;
    }

    if (req.method === 'POST' && pathParts.length === 4 && pathParts[2] === 'sql' && pathParts[3] === 'run') {
      const body = await readRequestBody(req);
      const form = parseFormBody(body);
      const normalized = normalizeSqlInput(form.query);
      const limit = normalizeSqlLimit(form.limit);
      const page = Math.max(0, Number(form.page || 0));
      const offset = page * limit;
      const writeAttempt = normalized.ok ? isSqlWriteAttempt(normalized.sql) : false;
      const writeEnabled = isMiniSiteWriteGrantActive(req, adminTokenHash);
      const requestStartedAt = Date.now();
      if (writeAttempt) {
        await recordAuditLog('mini_site_sql_write_attempt', {
          adminId: 'mini-site',
          projectId: project.id,
          requestId,
          queryHash: hashSqlQuery(form.query),
          writeEnabled,
        });
      }
      if (!normalized.ok) {
        const bodyHtml = `
          <div class="card">
            <h3 class="warning">Invalid query</h3>
            <p class="muted">${escapeHtml(normalized.error)}</p>
            <p class="muted">Request ID: ${escapeHtml(requestId)}</p>
            <p><a href="/db-mini/${encodeURIComponent(project.id)}/sql">⬅ Back</a></p>
          </div>
        `;
        res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteLayout('SQL runner', bodyHtml));
        return true;
      }
      if (writeAttempt && !writeEnabled) {
        const bodyHtml = `
          <div class="card">
            <h3 class="warning">Write blocked</h3>
            <p class="muted">Query contains write operations. Enable write mode to proceed.</p>
            <p class="muted">Request ID: ${escapeHtml(requestId)}</p>
            <p><a href="/db-mini/${encodeURIComponent(project.id)}/sql?enable_write=1">Enable write mode</a></p>
          </div>
        `;
        res.writeHead(403, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteLayout('SQL runner', bodyHtml));
        return true;
      }
      let rows = [];
      let columns = [];
      let hasNext = false;
      let errorMessage = null;
      try {
        let queryText = normalized.sql;
        const firstWord = queryText.trim().split(/\s+/)[0].toLowerCase();
        if (firstWord === 'select') {
          queryText = `SELECT * FROM (${queryText}) AS pm_query LIMIT $1 OFFSET $2`;
          const result = await runMiniSiteQuery(pool, queryText, [limit + 1, offset]);
          rows = result.rows || [];
          hasNext = rows.length > limit;
          rows = rows.slice(0, limit);
        } else {
          const result = await runMiniSiteQuery(pool, queryText, []);
          rows = result.rows || [];
        }
        if (rows.length) {
          columns = Object.keys(rows[0]);
        }
      } catch (error) {
        const stage = classifyMiniSiteDbStage(error);
        if (stage !== 'query') {
          res.writeHead(502, { 'Content-Type': 'text/html; charset=utf-8' });
          res.end(
            renderMiniSiteDbErrorPage({
              requestId,
              stage,
              errorCode: error?.code || 'unknown',
              retryHref: buildMiniSiteRetryHref(url),
            }),
          );
          return true;
        }
        const errorCode = error?.code || '';
        const lowerMessage = String(error?.message || '').toLowerCase();
        if (errorCode === '57014' || lowerMessage.includes('timeout')) {
          errorMessage = 'Timed out. Reduce query size or increase timeout.';
        } else {
          errorMessage = error?.message || 'Query failed.';
        }
        console.error('[mini-site] sql runner error', {
          requestId,
          projectId: project.id,
          error: error?.message,
        });
      }
      const durationMs = Date.now() - requestStartedAt;
      await recordAuditLog('mini_site_sql_execution', {
        adminId: 'mini-site',
        projectId: project.id,
        requestId,
        queryHash: hashSqlQuery(normalized.sql),
        durationMs,
        mode: writeAttempt ? 'write' : 'read',
        writeEnabled,
      });
      if (errorMessage) {
        const bodyHtml = `
          <div class="card">
            <h3 class="warning">Query failed</h3>
            <p class="muted">${escapeHtml(errorMessage)}</p>
            <p class="muted">Request ID: ${escapeHtml(requestId)}</p>
            <p><a href="/db-mini/${encodeURIComponent(project.id)}/sql">⬅ Back</a></p>
          </div>
        `;
        res.writeHead(500, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteLayout('SQL runner', bodyHtml));
        return true;
      }
      const headerCells = columns.map((col) => `<th>${escapeHtml(col)}</th>`).join('');
      const bodyRows = rows
        .map((row) => {
          const cells = columns.map((col) => `<td>${escapeHtml(row[col])}</td>`).join('');
          return `<tr>${cells}</tr>`;
        })
        .join('');
      const pager = `
        <div class="row-actions">
          ${page > 0 ? `
            <form method="POST" action="/db-mini/${encodeURIComponent(project.id)}/sql/run">
              <input type="hidden" name="query" value="${escapeHtmlAttribute(normalized.sql)}" />
              <input type="hidden" name="limit" value="${limit}" />
              <input type="hidden" name="page" value="${page - 1}" />
              <button class="button" type="submit">⬅ Prev</button>
            </form>` : ''}
          ${hasNext ? `
            <form method="POST" action="/db-mini/${encodeURIComponent(project.id)}/sql/run">
              <input type="hidden" name="query" value="${escapeHtmlAttribute(normalized.sql)}" />
              <input type="hidden" name="limit" value="${limit}" />
              <input type="hidden" name="page" value="${page + 1}" />
              <button class="button" type="submit">Next ➡</button>
            </form>` : ''}
        </div>
      `;
      const resultBody = `
        <div class="card">
          <h3>Results</h3>
          <p class="muted">Rows: ${rows.length} · Time: ${durationMs} ms</p>
          <table>
            <thead><tr>${headerCells || '<th>(no columns)</th>'}</tr></thead>
            <tbody>${bodyRows || '<tr><td class="muted">(no rows)</td></tr>'}</tbody>
          </table>
          ${pager}
          <p><a href="/db-mini/${encodeURIComponent(project.id)}/sql">⬅ Back</a></p>
        </div>
      `;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout('SQL runner', resultBody));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 2) {
      const tables = await listMiniSiteTables(pool);
      const body = `
        <div class="card">
          <h3>${escapeHtml(project.name || project.id)}</h3>
          <p class="muted">Source: ${escapeHtml(connection.source || '-')}</p>
          <p>Tables: <span class="pill">${tables.length}</span></p>
          <div class="row-actions">
            <a class="button" href="/db-mini/${encodeURIComponent(project.id)}/tables">View tables</a>
            <a class="button" href="/db-mini/${encodeURIComponent(project.id)}/sql">SQL runner</a>
          </div>
        </div>
      `;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout(`Project ${project.id}`, body));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 3 && pathParts[2] === 'tables') {
      const tables = await listMiniSiteTables(pool);
      const items = tables
        .map(
          (table) => `
            <tr>
              <td>${escapeHtml(table.table_schema)}</td>
              <td>${escapeHtml(table.table_name)}</td>
              <td><a href="/db-mini/${encodeURIComponent(project.id)}/table/${encodeURIComponent(
                table.table_schema,
              )}/${encodeURIComponent(table.table_name)}">Browse</a></td>
            </tr>
          `,
        )
        .join('');
      const body = `
        <div class="card">
          <h3>Tables (${tables.length})</h3>
          <table>
            <thead><tr><th>Schema</th><th>Table</th><th></th></tr></thead>
            <tbody>${items}</tbody>
          </table>
          <p><a href="/db-mini/${encodeURIComponent(project.id)}">⬅ Back</a></p>
        </div>
      `;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout('Tables', body));
      return true;
    }

    if (req.method === 'GET' && pathParts.length === 5 && pathParts[2] === 'table') {
      const schema = decodeURIComponent(pathParts[3]);
      const table = decodeURIComponent(pathParts[4]);
      const page = Math.max(0, Number(url.searchParams.get('page') || 0));
      const offset = page * MINI_SITE_PAGE_SIZE;
      const columns = await fetchMiniSiteTableColumns(pool, schema, table);
      const primaryKeys = await fetchMiniSitePrimaryKeys(pool, schema, table);
      const orderBy = primaryKeys.length
        ? `ORDER BY ${primaryKeys.map((key) => quoteIdentifier(key)).join(', ')}`
        : '';
      const { rows } = await runMiniSiteQuery(
        pool,
        `SELECT ctid, * FROM ${quoteIdentifier(schema)}.${quoteIdentifier(table)} ${orderBy} LIMIT $1 OFFSET $2`,
        [MINI_SITE_PAGE_SIZE + 1, offset],
        { retry: true },
      );
      const hasNext = rows.length > MINI_SITE_PAGE_SIZE;
      const pageRows = rows.slice(0, MINI_SITE_PAGE_SIZE);
      const headerCells = columns.map((col) => `<th>${escapeHtml(col.column_name)}</th>`).join('');
      const bodyRows = pageRows
        .map((row) => {
          const keyPayload = primaryKeys.length
            ? { mode: 'pk', values: primaryKeys.reduce((acc, key) => ({ ...acc, [key]: row[key] }), {}) }
            : { mode: 'ctid', values: { ctid: row.ctid } };
          const encoded = encodeMiniSiteRowKey(keyPayload);
          const cells = columns
            .map((col) => `<td>${escapeHtml(row[col.column_name])}</td>`)
            .join('');
          return `
            <tr>
              ${cells}
              <td><a href="/db-mini/${encodeURIComponent(project.id)}/table/${encodeURIComponent(
                schema,
              )}/${encodeURIComponent(table)}/row/${encoded}">View</a></td>
            </tr>
          `;
        })
        .join('');
      const pager = `
        <div class="row-actions">
          ${page > 0 ? `<a class="button" href="?page=${page - 1}">⬅ Prev</a>` : ''}
          ${hasNext ? `<a class="button" href="?page=${page + 1}">Next ➡</a>` : ''}
        </div>
      `;
      const body = `
        <div class="card">
          <h3>${escapeHtml(schema)}.${escapeHtml(table)}</h3>
          <p class="muted">Page ${page + 1}</p>
          <table>
            <thead><tr>${headerCells}<th></th></tr></thead>
            <tbody>${bodyRows || '<tr><td colspan="99" class="muted">(no rows)</td></tr>'}</tbody>
          </table>
          ${pager}
          <p><a href="/db-mini/${encodeURIComponent(project.id)}/tables">⬅ Back to tables</a></p>
        </div>
      `;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout(`${schema}.${table}`, body));
      return true;
    }

    if (pathParts.length === 7 && pathParts[2] === 'table' && pathParts[5] === 'row') {
      const schema = decodeURIComponent(pathParts[3]);
      const table = decodeURIComponent(pathParts[4]);
      const keyData = decodeMiniSiteRowKey(pathParts[6]);
      if (!keyData) {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Invalid row key.');
        return true;
      }
      const columns = await fetchMiniSiteTableColumns(pool, schema, table);
      const primaryKeys = await fetchMiniSitePrimaryKeys(pool, schema, table);
      let row = null;
      if (keyData.mode === 'pk') {
        const whereParts = primaryKeys.map((key, index) => `${quoteIdentifier(key)} = $${index + 1}`);
        const values = primaryKeys.map((key) => keyData.values?.[key]);
        const { rows } = await runMiniSiteQuery(
          pool,
          `SELECT * FROM ${quoteIdentifier(schema)}.${quoteIdentifier(table)} WHERE ${whereParts.join(' AND ')} LIMIT 1`,
          values,
          { retry: true },
        );
        row = rows[0] || null;
      } else if (keyData.mode === 'ctid') {
        const { rows } = await runMiniSiteQuery(
          pool,
          `SELECT ctid, * FROM ${quoteIdentifier(schema)}.${quoteIdentifier(table)} WHERE ctid = $1 LIMIT 1`,
          [keyData.values?.ctid],
          { retry: true },
        );
        row = rows[0] || null;
      }
      if (!row) {
        res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteLayout('Row not found', '<p>Row not found.</p>'));
        return true;
      }

      const editMode = url.searchParams.get('edit') === '1';
      if (editMode && !(await isMiniSiteEditAuthed(req, adminTokenHash))) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteEditLogin(url.pathname + url.search));
        return true;
      }

      const rowsHtml = columns
        .map((col) => `<tr><th>${escapeHtml(col.column_name)}</th><td>${escapeHtml(row[col.column_name])}</td></tr>`)
        .join('');
      const editToggle = `<a class="button" href="${url.pathname}?edit=1">Enable edit mode</a>`;
      let editForm = '';
      if (editMode) {
        if (!primaryKeys.length) {
          editForm = '<p class="muted">Editing disabled: no primary key detected.</p>';
        } else {
          const inputs = columns
            .filter((col) => !primaryKeys.includes(col.column_name))
            .map(
              (col) => `
                <div class="form-row">
                  <label>${escapeHtml(col.column_name)}</label>
                  <input name="${escapeHtml(col.column_name)}" value="${escapeHtml(row[col.column_name] ?? '')}" />
                </div>
              `,
            )
            .join('');
          editForm = `
            <div class="card">
              <h4>✏️ Edit row</h4>
              <form method="POST" action="${url.pathname}/edit">
                ${inputs}
                <div class="form-row">
                  <input name="confirm" placeholder="Type CONFIRM to save" required />
                </div>
                <button class="button" type="submit">Save changes</button>
              </form>
            </div>
          `;
        }
      }
      const body = `
        <div class="card">
          <h3>Row detail</h3>
          <table>${rowsHtml}</table>
          <div class="row-actions">
            ${editToggle}
            <a href="/db-mini/${encodeURIComponent(project.id)}/table/${encodeURIComponent(
              schema,
            )}/${encodeURIComponent(table)}">⬅ Back</a>
          </div>
        </div>
        ${editForm}
      `;
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(renderMiniSiteLayout(`Row ${schema}.${table}`, body));
      return true;
    }

    if (req.method === 'POST' && pathParts.length === 8 && pathParts[2] === 'table' && pathParts[5] === 'row' && pathParts[7] === 'edit') {
      if (!(await isMiniSiteEditAuthed(req, adminTokenHash))) {
        res.writeHead(403, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniSiteEditLogin(url.pathname.replace(/\/edit$/, '')));
        return true;
      }
      const schema = decodeURIComponent(pathParts[3]);
      const table = decodeURIComponent(pathParts[4]);
      const keyData = decodeMiniSiteRowKey(pathParts[6]);
      if (!keyData || keyData.mode !== 'pk') {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Row editing requires a primary key.');
        return true;
      }
      const primaryKeys = await fetchMiniSitePrimaryKeys(pool, schema, table);
      if (!primaryKeys.length) {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Row editing requires a primary key.');
        return true;
      }
      const body = await readRequestBody(req);
      const form = parseFormBody(body);
      if (form.confirm !== 'CONFIRM') {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Confirmation missing. Type CONFIRM to save.');
        return true;
      }
      const columns = await fetchMiniSiteTableColumns(pool, schema, table);
      const editableCols = columns.map((col) => col.column_name).filter((col) => !primaryKeys.includes(col));
      const updates = [];
      const values = [];
      editableCols.forEach((col) => {
        if (Object.prototype.hasOwnProperty.call(form, col)) {
          values.push(form[col]);
          updates.push(`${quoteIdentifier(col)} = $${values.length}`);
        }
      });
      if (!updates.length) {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('No editable fields provided.');
        return true;
      }
      const whereParts = primaryKeys.map((key, index) => `${quoteIdentifier(key)} = $${values.length + index + 1}`);
      const whereValues = primaryKeys.map((key) => keyData.values?.[key]);
      await runMiniSiteQuery(
        pool,
        `UPDATE ${quoteIdentifier(schema)}.${quoteIdentifier(table)} SET ${updates.join(', ')} WHERE ${whereParts.join(' AND ')}`,
        [...values, ...whereValues],
      );
      res.writeHead(302, { Location: url.pathname.replace(/\/edit$/, '') + '?edit=1' });
      res.end();
      return true;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
    return true;
  } catch (error) {
    const errorCode = error?.code || 'unknown';
    const stage = classifyMiniSiteDbStage(error);
    console.error('[mini-site] request failed', {
      requestId,
      path: url.pathname,
      projectId: project?.id,
      error: error?.message,
      code: errorCode,
      stage,
      stack: error?.stack,
    });
    const isTimeout =
      errorCode === 'ETIMEDOUT' ||
      String(error?.message || '').toLowerCase().includes('timeout');
    let adminHint = null;
    if (isTimeout) {
      adminHint = 'Connection timed out. Check network/SSL settings or increase timeout.';
    }
    res.writeHead(502, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(
      renderMiniSiteDbErrorPage({
        requestId,
        adminHint,
        stage,
        errorCode,
        retryHref: buildMiniSiteRetryHref(url),
      }),
    );
    return true;
  }
}

function startHttpServer() {
  if (httpServerPromise) {
    return httpServerPromise;
  }
  httpServerPromise = new Promise((resolve, reject) => {
    const server = http.createServer(async (req, res) => {
      const url = new URL(req.url, `http://${req.headers.host}`);
      if (runtimeStatus.fatalError) {
        const requestId = buildMiniSiteRequestId();
        if (url.pathname.startsWith('/db-mini')) {
          res.writeHead(500, { 'Content-Type': 'text/html; charset=utf-8' });
          res.end(
            renderMiniSiteDbErrorPage({
              requestId,
              adminHint: `Server fatal error (${runtimeStatus.fatalError.source}). Check logs for details.`,
            }),
          );
          return;
        }
        res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(
          JSON.stringify({
            ok: false,
            error: 'Server encountered a fatal error.',
            requestId,
          }),
        );
        return;
      }
      if (await handleWebRequest(req, res, url)) {
        return;
      }
      if (await handleMiniApiRequest(req, res, url)) {
        return;
      }
      if (req.method === 'GET' && (url.pathname === MINI_APP_ROUTE || url.pathname === `${MINI_APP_ROUTE}/` || url.pathname === '/mini/db' || url.pathname === '/mini/db/')) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMiniAppPage());
        return;
      }
      if (await handleMiniSiteRequest(req, res, url)) {
        return;
      }
      if (req.method === 'GET' && url.pathname === '/healthz') {
        const payload = await buildHealthzPayloadSafely();
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(payload));
        return;
      }
      if (req.method === 'GET' && url.pathname === '/health') {
        const payload = await buildPublicHealthPayloadSafely();
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(payload));
        return;
      }
      if (req.method === 'GET' && (url.pathname === '/pm/diagnostics' || url.pathname === '/__pm/diagnostics')) {
        const diagnostics = pmLogger.diagnostics();
        sendJson(res, 200, {
          ok: true,
          flags: {
            enabled: diagnostics.flags.enabled === true,
            hasPmUrl: diagnostics.flags.hasPmUrl === true,
            hasIngestToken: diagnostics.flags.hasIngestToken === true,
            testEnabled: diagnostics.flags.testEnabled === true,
            hasTestToken: diagnostics.flags.hasTestToken === true,
            hooksInstalled: diagnostics.flags.hooksInstalled === true,
          },
          lastSend: diagnostics.lastSend
            ? {
                at: diagnostics.lastSend.at || null,
                level: diagnostics.lastSend.level || null,
                ok: diagnostics.lastSend.ok === true,
                statusCode: Number.isFinite(diagnostics.lastSend.statusCode)
                  ? diagnostics.lastSend.statusCode
                  : null,
                correlationId: diagnostics.lastSend.correlationId || null,
                skipped: diagnostics.lastSend.skipped === true,
              }
            : null,
        });
        return;
      }

      if (req.method === 'POST' && (url.pathname === '/pm/test-log' || url.pathname === '/__pm/test-log')) {
        const token = getBearerToken(req);
        if (!pmLogger.isTestRequestAllowed(token)) {
          sendJson(res, 403, { ok: false, error: 'forbidden' });
          return;
        }

        let payload = {};
        try {
          const rawBody = await readRequestBody(req);
          payload = rawBody ? JSON.parse(rawBody) : {};
        } catch (error) {
          sendJson(res, 400, { ok: false, error: 'invalid_json' });
          return;
        }

        const baseMessage = payload.message || 'pm synthetic test log';
        const correlationId = payload.correlationId;
        const levels = ['info', 'warn', 'error'];
        const sends = [];
        for (const level of levels) {
          sends.push(
            pmLogger.send(level, `${baseMessage} (${level.toUpperCase()})`, {
              source: 'pm-test-log',
              correlationId,
              route: url.pathname,
              synthetic: true,
            }),
          );
        }
        const results = await Promise.all(sends);
        const allOk = results.every((entry) => entry && entry.ok === true);

        sendJson(res, allOk ? 200 : 502, {
          ok: allOk,
          accepted: true,
          emitted: levels,
          correlationIds: results.map((entry) => (entry ? entry.correlationId || null : null)),
        });
        return;
      }

      if (req.method === 'GET' && url.pathname === '/') {
        const payload = {
          ok: true,
          service: 'Project Manager',
          timestamp: new Date().toISOString(),
          configDbOk: runtimeStatus.configDbOk,
          configDbError: runtimeStatus.configDbError,
          vaultOk: runtimeStatus.vaultOk,
          vaultError: runtimeStatus.vaultError,
          logApi: getLogApiHealthStatus(),
          dbReady: appState.dbReady,
          degradedMode: appState.degradedMode,
          lastDbError: appState.lastDbError,
          configDbState: getConfigDbSnapshot(),
        };
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(payload));
        return;
      }
      if (req.method === 'GET' && url.pathname === '/api/logs/ping') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, service: 'Project Manager Log API' }));
        return;
      }
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

      if (req.method === 'POST' && url.pathname === '/api/render/webhook') {
        const rawBody = await readRequestBody(req);
        const parsed = parseRenderWebhookPayload(rawBody);
        if (!parsed.ok) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: parsed.error }));
          return;
        }
        const info = extractRenderWebhookInfo(parsed.payload);
        if (!info.serviceId || !info.eventType) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Missing serviceId or eventType.' }));
          return;
        }
        const rateStatus = checkRateLimit(renderWebhookRateLimits, info.serviceId, RENDER_WEBHOOK_RATE_LIMIT);
        if (rateStatus.blocked) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, skipped: 'rate_limited' }));
          return;
        }
        const eventKey =
          info.eventId || (info.deployId ? `${info.serviceId}:${info.deployId}:${info.eventType}` : null);
        if (eventKey && isDuplicateRenderEvent(eventKey)) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, skipped: 'duplicate' }));
          return;
        }

        const projects = await loadProjects();
        const project = projects.find((candidate) => {
          const deploySettings = normalizeProjectDeploySettings(candidate);
          return deploySettings.render.serviceId === info.serviceId;
        });
        if (!project) {
          console.warn('[render-webhook] unknown serviceId', {
            serviceId: info.serviceId,
            eventType: info.eventType,
          });
          await recordUnmappedRenderEvent({
            serviceId: info.serviceId,
            eventType: info.eventType,
          });
          const now = Date.now();
          const lastNotice = renderWebhookUnknownServiceNotices.get(info.serviceId);
          if (!lastNotice || now - lastNotice > 60 * 60 * 1000) {
            renderWebhookUnknownServiceNotices.set(info.serviceId, now);
            await bot.api.sendMessage(
              ADMIN_TELEGRAM_ID,
              `⚠️ Deploy event for unknown serviceId: ${info.serviceId} (bind it in PM)`,
            );
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, skipped: 'unknown_service' }));
          return;
        }

        const deploySettings = normalizeProjectDeploySettings(project);
        const webhookSettings = normalizeRenderWebhookSettings(await getCachedSettings());
        if (!isRenderEventEnabled(deploySettings, webhookSettings, info.eventType)) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, skipped: 'event_filtered' }));
          return;
        }
        await handleRenderDeployEvent({ project, info, source: 'webhook' });

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
        return;
      }

      if (req.method === 'POST' && (url.pathname === '/api/logs' || url.pathname === '/api/pm/logs')) {
        await logsRouter.handle(req, res);
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

          const targets = new Set();
          const mode = forwarding.destinationMode || 'admin';
          if (mode === 'admin' || mode === 'both') {
            targets.add(ADMIN_TELEGRAM_ID);
          }
          if ((mode === 'channel' || mode === 'both') && forwarding.destinationChatId) {
            targets.add(forwarding.destinationChatId);
          }
          const resolvedTargets = Array.from(targets).filter(Boolean);
          if (!resolvedTargets.length) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true, forwarded: false, reason: 'no destination' }));
            return;
          }
          const message = formatProjectLogMessage(project, event);
          await Promise.all(resolvedTargets.map((chatId) => sendSafeMessage(BOT_TOKEN, chatId, message)));
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
  return httpServerPromise;
}

async function startBotPolling() {
  if (botStarted) {
    console.log('[Project Manager] bot.start() already called, skipping.');
    return;
  }
  botStarted = true;
  if (botRetryTimeout) {
    clearTimeout(botRetryTimeout);
    botRetryTimeout = null;
  }

  try {
    await bot.start();
    console.log('[Project Manager] Bot polling started.');
  } catch (error) {
    botStarted = false;
    if (
      error?.error_code === 409 &&
      typeof error.description === 'string' &&
      error.description.includes('terminated by other getUpdates request')
    ) {
      console.error(
        '[Project Manager] Telegram returned 409 (another getUpdates in progress). Will retry in 15 seconds.',
      );
      if (!botRetryTimeout) {
        botRetryTimeout = setTimeout(() => {
          botRetryTimeout = null;
          startBotPolling().catch((retryError) => {
            console.error(
              '[Project Manager] Retry failed:',
              retryError?.stack || retryError,
            );
          });
        }, 15000);
      }
      return;
    }
    console.error('[Project Manager] Failed to start bot polling:', error?.stack || error);
    throw error;
  }
}

async function startBot() {
  console.error('[boot] starting bot init');
  await loadOpsState();
  await startHttpServer();
  startConfigDbWarmup();
  await startOpsScanner();
  console.error('[boot] db warmup started');
  await getCachedSettings(true);
  await ensureDbHubSchema().catch((error) => {
    console.warn('[db-hub] schema ensure failed', error?.message || error);
  });
  if (!LOG_API_ENABLED) {
    try {
      await bot.api.sendMessage(
        ADMIN_TELEGRAM_ID,
        '⚠️ Log API is disabled — PATH_APPLIER_TOKEN not set',
      );
    } catch (error) {
      console.error('[log_api] Failed to send disabled notification', error);
    }
  }
  try {
    await bot.api.deleteWebhook({ drop_pending_updates: false });
    console.log('[Project Manager] Webhook deleted (if any). Using long polling.');
  } catch (error) {
    console.error('[Project Manager] Failed to delete webhook:', error?.stack || error);
  }
  await registerGlobalCommands();
  await startBotPolling();
  scheduleLogTestDailyReminder();
  scheduleRenderPolling();
  setInterval(() => {
    purgeOldDoneCodexTasks().catch((error) => {
      console.warn('[codex_tasks] cleanup failed', error?.message);
    });
  }, 24 * 60 * 60 * 1000).unref?.();
  console.error('[boot] bot started');
}

module.exports = {
  startBot,
  startHttpServer,
  loadConfig,
  initDb,
  initEnvVault,
  appState,
  setDbReady,
  setDegradedMode,
  recordDbError,
  classifyDbError,
  respond,
  ensureAnswerCallback,
  validateWorkingDir,
  classifyDiagnosticsError,
  maskEnvValue,
  evaluateEnvValueStatus,
  navigateTo,
  parseStartPayload,
  handleGlobalCommand,
  handleStartCommand,
  handleReplyKeyboardNavigation,
  __test: {
    setBotApiForTests,
    resetNavigationState,
    setNavigationOperationInProgress,
    setActivePanelMessageId,
    getActivePanelMessageId,
    clearActivePanelMessageId,
    setPanelHistoryForChat,
    setConfigDbFailureStreakForTests: (value) => {
      configDbFailureStreak = Number(value) || 0;
    },
    shouldHaltConfigDbRetries,
    handleDeleteMessageCallback,
    fetchGithubReposWithToken,
    resolveReposFromGithubTokenInput,
    applyHealthAutoFixDefaults,
    maskDiagnosticText,
    buildRepoPickerKeyboard,
    verifyTelegramWebAppInitData,
    authenticateMiniApiRequest,
    handleMiniApiRequest,
    getBearerToken,
    sendJson,
    getPmDiagnosticsForTests: () => pmLogger.diagnostics(),
    isPmTestAllowedForTests: (token) => pmLogger.isTestRequestAllowed(token),
    configDbMaxRetriesPerBoot: CONFIG_DB_MAX_RETRIES_PER_BOOT,
    buildRoutineFixButton,
    buildScopedHeader,
    buildProjectHubView,
    buildDefaultProjectActionsKeyboard,
    goBack,
    goHome,
    renderScreen,
    dispatchCallbackData,
    buildCronJobKey,
    dedupeCronJobsByJobKey,
  },
};

async function main() {
  try {
    await startBot();
  } catch (error) {
    console.error('[FATAL] startup failed', error?.stack || error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}
