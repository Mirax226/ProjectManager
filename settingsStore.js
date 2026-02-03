const fs = require('fs/promises');
const path = require('path');
const configStore = require('./configStore');
const { forwardSelfLog } = require('./logger');

const SETTINGS_FILE = path.join(__dirname, 'globalSettings.json');
const ENV_DEFAULT_BASE = process.env.DEFAULT_BASE_BRANCH || 'main';
const DEFAULT_CRON_SETTINGS = { enabled: true, defaultTimezone: 'UTC' };
const DEFAULT_GLOBAL_SETTINGS = {
  defaultBaseBranch: ENV_DEFAULT_BASE,
  defaultProjectId: undefined,
  selfLogForwarding: { enabled: false, levels: ['error'], targetChatId: null },
  uiCleanup: {
    autoCleanMenus: true,
    ephemeralTtlSec: 30,
    keepLastPanels: 1,
  },
  security: {
    adminIds: [],
    miniSiteSessionTtlMinutes: 10,
    envMaskPolicy: 'strict',
  },
  logs: {
    defaultLevels: ['error'],
    allowedProjectsMode: 'allow-all',
  },
  integrations: {
    baseUrlOverride: '',
    healthPingIntervalMinutes: 5,
  },
  renderWebhook: {
    webhookId: null,
    targetUrl: '',
    events: ['deploy_started', 'deploy_ended'],
    lastVerifiedAt: null,
  },
  renderDeploy: {
    pollingEnabledGlobal: true,
    webhookEnabledGlobal: true,
    pollIntervalSec: 60,
    pollMaxServicesPerTick: 10,
    pollTimeoutMs: 8000,
    workspaceId: null,
    unmappedEvents: [],
  },
  backups: {
    channelId: '',
    captionTemplate:
      'Project: {projectName} ({projectId})\\nNote: {title}\\nCategory: {category}\\nStatus: {status}\\nCreated: {createdAt}\\nNoteId: {noteId}',
  },
};

function applySettingsDefaults(settings) {
  const payload = settings && typeof settings === 'object' ? settings : {};
  return {
    ...DEFAULT_GLOBAL_SETTINGS,
    ...payload,
    selfLogForwarding: {
      ...DEFAULT_GLOBAL_SETTINGS.selfLogForwarding,
      ...(payload.selfLogForwarding || {}),
    },
    uiCleanup: {
      ...DEFAULT_GLOBAL_SETTINGS.uiCleanup,
      ...(payload.uiCleanup || {}),
    },
    security: {
      ...DEFAULT_GLOBAL_SETTINGS.security,
      ...(payload.security || {}),
    },
    logs: {
      ...DEFAULT_GLOBAL_SETTINGS.logs,
      ...(payload.logs || {}),
    },
    integrations: {
      ...DEFAULT_GLOBAL_SETTINGS.integrations,
      ...(payload.integrations || {}),
    },
    renderWebhook: {
      ...DEFAULT_GLOBAL_SETTINGS.renderWebhook,
      ...(payload.renderWebhook || {}),
    },
    renderDeploy: {
      ...DEFAULT_GLOBAL_SETTINGS.renderDeploy,
      ...(payload.renderDeploy || {}),
    },
    backups: {
      ...DEFAULT_GLOBAL_SETTINGS.backups,
      ...(payload.backups || {}),
    },
  };
}

async function ensureSettingsFile() {
  try {
    await fs.access(SETTINGS_FILE);
  } catch (err) {
    await fs.writeFile(
      SETTINGS_FILE,
      JSON.stringify({ defaultBaseBranch: ENV_DEFAULT_BASE }, null, 2),
      'utf-8',
    );
  }
}

async function loadGlobalSettings() {
  const dbSettings = await configStore.loadGlobalSettings();
  if (dbSettings) {
    const hydrated = applySettingsDefaults(dbSettings);
    if (JSON.stringify(hydrated) !== JSON.stringify(dbSettings)) {
      await configStore.saveGlobalSettings(hydrated);
    }
    return hydrated;
  }

  await ensureSettingsFile();
  try {
    const raw = await fs.readFile(SETTINGS_FILE, 'utf-8');
    const parsed = applySettingsDefaults(JSON.parse(raw));
    if (parsed && !dbSettings) {
      await configStore.saveGlobalSettings(parsed);
    }
    return parsed;
  } catch (error) {
    console.error('Failed to load globalSettings.json', error);
    return applySettingsDefaults({});
  }
}

async function saveGlobalSettings(settings) {
  try {
    const payload = applySettingsDefaults(settings);
    await configStore.saveGlobalSettings(payload);
    await fs.writeFile(SETTINGS_FILE, JSON.stringify(payload, null, 2), 'utf-8');
  } catch (error) {
    console.error('Failed to save globalSettings.json', error);
    await forwardSelfLog('error', 'Failed to save global settings file', {
      stack: error?.stack,
      context: { error: error?.message },
    });
    throw error;
  }
}

module.exports = {
  SETTINGS_FILE,
  loadGlobalSettings,
  saveGlobalSettings,
  loadCronSettings,
  saveCronSettings,
};

async function loadCronSettings() {
  const settings = await configStore.loadJson('cronSettings');
  if (!settings || typeof settings !== 'object') {
    return { ...DEFAULT_CRON_SETTINGS };
  }
  return {
    enabled: typeof settings.enabled === 'boolean' ? settings.enabled : DEFAULT_CRON_SETTINGS.enabled,
    defaultTimezone: settings.defaultTimezone || DEFAULT_CRON_SETTINGS.defaultTimezone,
  };
}

async function saveCronSettings(settings) {
  const payload = {
    enabled: typeof settings?.enabled === 'boolean' ? settings.enabled : DEFAULT_CRON_SETTINGS.enabled,
    defaultTimezone: settings?.defaultTimezone || DEFAULT_CRON_SETTINGS.defaultTimezone,
  };
  try {
    await configStore.saveJson('cronSettings', payload);
  } catch (error) {
    console.error('[configStore] Failed to save cronSettings', error);
    await forwardSelfLog('error', 'Failed to save cron settings', {
      stack: error?.stack,
      context: { error: error?.message },
    });
    throw error;
  }
}
