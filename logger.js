const { normalizeLogLevel } = require('./logLevels');
const { addSelfLog } = require('./loggerStore');
const { appState } = require('./appState');

let botInstance = null;
let adminTelegramId = null;
let loadGlobalSettings = null;
const SELF_LOG_FORWARDING = String(process.env.SELF_LOG_FORWARDING || 'enabled').toLowerCase();
const SELF_LOG_FALLBACK = String(process.env.SELF_LOG_FALLBACK || 'console').toLowerCase();

function configureSelfLogger({ bot, adminId, loadSettings }) {
  botInstance = bot;
  adminTelegramId = adminId;
  loadGlobalSettings = loadSettings;
}

function normalizeLogLevels(levels) {
  if (!Array.isArray(levels)) return [];
  return levels.map((level) => normalizeLogLevel(level)).filter(Boolean);
}

function truncateText(value, limit) {
  if (!value) return '';
  const text = String(value);
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 1))}…`;
}

function formatContext(context, limit) {
  if (!context || typeof context !== 'object') return '';
  try {
    return truncateText(JSON.stringify(context), limit);
  } catch (error) {
    return '';
  }
}

async function forwardSelfLog(level, message, options = {}) {
  try {
    if (appState.dbReady && !appState.degradedMode) {
      await addSelfLog({
        level,
        message,
        stack: options.stack,
        context: options.context,
      });
    }

    if (SELF_LOG_FORWARDING === 'disabled') return;

    const normalizedLevel = normalizeLogLevel(level) || 'error';
    const timestamp = new Date().toISOString();
    const lines = [
      `⚠️ [${normalizedLevel.toUpperCase()}] Project Manager`,
      `Time: ${timestamp}`,
      `Message: ${truncateText(message, 1200) || '(no message)'}`,
    ];

    if (options.stack) {
      lines.push(`Stack: ${truncateText(options.stack, 800)}`);
    }

    const contextText = formatContext(options.context, 600);
    if (contextText) {
      lines.push(`Context: ${contextText}`);
    }

    if (!appState.dbReady || appState.degradedMode) {
      if (SELF_LOG_FALLBACK === 'telegram' && botInstance && adminTelegramId) {
        await botInstance.api.sendMessage(adminTelegramId, lines.join('\n'), {
          disable_web_page_preview: true,
        });
        return;
      }
      console.error(lines.join('\n'));
      return;
    }

    if (!botInstance || !loadGlobalSettings) return;
    const settings = await loadGlobalSettings();
    const forwarding = settings?.selfLogForwarding || {};
    if (forwarding.enabled !== true) return;

    let allowedLevels = normalizeLogLevels(forwarding.levels);
    if (!allowedLevels.length) {
      allowedLevels = ['error'];
    }
    if (!allowedLevels.includes(normalizedLevel)) return;

    const targetChatId = forwarding.targetChatId || adminTelegramId;
    if (!targetChatId) return;

    await botInstance.api.sendMessage(targetChatId, lines.join('\n'), {
      disable_web_page_preview: true,
    });
  } catch (error) {
    console.error('[self-log] Failed to forward log', error);
  }
}

module.exports = {
  configureSelfLogger,
  forwardSelfLog,
};
