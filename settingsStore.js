const fs = require('fs/promises');
const path = require('path');

const SETTINGS_FILE = path.join(__dirname, 'globalSettings.json');
const ENV_DEFAULT_BASE = process.env.DEFAULT_BASE_BRANCH || 'main';

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
  await ensureSettingsFile();
  try {
    const raw = await fs.readFile(SETTINGS_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    if (!parsed.defaultBaseBranch) {
      parsed.defaultBaseBranch = ENV_DEFAULT_BASE;
    }
    return parsed;
  } catch (error) {
    console.error('Failed to load globalSettings.json', error);
    return { defaultBaseBranch: ENV_DEFAULT_BASE };
  }
}

async function saveGlobalSettings(settings) {
  try {
    const payload = { ...settings };
    await fs.writeFile(SETTINGS_FILE, JSON.stringify(payload, null, 2), 'utf-8');
  } catch (error) {
    console.error('Failed to save globalSettings.json', error);
    throw error;
  }
}

module.exports = {
  SETTINGS_FILE,
  loadGlobalSettings,
  saveGlobalSettings,
};
