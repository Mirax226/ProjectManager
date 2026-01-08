const { getConfigDbPool } = require('./configDb');
const { encryptSecret, decryptSecret } = require('./envVaultCrypto');
const { forwardSelfLog } = require('./logger');

let tablesReady = false;
const memory = {
  bots: [],
};

async function ensureTelegramBotTable(db) {
  if (!db || tablesReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS project_telegram_bots (
      project_id TEXT PRIMARY KEY,
      bot_token_enc TEXT NULL,
      webhook_url TEXT NULL,
      webhook_path TEXT NULL,
      last_set_at TIMESTAMPTZ NULL,
      last_test_at TIMESTAMPTZ NULL,
      last_test_status TEXT NULL,
      enabled BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  tablesReady = true;
}

async function getDb() {
  const db = await getConfigDbPool();
  if (db) {
    await ensureTelegramBotTable(db);
  }
  return db;
}

function normalizeBotRow(row) {
  if (!row) return null;
  return {
    projectId: row.project_id,
    botTokenEnc: row.bot_token_enc,
    webhookUrl: row.webhook_url,
    webhookPath: row.webhook_path,
    lastSetAt: row.last_set_at,
    lastTestAt: row.last_test_at,
    lastTestStatus: row.last_test_status,
    enabled: row.enabled === true,
  };
}

async function getProjectTelegramBot(projectId) {
  const db = await getDb();
  if (!db) {
    return memory.bots.find((bot) => bot.projectId === projectId) || null;
  }
  const { rows } = await db.query(
    'SELECT * FROM project_telegram_bots WHERE project_id = $1 LIMIT 1',
    [projectId],
  );
  return normalizeBotRow(rows[0]);
}

async function upsertTelegramBotToken(projectId, token) {
  const tokenEnc = encryptSecret(token);
  const db = await getDb();
  if (!db) {
    const existingIndex = memory.bots.findIndex((bot) => bot.projectId === projectId);
    const payload = {
      projectId,
      botTokenEnc: tokenEnc,
      enabled: true,
      updatedAt: new Date().toISOString(),
    };
    if (existingIndex === -1) {
      memory.bots.push(payload);
    } else {
      memory.bots[existingIndex] = { ...memory.bots[existingIndex], ...payload };
    }
    return payload;
  }
  try {
    await db.query(
      `
        INSERT INTO project_telegram_bots (project_id, bot_token_enc, enabled)
        VALUES ($1, $2, true)
        ON CONFLICT (project_id)
        DO UPDATE SET bot_token_enc = EXCLUDED.bot_token_enc, enabled = true, updated_at = NOW()
      `,
      [projectId, tokenEnc],
    );
  } catch (error) {
    console.error('[telegramBotStore] Failed to save bot token', error);
    await forwardSelfLog('error', 'Failed to save telegram bot token', {
      stack: error?.stack,
      context: { error: error?.message, projectId },
    });
    throw error;
  }
  return getProjectTelegramBot(projectId);
}

async function clearTelegramBotToken(projectId) {
  const db = await getDb();
  if (!db) {
    const existingIndex = memory.bots.findIndex((bot) => bot.projectId === projectId);
    if (existingIndex === -1) return false;
    memory.bots[existingIndex] = {
      ...memory.bots[existingIndex],
      botTokenEnc: null,
      enabled: false,
      updatedAt: new Date().toISOString(),
    };
    return true;
  }
  const result = await db.query(
    `
      UPDATE project_telegram_bots
      SET bot_token_enc = NULL, enabled = false, updated_at = NOW()
      WHERE project_id = $1
    `,
    [projectId],
  );
  return result.rowCount > 0;
}

async function updateTelegramWebhook(projectId, payload) {
  const db = await getDb();
  const updates = {
    webhookUrl: payload.webhookUrl || null,
    webhookPath: payload.webhookPath || null,
    lastSetAt: payload.lastSetAt || new Date().toISOString(),
    enabled: payload.enabled !== false,
  };
  if (!db) {
    const existingIndex = memory.bots.findIndex((bot) => bot.projectId === projectId);
    const next = {
      ...(existingIndex === -1 ? { projectId } : memory.bots[existingIndex]),
      ...updates,
    };
    if (existingIndex === -1) {
      memory.bots.push(next);
    } else {
      memory.bots[existingIndex] = next;
    }
    return next;
  }

  try {
    await db.query(
      `
        INSERT INTO project_telegram_bots (project_id, webhook_url, webhook_path, last_set_at, enabled)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (project_id)
        DO UPDATE SET webhook_url = EXCLUDED.webhook_url,
          webhook_path = EXCLUDED.webhook_path,
          last_set_at = EXCLUDED.last_set_at,
          enabled = EXCLUDED.enabled,
          updated_at = NOW()
      `,
      [projectId, updates.webhookUrl, updates.webhookPath, updates.lastSetAt, updates.enabled],
    );
  } catch (error) {
    console.error('[telegramBotStore] Failed to update webhook info', error);
    await forwardSelfLog('error', 'Failed to update telegram webhook info', {
      stack: error?.stack,
      context: { error: error?.message, projectId },
    });
    throw error;
  }

  return getProjectTelegramBot(projectId);
}

async function updateTelegramTestStatus(projectId, status) {
  const db = await getDb();
  const payload = {
    lastTestAt: new Date().toISOString(),
    lastTestStatus: status || null,
  };
  if (!db) {
    const existingIndex = memory.bots.findIndex((bot) => bot.projectId === projectId);
    const next = {
      ...(existingIndex === -1 ? { projectId } : memory.bots[existingIndex]),
      ...payload,
    };
    if (existingIndex === -1) {
      memory.bots.push(next);
    } else {
      memory.bots[existingIndex] = next;
    }
    return next;
  }

  await db.query(
    `
      INSERT INTO project_telegram_bots (project_id, last_test_at, last_test_status)
      VALUES ($1, $2, $3)
      ON CONFLICT (project_id)
      DO UPDATE SET last_test_at = EXCLUDED.last_test_at,
        last_test_status = EXCLUDED.last_test_status,
        updated_at = NOW()
    `,
    [projectId, payload.lastTestAt, payload.lastTestStatus],
  );

  return getProjectTelegramBot(projectId);
}

async function getTelegramBotToken(projectId) {
  const record = await getProjectTelegramBot(projectId);
  if (!record?.botTokenEnc) return null;
  return decryptSecret(record.botTokenEnc);
}

module.exports = {
  getProjectTelegramBot,
  getTelegramBotToken,
  upsertTelegramBotToken,
  clearTelegramBotToken,
  updateTelegramWebhook,
  updateTelegramTestStatus,
};
