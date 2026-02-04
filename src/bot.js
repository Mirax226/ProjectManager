console.error('[boot] starting app');

process.on('uncaughtException', (error) => {
  console.error('[FATAL] uncaughtException', error);
});

process.on('unhandledRejection', (error) => {
  console.error('[FATAL] unhandledRejection', error);
});

process.on('exit', (code) => {
  console.error('[boot] process exit', code);
});

async function main() {
  console.error('[env]', {
    BOT_TOKEN: Boolean(process.env.BOT_TOKEN),
    DB: Boolean(process.env.DATABASE_URL),
    VAULT: Boolean(process.env.ENV_VAULT_MASTER_KEY),
  });

  const {
    startBot,
    loadConfig,
    initDb,
    initEnvVault,
    startHttpServer,
    setDbReady,
    setDegradedMode,
    recordDbError,
    classifyDbError,
  } = require('../bot.js');

  await startHttpServer();
  const retryDelaysMs = [500, 1000, 2000, 4000, 8000];
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  let configReady = false;
  for (let attempt = 0; attempt < retryDelaysMs.length; attempt += 1) {
    const attemptNumber = attempt + 1;
    try {
      const configOk = await loadConfig();
      const dbStatus = await initDb();
      configReady = Boolean(configOk && dbStatus?.ok);
      if (configReady) {
        setDbReady(true);
        setDegradedMode(false);
        break;
      }
      if (dbStatus?.message) {
        const category = classifyDbError(new Error(dbStatus.message));
        if (category) {
          recordDbError(category, dbStatus.message);
        }
      }
    } catch (error) {
      const category = classifyDbError(error);
      if (category) {
        recordDbError(category, error?.message);
      }
      console.warn('[boot] config init attempt failed', {
        attempt: attemptNumber,
        error: error?.message || 'unknown error',
      });
    }
    const delay = retryDelaysMs[attempt];
    console.warn(`[boot] config init not ready; retrying in ${delay}ms (attempt ${attemptNumber}/5)`);
    await sleep(delay);
  }

  if (!configReady) {
    setDbReady(false);
    setDegradedMode(true);
    console.error('[boot] Config DB unavailable after retries. Starting in degraded mode.');
  }

  try {
    await initEnvVault();
  } catch (error) {
    console.error('[WARN] Env Vault init failed', error?.stack || error);
  }

  await startBot();

  console.error('[boot] bot started');

  if (process.env.NODE_ENV === 'production') {
    setInterval(() => {
      console.debug('[keepalive] alive');
    }, 60_000);
  }
}

main().catch((error) => {
  console.error('[FATAL] startup failed', error?.stack || error);
  process.exit(1);
});
