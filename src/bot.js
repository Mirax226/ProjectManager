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
    initEnvVault,
  } = require('../bot.js');

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
