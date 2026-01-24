const PROJECT_TYPES = [
  {
    id: 'node-bot',
    label: 'Node Bot',
    required: [
      { key: 'PATH_APPLIER_LOG_INGEST_URL', label: 'Log ingest URL', notes: 'Path Applier log ingest endpoint.' },
      { key: 'PATH_APPLIER_LOG_INGEST_KEY', label: 'Log ingest key', notes: 'Shared secret for log ingest.' },
      { key: 'PROJECT_ID', label: 'Project ID', notes: 'Path Applier project identifier.' },
      { key: 'APP_ENV', label: 'App env', notes: 'Environment name (production/staging).' },
      { key: 'LOG_REPORTER_ENABLED', label: 'Log reporter enabled', notes: 'true/false.' },
      { key: 'LOG_LEVELS', label: 'Log levels', notes: 'Comma-separated levels.' },
    ],
    optional: [
      { key: 'GITHUB_TOKEN', label: 'GitHub token', notes: 'Optional GitHub access token.' },
      { key: 'CRON_API_TOKEN', label: 'Cron API token', notes: 'Optional Cron API token.' },
    ],
  },
  {
    id: 'node-api',
    label: 'Node API',
    required: [
      { key: 'APP_ENV', label: 'App env', notes: 'Environment name (production/staging).' },
      { key: 'LOG_LEVELS', label: 'Log levels', notes: 'Comma-separated levels.' },
    ],
    optional: [
      { key: 'GITHUB_TOKEN', label: 'GitHub token', notes: 'Optional GitHub access token.' },
      { key: 'CRON_API_TOKEN', label: 'Cron API token', notes: 'Optional Cron API token.' },
    ],
  },
  {
    id: 'python',
    label: 'Python',
    required: [
      { key: 'APP_ENV', label: 'App env', notes: 'Environment name (production/staging).' },
    ],
    optional: [
      { key: 'LOG_LEVELS', label: 'Log levels', notes: 'Comma-separated levels.' },
    ],
  },
  {
    id: 'generic',
    label: 'Generic',
    required: [],
    optional: [],
  },
  {
    id: 'daily_system',
    label: 'Daily System',
    required: [
      { key: 'SUPABASE_URL', label: 'Supabase URL', notes: 'Project REST base URL.' },
      { key: 'SUPABASE_SERVICE_ROLE_KEY', label: 'Supabase service role key', notes: 'Server-only key.' },
      { key: 'TELEGRAM_BOT_TOKEN', label: 'Telegram bot token', notes: 'Project bot token.' },
    ],
    optional: [
      { key: 'SUPABASE_ANON_KEY', label: 'Supabase anon key', notes: 'Client key if needed.' },
      { key: 'APP_BASE_URL', label: 'App base URL', notes: 'Public base URL.' },
      { key: 'WEBHOOK_SECRET', label: 'Webhook secret', notes: 'Validate incoming webhooks.' },
      { key: 'TZ', label: 'Timezone', notes: 'IANA timezone, e.g. Asia/Tehran.' },
      { key: 'NODE_ENV', label: 'Node env', notes: 'production' },
      { key: 'LOG_LEVEL', label: 'Log level', notes: 'info | warn | error' },
      { key: 'HEALTH_PORT', label: 'Health port', notes: 'Port for health checks.' },
    ],
  },
  {
    id: 'store_bot',
    label: 'Store Bot',
    required: [
      { key: 'SUPABASE_URL', label: 'Supabase URL', notes: 'Project REST base URL.' },
      { key: 'SUPABASE_SERVICE_ROLE_KEY', label: 'Supabase service role key', notes: 'Server-only key.' },
      { key: 'TELEGRAM_BOT_TOKEN', label: 'Telegram bot token', notes: 'Project bot token.' },
    ],
    optional: [
      { key: 'PAYMENT_PROVIDER_KEY', label: 'Payment provider key', notes: 'Zarinpal/Stripe key.' },
      { key: 'ZARINPAL_MERCHANT_ID', label: 'Zarinpal merchant ID', notes: 'If using Zarinpal.' },
      { key: 'STRIPE_SECRET_KEY', label: 'Stripe secret key', notes: 'If using Stripe.' },
      { key: 'ADMIN_TELEGRAM_ID', label: 'Admin Telegram ID', notes: 'Admin user id.' },
      { key: 'APP_BASE_URL', label: 'App base URL', notes: 'Public base URL.' },
      { key: 'WEBHOOK_SECRET', label: 'Webhook secret', notes: 'Validate webhooks.' },
      { key: 'LOG_LEVEL', label: 'Log level', notes: 'info | warn | error' },
      { key: 'TZ', label: 'Timezone', notes: 'IANA timezone, e.g. Asia/Tehran.' },
    ],
  },
  {
    id: 'trader_bot',
    label: 'Trader Bot',
    required: [
      { key: 'SUPABASE_URL', label: 'Supabase URL', notes: 'Project REST base URL.' },
      { key: 'TELEGRAM_BOT_TOKEN', label: 'Telegram bot token', notes: 'Project bot token.' },
    ],
    optional: [
      { key: 'DATABASE_URL', label: 'Database URL', notes: 'Postgres connection string.' },
      { key: 'EXCHANGE_API_KEY', label: 'Exchange API key', notes: 'Exchange auth key.' },
      { key: 'EXCHANGE_API_SECRET', label: 'Exchange API secret', notes: 'Exchange auth secret.' },
      { key: 'EXCHANGE_API_PASSPHRASE', label: 'Exchange API passphrase', notes: 'If required.' },
      { key: 'SYMBOLS_WHITELIST', label: 'Symbols whitelist', notes: 'Comma-separated.' },
      { key: 'RISK_MAX_DAILY_LOSS', label: 'Risk cap', notes: 'Max daily loss.' },
      { key: 'LOG_LEVEL', label: 'Log level', notes: 'info | warn | error' },
      { key: 'TZ', label: 'Timezone', notes: 'IANA timezone, e.g. Asia/Tehran.' },
      { key: 'APP_BASE_URL', label: 'App base URL', notes: 'Public base URL.' },
    ],
  },
  {
    id: 'generic_node',
    label: 'Generic Node',
    required: [
      { key: 'APP_BASE_URL', label: 'App base URL', notes: 'Public base URL.' },
    ],
    optional: [
      { key: 'NODE_ENV', label: 'Node env', notes: 'production' },
      { key: 'PORT', label: 'Port', notes: 'Service port.' },
      { key: 'LOG_LEVEL', label: 'Log level', notes: 'info | warn | error' },
      { key: 'TZ', label: 'Timezone', notes: 'IANA timezone.' },
    ],
  },
  {
    id: 'generic_python',
    label: 'Generic Python',
    required: [
      { key: 'APP_BASE_URL', label: 'App base URL', notes: 'Public base URL.' },
    ],
    optional: [
      { key: 'PYTHONUNBUFFERED', label: 'Python unbuffered', notes: '1 to disable buffering.' },
      { key: 'LOG_LEVEL', label: 'Log level', notes: 'info | warn | error' },
      { key: 'TZ', label: 'Timezone', notes: 'IANA timezone.' },
    ],
  },
  {
    id: 'other',
    label: 'Other / Custom',
    required: [],
    optional: [],
  },
];

const QUICK_KEYS = [
  'SUPABASE_URL',
  'SUPABASE_SERVICE_ROLE_KEY',
  'DATABASE_URL',
  'SUPABASE_DSN',
  'TELEGRAM_BOT_TOKEN',
  'APP_BASE_URL',
  'WEBHOOK_SECRET',
  'TZ',
  'LOG_LEVEL',
];

function getProjectTypeTemplate(typeId) {
  return PROJECT_TYPES.find((type) => type.id === typeId) || PROJECT_TYPES.find((t) => t.id === 'other');
}

function getProjectTypeOptions() {
  return PROJECT_TYPES.map((type) => ({ id: type.id, label: type.label }));
}

module.exports = {
  PROJECT_TYPES,
  QUICK_KEYS,
  getProjectTypeTemplate,
  getProjectTypeOptions,
};
