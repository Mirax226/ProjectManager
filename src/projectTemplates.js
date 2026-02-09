const BUILTIN_TEMPLATES = [
  {
    id: 'node-telegram-bot-render',
    name: 'Node Telegram Bot (Render-friendly)',
    stack: 'node',
    defaults: {
      startCommand: 'npm start',
      testCommand: 'npm test',
      diagnosticCommand: 'npm run health',
      healthPath: '/health',
      portMode: 'auto',
      envKeysSuggested: ['BOT_TOKEN', 'ADMIN_TELEGRAM_ID', 'DATABASE_URL'],
      cronPresets: [],
    },
  },
  {
    id: 'node-worker',
    name: 'Node Worker (no HTTP) with minimal health shim',
    stack: 'worker',
    defaults: {
      startCommand: 'node worker.js',
      testCommand: 'npm test',
      diagnosticCommand: 'node scripts/diag.js',
      healthPath: '/health',
      portMode: 'shim',
      envKeysSuggested: ['QUEUE_URL'],
      cronPresets: [],
    },
  },
  {
    id: 'simple-express-api',
    name: 'Simple API (Express) with /health',
    stack: 'api',
    defaults: {
      startCommand: 'node server.js',
      testCommand: 'npm test',
      diagnosticCommand: 'npm run diag',
      healthPath: '/health',
      portMode: 'http',
      envKeysSuggested: ['PORT'],
      cronPresets: [],
    },
  },
  {
    id: 'cron-only',
    name: 'Cron-only job (with ping/heartbeat)',
    stack: 'worker',
    defaults: {
      startCommand: 'node cron.js',
      testCommand: 'npm test',
      diagnosticCommand: 'node scripts/heartbeat.js',
      healthPath: '/health',
      portMode: 'none',
      envKeysSuggested: ['HEARTBEAT_URL'],
      cronPresets: [{ name: 'heartbeat', schedule: '*/5 * * * *' }],
    },
  },
];

function listTemplates() { return BUILTIN_TEMPLATES.slice(); }
function getTemplate(id) { return BUILTIN_TEMPLATES.find((item) => item.id === id) || null; }

module.exports = { listTemplates, getTemplate };
