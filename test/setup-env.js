process.env.BOT_TOKEN ||= 'TEST';
process.env.ADMIN_TELEGRAM_ID ||= '1';
// Each node:test worker owns its checkout root; never delete shared C:\tmp
// fixtures from parallel tests or another run.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
process.env.WORKDIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pj-test-workdir-'));
