const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { dispatchTelegramUpdate } = require('../src/telegramApplication');
const { ControlPlaneStore } = require('../src/controlPlane/store');
const env = { TELEGRAM_ADMIN_USER_IDS: '843686302' };
const message = (text, id = 843686302) => ({ update_id: 1, message: { text, from: { id }, chat: { id, type: 'private' } } });
test('start/admin omit command footer while preserving status and emoji callbacks', async () => {
  for (const text of ['/start', '/admin']) {
    const output = await dispatchTelegramUpdate(message(text), { store: new ControlPlaneStore(), env });
    assert.equal(output.authorized, true);
    assert.match(output.text, /ProjectManager Admin/); assert.match(output.text, /Archive destination/);
    for (const footer of ['Start: /start', 'Health: /health', 'Project Status: /project_status', 'Excel Health: /excel_healthcheck']) assert.equal(output.text.includes(footer), false);
    const buttons = output.replyMarkup.inline_keyboard.flat();
    assert.deepEqual(buttons.map((button) => button.callback_data), ['status', 'runners', 'incidents', 'jobs']);
    assert.deepEqual(buttons.map((button) => button.text), ['📊 Status', '🖥️ Runners', '🚨 Incidents', '📋 Jobs']);
  }
});
test('BotFather diagnostic commands remain routed after footer removal', async () => {
  for (const [command, type] of [['/health daily-system', 'HEALTHCHECK'], ['/project_status daily-system', 'PROJECT_STATUS'], ['/excel_healthcheck daily-system mirax', 'EXCEL_HEALTHCHECK']]) {
    const store = new ControlPlaneStore();
    const result = await dispatchTelegramUpdate(message(command), { store, env });
    assert.equal(result.authorized, true); assert.match(result.text, new RegExp(`Queued ${type}`));
    assert.equal(store.listJobs()[0].type, type);
  }
});
test('status message and unchanged status callback return the same authorized view', async () => {
  const store = new ControlPlaneStore();
  const status = await dispatchTelegramUpdate(message('/status'), { store, env });
  const callback = await dispatchTelegramUpdate({ update_id: 2, callback_query: { id: 'fixture-callback', from: { id: 843686302 }, message: { chat: { id: 843686302, type: 'private' } }, data: 'status' } }, { store, env });
  assert.match(status.text, /📊 ProjectManager/); assert.equal(callback.text, status.text); assert.equal(callback.callbackId, 'fixture-callback');
  assert.equal(store.listJobs().length, 0);
});
test('emoji menus do not change admin authorization or private-chat restriction', async () => {
  const store = new ControlPlaneStore();
  assert.equal((await dispatchTelegramUpdate(message('/start', 456), { store, env })).authorized, false);
  const group = message('/start'); group.message.chat.type = 'group';
  assert.equal((await dispatchTelegramUpdate(group, { store, env })).authorized, false);
  assert.equal((await dispatchTelegramUpdate({ callback_query: { id: 'bad', data: 'status', from: { id: 456 }, message: { chat: { id: 843686302, type: 'private' } } } }, { store, env })).authorized, false);
});
const helper = path.resolve(__dirname, '../docs/handoffs/PJ-012/OWNER-FINAL-ACTIVATION.ps1');
function mockHelper(failInfo) {
  // Execute actual PowerShell control flow with every external operation mocked.
  const script = `
    function npx { process { $global:LASTEXITCODE=0; if ($args -contains 'whoami') { '{"loggedIn":true,"accounts":[{"id":"9f12f5d584ab6b4bd94c66d4bf7f53dc"}]}' } } }
    function Invoke-RestMethod { @{ok=$true;d1='available'} }
    function Read-Host { ConvertTo-SecureString 'fixture-private-token' -AsPlainText -Force }
    function npm { if (($args -contains '--info') -and ${failInfo ? '$true' : '$false'}) { $global:LASTEXITCODE=1 } else { $global:LASTEXITCODE=0; '{"ok":true,"stage":"SUCCESS","allowedUpdates":["message","callback_query"]}' } }
    function node { throw 'Fragile duplicate node/eval path invoked' }
    try { & '${helper.replace(/'/g, "''")}' -Webhook; 'HELPER_SUCCESS'; exit 0 } catch { 'SAFE_HELPER_FAILURE'; $_.Exception.Message; exit 1 }
  `;
  const childEnv = { ...process.env };
  // Windows PowerShell must use its own module paths, not inherited PowerShell 7 paths.
  for (const key of Object.keys(childEnv)) if (key.toLowerCase() === 'psmodulepath') delete childEnv[key];
  return execFileSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-EncodedCommand', Buffer.from(script, 'utf16le').toString('base64')], { encoding: 'utf8', windowsHide: true, timeout: 20000, env: childEnv, stdio: ['pipe', 'pipe', 'pipe'] });
}
test('owner helper completes after accepted set and verified info without eval false failure', () => {
  const output = mockHelper(false); assert.match(output, /HELPER_SUCCESS/); assert.equal(output.includes('fixture-private-token'), false);
});
test('owner helper exits nonzero if existing info verification fails', () => {
  assert.throws(() => mockHelper(true), (error) => { assert.equal(error.status, 1); assert.match(error.stdout, /SAFE_HELPER_FAILURE/); assert.equal(error.stdout.includes('fixture-private-token'), false); return true; });
});
