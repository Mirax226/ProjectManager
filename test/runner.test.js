const test = require('node:test');
const assert = require('node:assert/strict');
const { commandFor, resolveRepoPath, executeJob } = require('../src/runner/jobExecutor');

test('runner command construction is typed and does not invoke a shell', () => {
  assert.deepEqual(commandFor('RUN_TESTS', {}), ['npm.cmd', ['test', '--', '--run']]);
  assert.deepEqual(commandFor('RUN_TYPECHECK', {}), ['npm.cmd', ['run', 'typecheck']]);
  assert.throws(() => resolveRepoPath({ repoPath: 'C:\\safe\\repo' }, 'C:\\safe\\repo\\..\\other'), /not the configured/);
});

test('Codex task uses argument arrays and edit mode is disabled by default', async () => {
  const calls = [];
  const result = await executeJob({ type: 'CODEX_TASK', projectId: 'daily-system', payload: { task: 'inspect status; do not mutate', mode: 'read-only' } }, { profile: { repoPath: process.cwd() }, runCommand: async (...args) => { calls.push(args); return { ok: true, exitCode: 0, stdout: 'ok', stderr: '' }; } });
  assert.equal(result.ok, true); assert.equal(calls[0][2], process.cwd()); assert.equal(calls[0][1].includes('inspect status; do not mutate'), true);
  const edit = await executeJob({ type: 'CODEX_TASK', projectId: 'daily-system', payload: { task: 'edit', mode: 'edit' } }, { profile: { repoPath: process.cwd() }, runCommand: async () => ({ ok: true }) });
  assert.equal(edit.ok, false);
});
