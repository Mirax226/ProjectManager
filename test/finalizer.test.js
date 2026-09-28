const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pj011a-finalizer-'));
  fs.mkdirSync(path.join(root, 'tools'), { recursive: true });
  fs.copyFileSync(path.join(__dirname, '..', 'tools', 'finalize-task.ps1'), path.join(root, 'tools', 'finalize-task.ps1'));
  fs.mkdirSync(path.join(root, 'docs', 'project-plan', 'versions', 'PJ-011A'), { recursive: true });
  fs.mkdirSync(path.join(root, 'docs', 'handoffs', 'PJ-011A'), { recursive: true });
  fs.writeFileSync(path.join(root, 'docs', 'project-plan', 'PJ_CURRENT_PLAN.md'), 'plan');
  fs.writeFileSync(path.join(root, 'docs', 'project-plan', 'versions', 'PJ-011A', 'CHANGELOG.md'), 'change');
  fs.writeFileSync(path.join(root, 'docs', 'handoffs', 'PJ-011A', 'REVIEW.md'), 'review');
  return root;
}
function run(root, extra = []) {
  return spawnSync('powershell.exe', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', path.join(root, 'tools', 'finalize-task.ps1'), '-Project', 'PJ', '-Task', 'PJ-011A', '-DryRun', ...extra], { encoding: 'utf8' });
}

test('Finalizer dry-run validates sources and performs no destination writes', () => {
  const root = fixture(); const plans = path.join(root, 'out-plans'); const review = path.join(root, 'out-review');
  const result = run(root, ['-PlansRoot', plans, '-ReviewRoot', review]);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /DRY RUN/);
  assert.equal(fs.existsSync(plans), false);
  assert.equal(fs.existsSync(review), false);
});

test('Finalizer rejects missing REVIEW.md and forbidden workbook evidence', () => {
  const missing = fixture(); fs.unlinkSync(path.join(missing, 'docs', 'handoffs', 'PJ-011A', 'REVIEW.md'));
  assert.notEqual(run(missing).status, 0);
  const forbidden = fixture(); fs.writeFileSync(path.join(forbidden, 'docs', 'handoffs', 'PJ-011A', 'evidence.xlsx'), 'forbidden');
  const result = run(forbidden);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /Forbidden review\s+artifact/);
});
