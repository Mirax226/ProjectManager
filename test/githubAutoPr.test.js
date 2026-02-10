const test = require('node:test');
const assert = require('node:assert/strict');

const { createAutoPrForSuggestion } = require('../src/githubAutoPr');
const { createOpsTimelineStore } = require('../src/opsTimeline');

function makeGithubStub(overrides = {}) {
  return {
    async createBranch() {},
    async applyPatchPlan() { return { ok: true, filesChanged: 2 }; },
    async commit() { return { sha: 'abc123' }; },
    async createPullRequest() { return { number: 42, url: 'https://example/pr/42' }; },
    ...overrides,
  };
}

test('auto pr happy path creates PR and timeline event', async () => {
  const timeline = createOpsTimelineStore();
  const github = makeGithubStub();
  const result = await createAutoPrForSuggestion({
    project: { id: 'p1', repoSlug: 'acme/repo', defaultBranch: 'main' },
    suggestion: {
      ruleId: 'HEALTH_MISCONFIG',
      title: 'Missing health endpoint',
      patchOperations: [{ op: 'edit_file', file: 'server.js' }],
      problemSummary: 'Health route missing',
      fixSummary: 'Adds /health route',
    },
    actor: '1',
    hasAccess: true,
    timeline,
    github,
    mode: 'pr',
    shadowConfirmed: true,
  });
  assert.equal(result.mode, 'pr');
  assert.equal(result.prNumber, 42);

  const events = timeline.query({ scope: 'project', projectId: 'p1' }).items;
  assert.equal(events[0].type, 'github.auto_pr_created');
});

test('auto pr enforces permissions and shadow run', async () => {
  await assert.rejects(
    createAutoPrForSuggestion({ hasAccess: false }),
    /github_access_denied/
  );

  await assert.rejects(
    createAutoPrForSuggestion({ hasAccess: true, shadowConfirmed: false }),
    /shadow_run_required/
  );
});
