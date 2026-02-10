const test = require('node:test');
const assert = require('node:assert/strict');

const { renderMatch } = require('../src/routineFixes');
const { resolveGithubCapability } = require('../src/routineFixes/githubMapping');

test('routine rule github mapping resolves expected capabilities', () => {
  const cap = resolveGithubCapability('REPO_INSPECTION_MISSING_FEATURES');
  assert.equal(cap.githubImpact, true);
  assert.deepEqual(cap.githubActions, ['edit_file', 'create_pr']);

  const none = resolveGithubCapability('GENERIC_HANDLER');
  assert.equal(none.githubImpact, false);
  assert.deepEqual(none.githubActions, []);
});

test('rendered routine output includes github metadata', () => {
  const rule = {
    id: 'WORKDIR_OUTSIDE_REPO',
    title: 'Working directory invalid',
    render() {
      return {
        diagnosis: ['bad dir'],
        steps: ['fix'],
        task: 'task',
      };
    },
  };

  const output = renderMatch({ rule, confidence: 0.92, fields: {} });
  assert.equal(output.githubImpact, true);
  assert.deepEqual(output.githubActions, ['edit_file', 'create_pr']);
});
