const DEFAULT_ACTIONS = [];

const RULE_GITHUB_CAPABILITY = Object.freeze({
  WORKDIR_OUTSIDE_REPO: {
    githubImpact: true,
    githubActions: ['edit_file', 'create_pr'],
  },
  HEALTH_MISCONFIG: {
    githubImpact: true,
    githubActions: ['edit_file', 'create_pr'],
  },
  REPO_INSPECTION_MISSING_FEATURES: {
    githubImpact: true,
    githubActions: ['edit_file', 'create_pr'],
  },
  DRIFT_BASELINE_MISSING: {
    githubImpact: true,
    githubActions: ['replace_text', 'create_pr'],
  },
  SCHEMA_RUNNER_REGRESSION: {
    githubImpact: true,
    githubActions: ['edit_file', 'create_pr'],
  },
});

function resolveGithubCapability(ruleId) {
  const mapped = RULE_GITHUB_CAPABILITY[String(ruleId || '')];
  if (!mapped) return { githubImpact: false, githubActions: DEFAULT_ACTIONS };
  return {
    githubImpact: true,
    githubActions: Array.from(new Set(mapped.githubActions || DEFAULT_ACTIONS)),
  };
}

module.exports = {
  RULE_GITHUB_CAPABILITY,
  resolveGithubCapability,
};
