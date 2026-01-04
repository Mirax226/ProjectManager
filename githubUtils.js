const { Octokit } = require('@octokit/rest');

const githubToken = process.env.GITHUB_TOKEN;

function getOctokit() {
  if (!githubToken) {
    throw new Error('Missing GITHUB_TOKEN');
  }
  return new Octokit({ auth: githubToken });
}

async function createPullRequest({ owner, repo, baseBranch, headBranch, title, body }) {
  const octokit = getOctokit();
  const response = await octokit.pulls.create({
    owner,
    repo,
    base: baseBranch,
    head: headBranch,
    title,
    body,
  });
  return response.data;
}

async function measureGithubLatency() {
  const octokit = getOctokit();
  const start = Date.now();
  await octokit.rateLimit.get();
  const end = Date.now();
  return end - start;
}

module.exports = {
  createPullRequest,
  measureGithubLatency,
};
