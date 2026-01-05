const { Octokit } = require('@octokit/rest');

// Helper: make a branch-safe name from arbitrary text
function makeBranchSlug(input) {
  return String(input)
    .normalize('NFKD')
    // فقط حروف، اعداد، خط‌تیره، آندرلاین، نقطه و slash را نگه می‌داریم
    .replace(/[^\w\-\/.]+/g, '-')
    // چند تا - پشت سر هم را یکی می‌کنیم
    .replace(/-+/g, '-')
    // چند تا / پشت سر هم را یکی می‌کنیم
    .replace(/\/+/g, '/')
    // حذف - های ابتدا و انتها
    .replace(/^-+|-+$/g, '')
    .toLowerCase()
    .slice(0, 80); // خیلی بلند هم نشه
}

function getOctokit(token) {
  const githubToken = token || process.env.GITHUB_TOKEN;
  if (!githubToken) {
    throw new Error('Missing GITHUB_TOKEN');
  }
  return new Octokit({ auth: githubToken });
}

async function createPullRequest({
  owner,
  repo,
  baseBranch,
  headBranch,
  title,
  body,
  token,
}) {
  const octokit = getOctokit(token);
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
