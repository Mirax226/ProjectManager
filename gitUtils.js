const fs = require('fs/promises');
const path = require('path');
const simpleGit = require('simple-git');

const WORKDIR = process.env.WORKDIR || '/tmp/patch-runner-bot';
const DEFAULT_BASE_BRANCH = process.env.DEFAULT_BASE_BRANCH || 'main';

function getDefaultWorkingDir(repoSlug) {
  if (!repoSlug || !repoSlug.includes('/')) return undefined;
  const [owner, repo] = repoSlug.split('/');
  if (!owner || !repo) return undefined;
  return path.join(WORKDIR, `${owner}__${repo}`);
}

function getRepoInfo(project) {
  let repoSlug = project?.repoSlug;
  if (!repoSlug && project?.repoOwner && project?.repoName) {
    repoSlug = `${project.repoOwner}/${project.repoName}`;
  }
  if (!repoSlug && project?.owner && project?.repo) {
    repoSlug = `${project.owner}/${project.repo}`;
  }
  if (!repoSlug && typeof project?.repo === 'string' && project.repo.includes('/')) {
    repoSlug = project.repo;
  }

  if (!repoSlug) {
    throw new Error('Project is missing repoSlug');
  }

  let repoUrl = project?.repoUrl || `https://github.com/${repoSlug}`;
  if (!repoUrl.endsWith('.git')) {
    repoUrl = `${repoUrl}.git`;
  }

  let workingDir = project?.workingDir;
  if (!workingDir) {
    workingDir = getDefaultWorkingDir(repoSlug);
  }

  return { repoSlug, repoUrl, workingDir };
}

async function ensureWorkdir() {
  await fs.mkdir(WORKDIR, { recursive: true });
}

function getGithubToken(project) {
  const key = project?.githubTokenEnvKey || 'GITHUB_TOKEN';
  return process.env[key] || process.env.GITHUB_TOKEN;
}

function getRepoPath(project) {
  try {
    return getRepoInfo(project).workingDir;
  } catch (error) {
    return undefined;
  }
}

function applyGitTokenToUrl(repoUrl, token) {
  if (!token) return repoUrl;
  const url = new URL(repoUrl);
  url.username = token;
  return url.toString();
}

async function prepareRepository(project, effectiveBaseBranch) {
  await ensureWorkdir();
  const { repoUrl, workingDir } = getRepoInfo(project);
  const repoDir = workingDir;
  const baseBranch = effectiveBaseBranch || project.baseBranch || DEFAULT_BASE_BRANCH;

  let repoExists = true;
  try {
    await fs.access(repoDir);
  } catch (error) {
    repoExists = false;
  }

  if (!repoExists) {
    const gitInWorkdir = simpleGit(WORKDIR);
    const token = getGithubToken(project);
    const cloneUrl = applyGitTokenToUrl(repoUrl, token);
    await gitInWorkdir.clone(cloneUrl, repoDir);
  }

  const git = simpleGit(repoDir);
  await git.fetch();
  await git.checkout(baseBranch);
  await git.reset(['--hard', `origin/${baseBranch}`]);

  return { git, repoDir, baseBranch };
}

async function createWorkingBranch(git, baseBranch, branchName) {
  await git.checkout(baseBranch);
  await git.checkoutBranch(branchName, baseBranch);
}

async function applyPatchToRepo(git, repoDir, patchText) {
  const patchPath = path.join(repoDir, 'tmp.patch');
  await fs.writeFile(patchPath, patchText, 'utf-8');
  try {
    await git.raw(['apply', '--whitespace=nowarn', patchPath]);
  } catch (error) {
    throw new Error(`git apply failed: ${error.message}`);
  } finally {
    try {
      await fs.unlink(patchPath);
    } catch (err) {
      // ignore cleanup errors
    }
  }
}

async function commitAndPush(git, branchName) {
  const status = await git.status();
  if (status.files.length === 0) {
    return false;
  }

  await git.add('.');
  await git.commit('Apply patch via bot');
  await git.push('origin', branchName);
  return true;
}

async function fetchDryRun(project, effectiveBaseBranch) {
  const { git } = await prepareRepository(project, effectiveBaseBranch);
  await git.fetch(['--dry-run']);
}

module.exports = {
  WORKDIR,
  DEFAULT_BASE_BRANCH,
  prepareRepository,
  createWorkingBranch,
  applyPatchToRepo,
  commitAndPush,
  fetchDryRun,
  getRepoPath,
  getRepoInfo,
  getGithubToken,
  getDefaultWorkingDir,
};
