const fs = require('fs/promises');
const path = require('path');
const simpleGit = require('simple-git');

const WORKDIR = process.env.WORKDIR || '/tmp/patch-runner-bot';
const DEFAULT_BASE_BRANCH = process.env.DEFAULT_BASE_BRANCH || 'main';

function getRepoPath(project) {
  return path.join(WORKDIR, `${project.owner}__${project.repo}`);
}

async function ensureWorkdir() {
  await fs.mkdir(WORKDIR, { recursive: true });
}

async function prepareRepository(project, effectiveBaseBranch) {
  await ensureWorkdir();
  const repoDir = getRepoPath(project);
  const baseBranch = effectiveBaseBranch || project.baseBranch || DEFAULT_BASE_BRANCH;

  let repoExists = true;
  try {
    await fs.access(repoDir);
  } catch (error) {
    repoExists = false;
  }

  if (!repoExists) {
    const gitInWorkdir = simpleGit(WORKDIR);
    await gitInWorkdir.clone(`https://github.com/${project.owner}/${project.repo}.git`, repoDir);
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
};
