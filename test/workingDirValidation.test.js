const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs/promises');
const path = require('node:path');

const { validateWorkingDir } = require('../bot');
const { getDefaultWorkingDir } = require('../gitUtils');

test('working dir root dot is valid for non-node projects', async () => {
  const repoSlug = 'test-owner/test-repo';
  const checkoutDir = getDefaultWorkingDir(repoSlug);
  await fs.rm(checkoutDir, { recursive: true, force: true });
  await fs.mkdir(checkoutDir, { recursive: true });

  const result = await validateWorkingDir({
    repoSlug,
    workingDir: '.',
    projectType: 'python',
  });

  assert.equal(result.ok, true);
});

test('working dir subdir is valid', async () => {
  const repoSlug = 'test-owner/test-repo';
  const checkoutDir = getDefaultWorkingDir(repoSlug);
  const subdir = path.join(checkoutDir, 'subdir');
  await fs.rm(checkoutDir, { recursive: true, force: true });
  await fs.mkdir(subdir, { recursive: true });

  const result = await validateWorkingDir({
    repoSlug,
    workingDir: 'subdir',
    projectType: 'python',
  });

  assert.equal(result.ok, true);
});

test('working dir traversal is invalid', async () => {
  const repoSlug = 'test-owner/test-repo';
  const checkoutDir = getDefaultWorkingDir(repoSlug);
  await fs.rm(checkoutDir, { recursive: true, force: true });
  await fs.mkdir(checkoutDir, { recursive: true });

  const result = await validateWorkingDir({
    repoSlug,
    workingDir: '../',
    projectType: 'python',
  });

  assert.equal(result.ok, false);
  assert.equal(result.code, 'OUTSIDE_REPO');
});
