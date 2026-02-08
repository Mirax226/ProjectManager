const assert = require('node:assert/strict');
const test = require('node:test');

const { paginateRepos, listAllGithubRepos } = require('../src/repoPicker');

test('paginateRepos returns expected page slices', () => {
  const repos = Array.from({ length: 19 }, (_, i) => ({ full_name: `o/r${i}` }));
  const page1 = paginateRepos(repos, 1, 8);
  assert.equal(page1.items.length, 8);
  assert.equal(page1.hasPrev, true);
  assert.equal(page1.hasNext, true);
});

test('listAllGithubRepos paginates until short page', async () => {
  const calls = [];
  const repos = await listAllGithubRepos(async (page) => {
    calls.push(page);
    if (page === 1) return Array.from({ length: 100 }, (_, i) => ({ id: i }));
    if (page === 2) return [{ id: 101 }];
    return [];
  });
  assert.equal(repos.length, 101);
  assert.deepEqual(calls, [1, 2]);
});
