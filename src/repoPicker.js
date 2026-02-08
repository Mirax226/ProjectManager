function paginateRepos(repos, page = 0, pageSize = 8) {
  const safePageSize = Math.max(1, Number(pageSize) || 8);
  const safePage = Math.max(0, Number(page) || 0);
  const totalPages = Math.max(1, Math.ceil((repos?.length || 0) / safePageSize));
  const boundedPage = Math.min(safePage, totalPages - 1);
  const start = boundedPage * safePageSize;
  return {
    page: boundedPage,
    pageSize: safePageSize,
    totalPages,
    items: (repos || []).slice(start, start + safePageSize),
    hasPrev: boundedPage > 0,
    hasNext: boundedPage < totalPages - 1,
  };
}

function mapRepoButton(repo) {
  const owner = repo?.owner?.login || repo?.owner || '';
  const name = repo?.name || '';
  return `${owner}/${name}`;
}

async function listAllGithubRepos(fetchPage) {
  const all = [];
  let page = 1;
  while (true) {
    const batch = await fetchPage(page);
    if (!Array.isArray(batch) || !batch.length) break;
    all.push(...batch);
    if (batch.length < 100) break;
    page += 1;
  }
  return all;
}

module.exports = {
  paginateRepos,
  mapRepoButton,
  listAllGithubRepos,
};
