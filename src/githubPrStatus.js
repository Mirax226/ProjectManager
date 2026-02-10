function summarizeChecks(checkRuns = []) {
  const summary = { total: 0, success: 0, failure: 0, pending: 0 };
  for (const check of checkRuns) {
    summary.total += 1;
    const status = String(check.conclusion || check.status || '').toLowerCase();
    if (status === 'success') summary.success += 1;
    else if (['failure', 'timed_out', 'cancelled', 'action_required'].includes(status)) summary.failure += 1;
    else summary.pending += 1;
  }
  return summary;
}

function deriveOverall(summary, prState, mergeable) {
  if (prState !== 'open') return prState === 'merged' ? 'success' : 'warning';
  if (summary.failure > 0 || mergeable === false) return 'failure';
  if (summary.pending > 0 || mergeable == null) return 'warning';
  return 'success';
}

function parsePrStatus(input = {}) {
  const pr = input.pr || {};
  const checks = summarizeChecks(input.checkRuns || []);
  const mergeable = typeof input.mergeable === 'boolean' ? input.mergeable : null;
  return {
    state: pr.state || 'open',
    merged: Boolean(pr.merged_at),
    mergeable,
    checks,
    headSha: pr.head?.sha || null,
    requiredReviewsMet: input.requiredReviewsMet == null ? null : Boolean(input.requiredReviewsMet),
    updatedAt: pr.updated_at || new Date().toISOString(),
    overall: deriveOverall(checks, pr.state || 'open', mergeable),
  };
}

function createPrWatcher(options = {}) {
  const fetchStatus = options.fetchStatus;
  const onTransition = options.onTransition || (() => {});
  const now = options.now || (() => Date.now());
  const safeModeGetter = options.safeModeGetter || (() => false);
  const pollMs = Math.max(60_000, Number(options.pollMs) || 120_000);
  const ttlMs = Math.max(pollMs, Number(options.ttlMs) || 24 * 60 * 60 * 1000);
  const watchers = new Map();

  async function refresh(watchId) {
    const watch = watchers.get(watchId);
    if (!watch) return null;
    if (safeModeGetter()) return { paused: true };
    if (now() - watch.createdAt > ttlMs) {
      watchers.delete(watchId);
      return { expired: true };
    }
    const next = await fetchStatus(watch.repo, watch.number);
    const previous = watch.last;
    watch.last = next;
    watch.lastPolledAt = now();
    if (!previous) return next;
    const changed =
      previous.overall !== next.overall
      || previous.mergeable !== next.mergeable
      || previous.state !== next.state
      || previous.checks.failure !== next.checks.failure
      || previous.checks.pending !== next.checks.pending;
    if (changed) onTransition({ watchId, previous, next });
    if (next.state !== 'open') watchers.delete(watchId);
    return next;
  }

  function start(repo, number) {
    const watchId = `${repo}#${number}`;
    watchers.set(watchId, {
      watchId,
      repo,
      number,
      createdAt: now(),
      lastPolledAt: 0,
      nextPollAt: now(),
      last: null,
    });
    return watchId;
  }

  async function tick() {
    const ids = Array.from(watchers.keys());
    for (const id of ids) {
      const watch = watchers.get(id);
      if (!watch) continue;
      if (watch.nextPollAt > now()) continue;
      await refresh(id);
      if (watchers.has(id)) {
        watchers.get(id).nextPollAt = now() + pollMs;
      }
    }
  }

  return { start, refresh, tick, count: () => watchers.size };
}

module.exports = { summarizeChecks, parsePrStatus, createPrWatcher };
