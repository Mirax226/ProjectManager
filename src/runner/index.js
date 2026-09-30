const { RunnerClient } = require('./client');
const { executeJob, DEFAULT_PROFILE } = require('./jobExecutor');

async function runOnce(client, options = {}) {
  const claim = await client.claim(); if (!claim.job) return null;
  const job = claim.job;
  try { const result = await executeJob(job, options); await client.result(job.id, { ...result, attemptCount: job.attemptCount, leaseExpiresAt: job.leaseExpiresAt }); return result; }
  catch (error) { const result = { ok: false, error: String(error.message || error).slice(0, 1000) }; await client.result(job.id, { ...result, attemptCount: job.attemptCount, leaseExpiresAt: job.leaseExpiresAt }); return result; }
}

async function startRunner(options = {}) {
  const client = options.client || new RunnerClient(options);
  const profile = { ...DEFAULT_PROFILE, ...(options.profile || {}) };
  const heartbeatMs = Number(options.heartbeatMs || process.env.PG_RUNNER_HEARTBEAT_MS || 30000);
  const pollMs = Number(options.pollMs || process.env.PG_RUNNER_POLL_MS || 5000);
  let stopped = false; let heartbeatTimer; let pollTimer;
  const heartbeat = async () => { try { await client.heartbeat(); } catch (error) { console.error('[runner] heartbeat failed', error.message); } };
  await heartbeat();
  heartbeatTimer = setInterval(heartbeat, heartbeatMs);
  const poll = async () => { if (stopped) return; try { await runOnce(client, { profile }); } catch (error) { console.error('[runner] poll failed', error.message); } finally { if (!stopped) pollTimer = setTimeout(poll, pollMs); } };
  poll();
  return { stop: () => { stopped = true; clearInterval(heartbeatTimer); clearTimeout(pollTimer); } };
}

if (require.main === module) startRunner().then(() => console.log('PG Windows runner started')).catch((error) => { console.error(error.message); process.exitCode = 1; });
module.exports = { startRunner, runOnce };
