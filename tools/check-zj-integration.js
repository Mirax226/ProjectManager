// Local acceptance through the same typed queue and Windows Runner executor.
// Durable output contains bounded operational summaries, never raw command logs.
const fs = require('fs');
const path = require('path');
const os = require('os');
const { ControlPlaneStore } = require('../src/controlPlane/store');
const { runOnce } = require('../src/runner');
const { executeJob } = require('../src/runner/jobExecutor');
const { ZJ_REPO, ZJ_PLANS } = require('../src/runner/zjOperations');

async function main() {
  const profile = { id: 'zj', projectId: 'zj', repoPath: process.env.ZJ_REPO_PATH || ZJ_REPO, plansPath: process.env.ZJ_PLANS_PATH || ZJ_PLANS };
  const outputPath = path.join(os.tmpdir(), 'PJ-ZJ-001-local-evidence.json');
  const store = new ControlPlaneStore(); const runnerId = 'zj-local-acceptance';
  const client = { projectId: 'zj', claim: async () => ({ job: store.claimJob(runnerId, 'zj') }), result: async (id, result) => {
    const accepted = store.resultJob(id, runnerId, result);
    if (!accepted.ok) throw new Error(`result rejected: ${accepted.error}`);
    return accepted;
  } };
  const before = await executeJob({ projectId: 'zj', type: 'ZJ_REPO_STATUS', payload: {} }, { profile });
  const evidence = { recordedAt: new Date().toISOString(), sourceBefore: before.result, jobs: [] };
  for (const type of ['ZJ_REPO_STATUS', 'ZJ_RELEASE_EVIDENCE', 'ZJ_LOCAL_VALIDATION', 'ZJ_STAGING_HEALTHCHECK']) {
    const created = store.createJob({ projectId: 'zj', type, payload: {}, idempotencyKey: `local:${type}` }, 'local-acceptance');
    await runOnce(client, { profile });
    const completed = store.getJob(created.job.id);
    evidence.jobs.push(completed);
    fs.writeFileSync(outputPath, JSON.stringify(evidence, null, 2));
    console.log(JSON.stringify({ type, status: completed.status, diagnosticCode: completed.result?.diagnosticCode || null }));
  }
  evidence.readiness = store.createJob({ projectId: 'zj', type: 'ZJ_RELEASE_READINESS', payload: {} }, 'local-acceptance').job.result;
  evidence.sourceAfter = (await executeJob({ projectId: 'zj', type: 'ZJ_REPO_STATUS', payload: {} }, { profile })).result;
  evidence.sourceUnchanged = evidence.sourceBefore.head === evidence.sourceAfter.head && evidence.sourceAfter.state === 'CLEAN_SYNCED';
  fs.writeFileSync(outputPath, JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify({ outputPath, sourceUnchanged: evidence.sourceUnchanged, validation: evidence.jobs.find((job) => job.type === 'ZJ_LOCAL_VALIDATION')?.result?.testEvidence, readiness: evidence.readiness.gates }));
  if (!evidence.sourceUnchanged) process.exitCode = 1;
}
if (require.main === module) main().catch(() => { console.error('Local ZJ acceptance failed; no credential or raw error emitted.'); process.exitCode = 1; });
module.exports = { main };
