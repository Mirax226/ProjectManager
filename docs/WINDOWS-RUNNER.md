# Windows runner

Set `PG_CONTROL_PLANE_URL`, `PG_RUNNER_ID`, `PG_RUNNER_TOKEN`, `PG_PROJECT_ID=daily-system`, and `DAILYSYSTEM_REPO_PATH` in a local `.env` or service environment. Optional settings are `CODEX_BIN`, `CODEX_PROFILE`, `PG_ALLOW_CODEX_EDITS`, heartbeat/poll intervals, and `PG_RUNNER_HOST_LABEL`.

Start it with `npm.cmd run runner`. The runner sends authenticated heartbeats, claims one leased typed job at a time, executes only the allowlisted job types, and reports a truncated result. A temporary control-plane outage does not expose an inbound port; the process retries on its normal polling/heartbeat loop.

Initial job types are HEALTHCHECK, PROJECT_STATUS, GIT_STATUS, RUN_TESTS, RUN_TYPECHECK, DAILYSYSTEM_HEALTHCHECK, EXCEL_CONNECTIVITY_TEST, EXCEL_SYNC_DIAGNOSTIC, EXCEL_RECONCILIATION_CHECK, and CODEX_TASK. Push, deploy, arbitrary shell, secret mutation, and authoritative Excel writes are intentionally unavailable.

## ZJ runner profile (PJ-ZJ-001)

Run a **separate runner identity and credential scoped to `zj`**. Set `PG_PROJECT_ID=zj`, a unique `PG_RUNNER_ID`, the matching `PG_RUNNER_TOKEN`, and the existing `PG_CONTROL_PLANE_URL`. Runner-local paths are `ZJ_REPO_PATH=C:\Users\Amir\Documents\GitHub\ZJ` and `ZJ_PLANS_PATH=C:\Users\Amir\Documents\GitHub\Plans\ZJ`; these canonical paths are defaults if unset. `ZJ_STAGING_HEALTH_URL` is optional and must be an approved HTTPS `zj-staging.*.workers.dev` origin. Start with `npm.cmd run runner`. The existing DailySystem runner/credential cannot claim ZJ work.

Allowed ZJ jobs: `ZJ_REPO_STATUS`, `ZJ_RELEASE_EVIDENCE`, `ZJ_LOCAL_VALIDATION`, `ZJ_STAGING_HEALTHCHECK`. `ZJ_RELEASE_READINESS` is calculated by the Worker from prior evidence. All ZJ job payloads must be empty. Local validation clones the clean ZJ commit into a disposable temporary directory, installs dependencies there and runs the current repository scripts; it leaves the canonical ZJ checkout unchanged. No ZJ production deploy, migration, webhook, Cron, AI, secret or Telegram action is supported.
