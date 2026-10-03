# ProjectManager continuity backup

Updated 2026-10-03 Asia/Tehran for PJ-ZJ-001. This is the current recovery entrypoint. The dated snapshot in `history/2026-10-03-PJ-ZJ-001.md` records the same work checkpoint. Verify Git and live state before resuming; no chat history is required.

**STOPPED at the owner's explicit dirty-ZJ stop condition.** At continuation, the real typed runner queue observed `tests/worker.test.ts` modified (24 insertions, 8 deletions) in ZJ, with HEAD still `4c9fa08bec566b9bba4d0f884ffbf8335ffc76de`. `ZJ_LOCAL_VALIDATION` rejected it with `REPO_DIRTY` before cloning or running validation commands. PJ made no ZJ edits. Preserve this external work; do not reset, restore, stash or clean it. The current PJ changes remain uncommitted and undeployed. Final local checks after the last code changes and live acceptance are unverified. See the PJ-ZJ-001 handoff and `history/2026-10-03-PJ-ZJ-001-BLOCKED.md`.

## Purpose and boundaries

ProjectManager (PJ) is the Telegram administrator control plane for operational status, incidents, typed jobs and a Windows Runner. The production path is Telegram admin → `projectmanager-control-plane` Cloudflare Worker → PJ D1 → authenticated project-bound Windows Runner. Worker routes and D1 persist operational metadata; the runner alone reads local repositories or executes approved validation commands. ZJ remains independent: `PJ_ENABLED=false` in ZJ is a valid mode, and PJ downtime must not affect ZJ startup, Telegram traffic or scheduler behavior.

Canonical repositories: PJ `C:\Users\Amir\Documents\GitHub\ProjectManager`; DailySystem `C:\Users\Amir\Documents\GitHub\DailySystem`; ZJ `C:\Users\Amir\Documents\GitHub\ZJ`; external ZJ plans `C:\Users\Amir\Documents\GitHub\Plans\ZJ`. The former `cloned\ProjectManager` checkout appears only in historical evidence.

## Cloudflare and authentication

PJ Worker name `projectmanager-control-plane`, account `9f12f5d584ab6b4bd94c66d4bf7f53dc`, public origin `https://projectmanager-control-plane.amirhoseinsalmani194728095.workers.dev`. PJ D1 binding `CONTROL_PLANE_DB`, database `projectmanager-control-plane`, ID `1871da4e-4078-48d7-ac02-62ecb0662004`. Production migrations are `migrations/d1/20260924_control_plane.sql`, `20260925_pj009_operational.sql`, `20260930_pj012_telegram.sql`; no PJ-ZJ-001 migration is required because the existing `cp_projects`, `cp_jobs`, `cp_runners` and event tables represent this milestone. Check remote migration state before any deploy. ZJ's `zj-db` is a different account/database and must never be a PJ migration target.

The Telegram webhook requires `TELEGRAM_WEBHOOK_SECRET` and `TELEGRAM_BOT_TOKEN`; authorized private-chat administrator IDs are configured by name in `wrangler.jsonc`. API admin, runner and project bearer identities are distinct. Runner tokens bind a runner ID to one project. Do not print or store token values in documentation, D1 project metadata or logs. ZJ needs a separate `zj` runner credential and a local runner process with `PG_PROJECT_ID=zj`; the existing DailySystem token is scoped to `daily-system` and cannot run ZJ jobs.

## Project registry and jobs

`src/controlPlane/projectRegistry.js` is the static allowlist. `daily-system` preserves the existing health, Git, test, typecheck, Codex and Excel job types. `zj` permits only `ZJ_REPO_STATUS`, `ZJ_RELEASE_EVIDENCE`, `ZJ_LOCAL_VALIDATION`, `ZJ_STAGING_HEALTHCHECK`, `ZJ_RELEASE_READINESS`. No production deploy, D1 mutation, webhook, Cron, AI or secret operation is registered for ZJ. The PJ-side `PJ_ZJ_ENABLED` flag is separate from ZJ's own `PJ_ENABLED`; the checked-in PJ Worker configuration keeps it false until final acceptance gates pass.

ZJ local paths belong to runner configuration (`ZJ_REPO_PATH`, `ZJ_PLANS_PATH`) and default to the canonical paths above. `ZJ_STAGING_HEALTH_URL` is optional and must identify an HTTPS `zj-staging.*.workers.dev` origin. Without it, the health job reports `PENDING / STAGING_NOT_CONFIGURED`. Telegram supplies no paths, commands or URLs. The runner executes fixed Git/npm argument arrays; Node invokes npm's CLI directly on Windows to retain `shell:false`. ZJ validation has a bounded overall deadline and output summaries. Readiness gates distinguish `PASS`, `FAIL`, `PENDING`, `UNKNOWN`, `NOT_APPLICABLE`.

## Deployment and evidence state

Last known live PJ state before PJ-ZJ-001: PJ-012 COMPLETE; Worker `/health` HTTP 200 with D1 available; Telegram `/start` and `/status` confirmed; DailySystem `HEALTHCHECK` end to end succeeded once through Worker/D1/Runner. Last reported active Worker version `2f5db2c8-3624-495c-800e-489c8a1aa099`; deployment `37271715-2e68-4915-82d5-574fc52d43bf`. These are historical live observations; verify current deployment before asserting they remain current. Excel Host Closure is CLOSED, with residual intermittent host risk and authoritative Excel sync disabled.

PJ-ZJ-001 started from clean local `main` and `origin/main` at `4aba1b11a62fd2f1bc308eb90a721fe31ef70ed7`. The implementation in progress is not yet committed or deployed at this checkpoint. The local ZJ branch is `feature/P0-011-weekly-report`, HEAD `4c9fa08bec566b9bba4d0f884ffbf8335ffc76de`, clean when inspected. Latest external ZJ plan `LATEST.md` says release NOT READY; staging Worker/D1 and GitHub release protections are incomplete. ZJ production Worker `zj`, production D1 `zj-db`, preferred staging `zj-staging` and `zj-staging-db` are protected identities. No ZJ production mutation belongs to this task.

## Active blockers, risks and next milestone

This checkpoint is **implementation in progress**, not a completed release. Required next work: finish tests and runner integration, run real ZJ repo/evidence/validation jobs without changing ZJ source, verify PJ Cloudflare account/D1 and migration status, provision a separately scoped ZJ runner credential through the existing owner-controlled secret process, deploy PJ only after gates pass, perform live PJ Telegram/runner checks, then commit and push PJ. If the owner-held runner token is unavailable, live ZJ end-to-end acceptance remains blocked; do not reuse or disclose the DailySystem token. Staging health is legitimately pending while no verifiable isolated endpoint exists. Production ZJ resources must remain untouched.

Known risks: ZJ validation scripts may create ignored build output; verify repository content before/after and use a disposable strategy if strict byte preservation is required. Runner lease duration must exceed the bounded validation deadline. The legacy PJ docs contain historical pre-Cloudflare language; prefer the current-state and PJ-012 closure sections. Do not infer a release pass from UNKNOWN or PENDING evidence.

## Safe recovery commands

In the canonical PJ directory: `git status -sb`, `git status -uall --porcelain`, `git rev-parse HEAD`, `git rev-parse origin/main`, `git diff --check`, `npm.cmd test`, `npm.cmd run check`, `npm.cmd run worker:build`. In ZJ, use read-only `git status -sb`, `git rev-parse HEAD`, and inspect `package.json` before validation. Consult `docs/WINDOWS-RUNNER.md`, `docs/handoffs/PJ-012/CLOUDFLARE-RUNBOOK.md`, and `docs/handoffs/PJ-ZJ-001-MULTI-PROJECT-CONTROL-PLANE.md` for operations. Never reset, clean, restore, stash, force push, rotate credentials, or target ZJ production as a recovery shortcut.

At this checkpoint: `PROJECT_CONTINUITY_BACKUP_UPDATED = YES`; `RECOVERY_FROM_ORIGIN_MAIN_POSSIBLE = NO` for the new PJ-ZJ-001 implementation until it is committed and pushed. The preceding PJ-012 baseline remains recoverable from origin/main.
