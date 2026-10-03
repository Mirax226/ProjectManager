# PJ-ZJ-002 coordinated Runner activation

Date: 2026-10-03 Asia/Tehran. This checkpoint is authoritative over older PJ-ZJ-001 disabled/pending notes.

## Baseline and credentials

PJ started clean at e18c878f15af20783697d00b18165562f1191b71. ZJ started clean and synced at f7d8bd43f9a1431a04d4e57bd3566dd18086a331 on feature/P0-011-weekly-report. DailySystem main/origin were 077a2ebd63de008a055c9e3dd0e134b9d398db80 with documented active Mirax work; it was not changed by this task.

The confirmed PG_RUNNER_TOKENS_JSON schema is a map keyed by Runner ID, each value containing a unique token and explicit project. Two independent 32-byte tokens were generated in memory and the complete map was uploaded in one ProjectManager Cloudflare secret update: windows-runner/daily-system and zj-runner/zj. Values were never printed, logged, or committed. Both local launch contexts use process-scoped environments; no established secure local persistence mechanism was found. RUNNER_DURABLE_SECRET_STORAGE=PROCESS_ONLY. An initial process-only session ended and its tokens were intentionally replaced together; the latest map is the second successful coordinated rotation. Do not use the historical owner helper, which uploads only a single-runner map.

## Verified live path

Pre-rotation focused auth matrix 23/23 PASS; PJ full suite 233/233 PASS. The small pre-activation heartbeat fix lets a scoped zj-runner prove identity while ZJ jobs stay disabled. Its focused Worker/ZJ matrix 34/34 and full suite 233/233 passed; check and dry-run build passed. Commit 53262f4 was pushed and deployed in disabled mode as d8383e0c-f6ce-4afa-958d-5adb4678b113.

DailySystem first: windows-runner heartbeat ONLINE; DailySystem→ZJ request HTTP 403 project_scope_denied; HEALTHCHECK job 505d5207-1e79-454e-b1b1-446e93e6a4c7 SUCCEEDED with repository available. Then zj-runner heartbeat ONLINE; ZJ→DailySystem request HTTP 403 project_scope_denied. While disabled, ZJ job creation returned HTTP 403 project_disabled. This proved scope and activation ordering.

PJ_ZJ_ENABLED was switched to true in wrangler.jsonc, tested 233/233, check/build/diff passed, committed as 6c6ba2c and pushed. PJ Worker version 45f537fb-f9dc-4d47-bb59-0b2fd7296d63 was deployed with the flag true. Live /health returned HTTP 200 and D1 available; PJ D1 contains daily-system and zj project records.

The initial live ZJ_REPO_STATUS job 69ae6df9-b433-4569-a1ca-b23e15ff8f9b SUCCEEDED and matched clean f7d8bd43. An initial ZJ_RELEASE_EVIDENCE job a228c40c-0840-4ef8-b324-7b43788f5b87 FAILED after a short Runner API timeout and retry. The local Runner client timeout was extended and Plans parsing updated for current ZJ release evidence; focused tests 18/18 and final PJ suite 234/234 passed, with check/build/diff passing. Commit d49c219 was pushed. Because process-only credentials from that session were lost, both Runner credentials were replaced together again; the latest values existed only in task-owned processes during validation.

On the current session, ZJ_REPO_STATUS job 9fb22084-4418-44f4-8987-e0fff142388d1 SUCCEEDED on attempt 1, reporting CLEAN_SYNCED, the expected branch, and f7d8bd43. ZJ_RELEASE_EVIDENCE job f39050b3-8756-4eb1-8ddf-083d17370953 SUCCEEDED on attempt 1. It extracted the current Plans checkpoint ZJ-RC-006, 61 files/1,160 tests, verified staging Worker/D1, successful CI, missing staging Telegram bot, and blocked staging application acceptance. This is newer Plans evidence than the task's historical STAB-012 expectation.

ZJ_LOCAL_VALIDATION job 448fe0fe-992b-46cf-b650-63b8efb669ba SUCCEEDED on attempt 1 through PJ Worker→PJ D1→zj-runner→a disposable ZJ clone. Result PASS: 61 files, 1,160 tests, 1,160 passed, 0 failed; lint, typecheck, build, credential scan, dependency audit, security, and verify all PASS. Stored execution duration 394297 ms; result was bounded. Canonical ZJ HEAD/upstream stayed f7d8bd43 and the worktree remained clean after validation.

After ZJ activation, DailySystem HEALTHCHECK job 3965ccd2-d0e0-43be-8680-9030383357e8d SUCCEEDED, with the repository available. Both project scopes remained isolated.

## Remaining acceptance and operations

Live ProjectManager Telegram admin navigation and a Telegram-triggered ZJ job still need direct chat observation. Local Telegram tests passed, but they do not prove the live chat UI. The ProjectManager bot must be used, never the ZJ production bot. At 2026-10-03 12:40 UTC, both task-owned process-only Runners were intentionally stopped after validation; their PIDs 40660 and 33912 were confirmed absent. The uploaded Worker token map remains configured, but there is no retained local token copy or verified durable restart configuration. To restore live Runner service, rotate the complete two-runner map again through an approved secure process and start both scoped Runners with matching credentials. Do not rely on the historical single-runner helper. No ZJ production Worker, D1, webhook, Cron, AI credential, production Telegram, or DailySystem authoritative Excel resource was modified by this task.

Current verdict: credential rotation, live Runner scope, DailySystem canary, ZJ repository status, ZJ release evidence, ZJ validation, and Worker/D1/Runner/ZJ path PASS at validation time. RUNNERS_CURRENTLY_ONLINE=NO. PJ_TELEGRAM_ZJ_ACCEPTANCE remains PENDING, so PJ_ZJ_MULTI_PROJECT_CONTROL_PLANE_COMPLETE and READY_FOR_ANTIGRAVITY_PJ_ZJ_ACCEPTANCE remain NO until the live chat and operational Runner persistence gap are resolved.
