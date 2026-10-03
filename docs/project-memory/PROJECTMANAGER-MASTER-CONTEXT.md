# ProjectManager Master Context

# Current master context — PJ-ZJ-001 (2026-10-03)

ProjectManager's canonical checkout is `C:\Users\Amir\Documents\GitHub\ProjectManager`. Its active owner-defined milestone, PJ-ZJ-001, extends the existing Worker/D1/runner/Telegram control plane to manage `daily-system` and `zj` with project-specific safe capabilities. ZJ remains optional and independent. The current durable recovery source is `PROJECT-CONTINUITY-BACKUP.md`; older activation and "next scope undefined" statements below are historical.

## Excel Host Closure CLOSED — 2026-10-01

EXCEL_HOST_CLOSURE = CLOSED; PJ-012 remains COMPLETE. Current classification: HISTORICAL_INTERMITTENT_NOT_REPRODUCED. This checkpoint supersedes all older OPEN/blocker/next-experiment statements preserved below; those statements are historical diagnostics, not active work instructions.

Closure evidence supplied by the owner in task PJ-EXCEL-HOST-CLOSURE-AND-NEXT-SCOPE: later bounded QA did not reproduce the intermittent failure; continuous window observation found no correlated dialog; independent AntiGravity production-path Excel runner soak passed 6/6, with 0 timeouts and 0 forced cleanup. Mirax and Gozareshkar source hashes were unchanged, process ownership was safe, every owned Excel process exited naturally, every disposable copy was deleted, and the soak made no repository changes. CODEX_FIX_REQUIRED = NO; EXCEL_HOST_CLOSURE_RECOMMENDED = YES. These are supplied independent results, not probes rerun by this documentation task.

Historical Open/Close/Quit failures, earlier COM/read failures, matrix evidence and diagnostic fixes remain preserved. Root cause remains unconfirmed; refresh/query, dialog, add-in and host-context causation are not established. Residual risk: the historical intermittent issue may recur; closure does not prove a root-cause fix or permanent host stability.

ACTIVE_EXCEL_HOST_BLOCKER = NONE. Authoritative Excel sync remains disabled. Final closure evidence and successor assessment: `docs/handoffs/PJ-EXCEL-HOST-CLOSURE/CLOSURE.md`.

NEXT_PJ_MILESTONE: PJ_NEXT_SCOPE_UNDEFINED. PJ_NEXT_SCOPE_DEFINED = NO. The pre-closure current plan and roadmap explicitly state: "Latest roadmap defines no formal successor after this milestone; do not invent PJ-013." The only latest ordered work was PJ-012 activation/health E2E, then separate Excel host closure; both are now closed. Older duplicate PJ-010 policy/review entries are historical and are not automatically promoted to a successor. Owner definition of the next scope is needed before implementation; no milestone number, objective, dependencies, acceptance criteria or implementation files are assigned here.

This task changes repository documentation only. DailySystem coordination required for this closure: NO; successor coordination: undefined until scope is defined. Production-impacting actions: NO. No Cloudflare, secrets, Telegram webhook, D1 production state, owner workbook or sync change. Existing gates remain: explicit owner review for integration/production enablement and ownership/restore verification before migration or archival mutation. External Plans/Review publication remains owner-only through the Finalizer.

## Historical checkpoints (superseded for current status)

## Excel Host Closure OPEN — 2026-10-01

EXCEL_HOST_CLOSURE = OPEN; PJ-012 remains COMPLETE. Starting clean 3ca7657c0bdc92b9eda84ca58929181830906635 matched origin/main. Two serial G/M/G/M disposable-copy matrices recorded: before, Gozareshkar quit timeout recovered after proven cleanup and Mirax close timeout; after, Mirax open timeout while three runs passed. MULTIPLE_HOST_FAILURE_MODES / WORKBOOK_OPEN_INSTABILITY / QUIT_CLEANUP_INSTABILITY; underlying cause UNKNOWN. Refresh/query causation is unconfirmed.

Corrected diagnostic semantics: successful open stays OPENABLE after a later lifecycle failure, while healthcheck still fails; persisted read/close/quit substages and safe timeout/probe/cleanup durations added. No feature, lifecycle budget or process ownership change. Eight owned instances cleaned, no probe orphans, pre-existing Excel PID 9396 preserved. Both authoritative hashes unchanged in all eight runs and final independent check; no source/VBA/global Office change.

Validation: 16 focused tests, full 215 passed with zero failures/skips, npm run check and diff checks PASS. Safe matrices and full evidence: docs/handoffs/PJ-EXCEL-HOST-CLOSURE/REVIEW.md. Finalizer DryRun only. No Cloudflare/Telegram/legacy DB/Render/secret mutation.

NEXT_PJ_MILESTONE: Excel Host Closure (still OPEN). Smallest next experiment: bounded copy-only Mirax open with read-only observation of the proven owned HWND's dialog state, plus one Gozareshkar control, before any Open argument or add-in change. Remaining blocker is intermittent open/close/quit hangs, not ownership/source integrity. Latest roadmap defines no formal successor after this milestone; do not invent PJ-013. Authoritative Excel sync remains disabled.

## PJ-012 COMPLETE — 2026-10-01

This final closure supersedes the pending checkpoints preserved below. CLOUDFLARE_RUNTIME_ACTIVE; TELEGRAM_WEBHOOK_ACTIVE; TELEGRAM_UI_SMOKE_PASS; EMOJI_MENU_ACTIVE; START_COMMAND_FOOTER_REMOVED; RUNNER_SCOPE_CORRECT; RUNNER_E2E_PASS; RENDER_LEGACY_UNUSED; OLD_POSTGRES_LEGACY_UNUSED.

Worker projectmanager-control-plane remains healthy: HTTP 200, runtime cloudflare, D1 available. Footer-removal code version 1930d9b0-e4f5-4f6e-bbd1-f7e15c99cfd9; active version after owner Runner-secret update 2f5db2c8-3624-495c-800e-489c8a1aa099 (deployment 37271715-2e68-4915-82d5-574fc52d43bf, 100%). Same existing Worker/account/D1; no second backend. No webhook secret or bot-token rotation.

Owner confirmed post-deployment /start PASS, /status PASS, removed duplicated footer and visible 📊 Status / 🖥️ Runners / 🚨 Incidents / 📋 Jobs. Webhook remains ACTIVE. Command/callback identifiers and authorization unchanged; menu uses BotFather commands instead of duplicated message help. Helper quoting defect was fixed in the prior checkpoint.

Owner performed exactly one intentional Runner rotation via existing helper and retained the matching token in the owner shell. RUNNER_TOKEN_PRESENT established by successful authenticated heartbeat/result, not value inspection. Scope windows-runner / daily-system; verified local repository C:\Users\Amir\Documents\GitHub\DailySystem. D1 confirmed fresh ONLINE heartbeat. No credential was sent to or printed by Codex.

Production E2E evidence: job 1bfad0eb-12c1-4c10-a996-7067ce693707 requested_by=telegram-admin, project=daily-system, type=HEALTHCHECK, terminal=SUCCEEDED, attempt_count=1, result_runner_id=windows-runner, diagnostic_status=available, lease_owner=null and lease_expires_at=null after result acceptance. The authenticated D1 result compare-and-swap validates the claimed owner, unexpired lease and current attempt before terminal acceptance; terminal lease clearing is expected. Total jobs=1 confirms a single smoke job. This proves Telegram -> Worker/D1 -> local Runner -> typed result. No Excel, shell, deploy, SQL, deletion or authoritative sync job ran. All CLI D1 inspections were read-only metadata selections.

Validation: six focused closure tests pass; full suite 213 passed, 0 failed/skipped (31.3718172 seconds); npm run check and diff checks passed. Finalizer DryRun only; no external Plans/Review writes. Normal footer checkpoint 0a8b6a71b97652a7bfade93bddc7571262ab750b pushed cleanly; final closure documentation commit/hash and final clean synchronization are recorded in final response (a commit cannot embed its own hash). No credential/env/log/database/workbook artifacts staged.

NEXT_PJ_MILESTONE: separate Excel host closure, currently unnumbered. NEXT_PJ_OBJECTIVE: narrow existing intermittent COM/open/read/quit failures and establish process ownership/cleanup plus preserved-source evidence; authoritative Excel sync remains disabled. WHY_THIS_IS_NEXT: the latest roadmap/current plan explicitly scheduled activation and safe health E2E first, then separate Excel host closure. No PJ-013 definition exists; older duplicate PJ-010 policy/review headings are preserved without renumbering. CURRENT_BLOCKERS for PJ-012: none. Existing Excel host uncertainty belongs to the next work item. That milestone was not started.


## Footer cleanup and final smoke checkpoint — 2026-10-01

Starting clean HEAD ef2a378a8cb75900e67405b74490d6acddd2b49f matched origin/main. Removed only the four-line /start/admin command-help footer; main Ops Center summary and compact emoji buttons retained. Commands and callback_data unchanged, including diagnostic commands registered in BotFather. Six focused tests pass; full suite 213 passed, 0 failed/skipped in 31.3718172 seconds. Syntax/diff checks passed.

Confirmed account and required secret NAMES, deployed footer cleanup to existing Worker. Menu code version 1930d9b0-e4f5-4f6e-bbd1-f7e15c99cfd9; post-deploy /health HTTP 200 runtime cloudflare, D1 available. Owner already confirmed active webhook, working /start and live emoji menu before this cleanup. Fresh /start/footer and /status observations are pending after cleanup. No webhook secret/bot-token change by Codex.

Owner reported RUNNER_TOKEN_ROTATION_REQUIRED, verified DailySystem path C:\Users\Amir\Documents\GitHub\DailySystem (directory existence also checked locally). Required identity windows-runner/daily-system. Owner-only existing -Runner procedure provided; same generated token must stay in owner shell, then npm run runner with DAILYSYSTEM_REPO_PATH set to the verified path. Exactly one Telegram /health daily-system requested. No authentication from Codex and no credential values requested. D1 initially had no jobs; heartbeat/job/lease/result proof awaits owner execution.

PJ-012 COMPLETE: NO while safe Runner E2E and fresh UI observations remain pending. CURRENT_BLOCKERS: retained owner-shell credentials/one live safe job and post-deployment /start /status observations.

NEXT_PJ_MILESTONE (after PJ-012 closure): separate Excel host closure, unnumbered in latest roadmap/current plan. NEXT_PJ_OBJECTIVE: narrow intermittent COM creation/open/read/quit behavior, verify process ownership/cleanup and source preservation while keeping authoritative sync disabled. WHY_THIS_IS_NEXT: latest roadmap explicitly orders authenticated activation and safe health E2E before separate Excel host closure. No PJ-013 identifier/scope is defined. Older duplicate PJ-010 entries retain independent review/persistence-policy work and should not be silently renumbered. Do not begin next work in this task.


## Latest closure checkpoint — 2026-10-01

Owner-confirmed WEBHOOK_ACTIVE: setWebhook SUCCESS/setAccepted true/HTTP 200/matching URL; getWebhookInfo matched, pending count 0, lastErrorPresent false, allowedUpdates message/callback_query. Owner confirms prior /start response. These are supplied production evidence, not credential inspection by Codex. No webhook/token rotation in this task.

Helper false-failure root cause: duplicated inline JavaScript passed via PowerShell/native node -e quoting, after successful activation. Removed only the redundant node/eval block; retained the tested npm run telegram:webhook -- --info path, safe failure exit and environment cleanup. Two actual PowerShell control-flow tests mock all external operations and prove success plus nonzero verification failure.

English active Worker menu now has 📊 Status, 🖥️ Runners, 🚨 Incidents, 📋 Jobs buttons plus 🏠 Start, ❤️ Health, 📁 Project Status and 📗 Excel Health command hints. Status/jobs headings consistent. Existing Ops Center already supplies admin/incident/runner emoji titles. Command identifiers and callback_data status/runners/incidents/jobs unchanged; allowlist/private-chat auth unchanged. Added three application tests for menus, message/callback equivalence and authorization.

Starting clean HEAD 2090fe1af80c804e9cf6d7db5198f1433fe54c12 == origin/main. Confirmed account 9f12f5d584ab6b4bd94c66d4bf7f53dc and four required secret names before deployment. New Worker version c7d32662-455e-41eb-9e7c-64d190d5911d deployed to same public origin; post-deploy /health HTTP 200 runtime cloudflare, D1 available. Read-only D1 metadata: five DONE Telegram updates and zero jobs at probe time; no raw update payload/data dump. Emoji code DEPLOYED; EMOJI_MENU_ACTIVE / fresh TELEGRAM_STATUS_PASS and post-deploy /start visibility await owner observation.

RUNNER_SCOPE_CORRECT per owner's confirmed prior rotation: windows-runner / daily-system. Codex process has no matching token and cannot inherit owner-shell environment. RUNNER_TOKEN_ROTATION_REQUIRED only if owner shell lost the matching value; do not rotate if retained. No automatic Runner rotation or auth attempt. RUNNER_E2E_PASS remains unverified; D1 job/result evidence not yet present. Owner should use retained shell to run one safe health job, then report its ID for independent D1 confirmation.

Verification: five new closure tests; 16 focused closure/webhook tests passed. Full suite 212 passed, 0 failed/skipped in 31.1011811 seconds; npm run check and git diff --check passed. Finalizer DryRun only, no external Plans/Review writes. Eight files changed: dispatcher, owner helper, closure tests, runbook, handoff, current plan and two memory files. No credentials/.env/log/database/workbook files staged.

PJ-012 COMPLETE: NO until post-deploy /start and /status, visible emoji menu and one safe local Runner E2E pass. No Excel/VBA write, Render/legacy DB investigation or shell/deploy/SQL job. Remaining owner actions documented in runbook; final commit/push hash and clean synchronized state reported in final response.


## Purpose

ProjectManager is a private operational control plane for multiple external projects, including future DailySystem integration. It coordinates authentication, operational events, alerts, runners, typed job validation, provider metadata, and operator views. It does not own the business logic or business data of external projects.

## Recovery Baseline

- Checkpoint: PJ-Checkpoint-003
- Coverage: PJ-001 through PJ-009 handoffs and current source contracts
- Date: 2026-09-25
- Git identity: unavailable in this tree because `.git` metadata is absent
- Production state: unchanged; no deployment, production mutation, integration enablement, secret change, commit, or push performed

## Current System Shape

The legacy Node runtime owns the existing Telegram bot, PostgreSQL/config database integrations, GitHub and deployment workflows, logs, Safe Mode, Ops Timeline, and web dashboard. The Fetch-compatible Control Plane under `src/controlPlane` owns project-scoped operational events, alerts, runner state, jobs, leases, status reads, and PJ-009 durable operational state in additive D1 tables. The Windows Runner is the only approved local execution boundary for typed Excel health, snapshot, backup, and reconciliation operations; authoritative Excel publication remains disabled.

## Source Of Truth

The PJ handoffs document decisions and audit evidence. Source contracts and tests are authoritative for behavior. This memory set is a recovery index, not a replacement for implementation tests or the handoff history in `docs/handoffs/`.

## Immediate Operating Rule

Continue with fixture-based, non-production hardening. Keep typed jobs validation-only until execution policy, adapters, approval, timeout, artifact, and rollback contracts are separately reviewed.

## Project Memory Location

- Canonical memory path: `docs/project-memory/` in the canonical Git repository (`C:\Users\Amir\Documents\GitHub\ProjectManager`).
- Purpose: recovery index for current architecture, verified state, decisions, milestones, and safe operating boundaries.
- Update protocol: update `CURRENT-STATE.md`, `RECOVERY-CHECKPOINT.md`, or `ROADMAP.md` when verified state changes; add immutable phase evidence under `docs/handoffs/`; keep source contracts and tests authoritative for behavior.
- Plans/PJ relationship: `C:\Users\Amir\Documents\GitHub\Plans\PJ\` is the owner-facing current-plan and transition log mirror. It summarizes the canonical repository state and does not replace repository memory or tests.
- Repository handoff relationship: `docs/handoffs/` contains historical phase/checkpoint evidence. Handoffs are preserved rather than rewritten; the project-memory files point to the current recovery position.

## Permanent Finalizer Workflow

- Codex writes generated plans, changelogs, evidence, and handoffs only inside the canonical repository.
- The owner explicitly executes `tools/finalize-task.ps1`; Codex does not publish directly to Desktop Review or external Plans.
- The Finalizer publishes external Plans and Review artifacts from repository-local plan sources after validation. External Plans are generated mirrors, not a second source of truth.
- Desktop Review is append-only for normal finalization; previous Review artifacts are never deleted and timestamp collisions are handled safely.
- Finalizer staging is created under the user's normal temporary directory and is deleted only after successful final artifact validation. Failed runs retain staging for diagnosis.

## PJ-011B Real Excel Probe

- The real Windows Excel host successfully opened disposable copies of Mirax.xlsm and Gozareshkar.xlsm through COM creation, configuration, `Workbooks.Open`, read probe, close, and quit.
- Source hashes were unchanged before/after both probes, and probe-owned Excel processes exited. A pre-existing Excel process was left untouched.
- The historical `EXCEL_OPEN_FAILED` did not reproduce; classify its root cause as unknown until a future recurrence supplies a first failing stage and safe HRESULT. Authoritative sync remains disabled.

## PJ-011B desktop resume — 2026-09-30

Canonical baseline 6d4169e; five interrupted dirty files preserved. Repository hardening has typed Config DB preflight/classification, repeat-safe DSN repair, bounded transient retries, active incident warning dedupe, and one recovery contract. The reported Supabase tenant/user error is a provider routing/authentication class, not hostname evidence. README/Git history document Render as intended bot hosting, but ACTIVE_NODE_HOST_UNKNOWN remains. Worker/D1 is a separate Control Plane and does not execute legacy Telegram/Config DB warmup.

Today's host evidence supersedes any assumption that the older successful probe closed the incident. Mirax repeatedly timed out at WORKBOOK_OPEN_START; refresh suppression helped once but did not consistently resolve it, and a later COM_CREATE timeout prevents a confirmed underlying root cause. Gozareshkar opens/reads/closes; verified process cleanup recovers a quit timeout with an explicit warning. Original XLSM/VBA hashes remain unchanged. New automation PID 31620 has unverified ownership and remains untouched; PID 2868 was preserved. Overall no-orphan verification is incomplete.

CODEX_TASK project/mode/sandbox/environment/output safety is tightened. Local Control Plane/claim/lease/real Excel/typed result was exercised; cloud runtime verification and authoritative sync remain disabled/pending. Detailed review, Config DB diagnosis, Excel diagnosis and safe JSON evidence live under docs/handoffs/PJ-011B. Next milestone: PJ-012 runtime provenance plus interactive-host COM/Mirax closure, without production changes until separately approved.

Final verification: Gozareshkar also timed out at WORKBOOK_READ_PROBE after successful open; its owned process was cleaned up and source unchanged. Host instability remains unresolved. Release suite: 180/180 passed; syntax/check and diff checks passed.

## PJ-012 Cloudflare production direction — 2026-09-30

PJ-011B finalized/pushed at 7a2a6f010d3f4756aaf76816c0a1faa303839e99 with clean synchronized main. Owner superseded legacy runtime provenance discovery: Render and old external Config DB are DEPRECATED / UNUSED; NO MIGRATION REQUIRED; no deletion. Cloudflare is the designated production runtime, D1 the operational persistence, Windows Runner the local execution plane. Implementation reuses the existing Control Plane, Ops Center models and typed jobs. Node polling/legacy DB warmup are explicit compatibility opt-ins; Worker has no legacy DSN dependency.

PJ-012 local Worker/D1/Runner validation is recorded under docs/handoffs/PJ-012. CLOUDFLARE_DEPLOYMENT_PENDING_SECRETS: account confirmed by owner; D1 created and all three remote migrations applied. Worker deployment, webhook activation and live smoke checks await interactive secret provisioning. No production webhook activation or live smoke success is claimed. Safe activation runbook is repository-local. Excel intermittent-host evidence remains unresolved and observable; source workbooks/VBA untouched, sync disabled. Next: authenticated activation and safe production health E2E, then separate Excel host closure. Historical memory remains preserved above.

PJ-012 owner follow-up: bootstrap Telegram administrator 843686302 configured; no secret value supplied in chat.

## Activation resume — 2026-10-01 (authoritative current checkpoint)

DEPLOYED; HEALTH_VERIFIED; READY_FOR_WEBHOOK_ACTIVATION. Actual repository Worker deployed from clean 8d15a46e46e3f4b1b10706c36ea2fee117a163b3, preserving all previous PJ-012 changes. Confirmed account 9f12f5d584ab6b4bd94c66d4bf7f53dc; all four required secret names present before deployment. D1 CONTROL_PLANE_DB -> projectmanager-control-plane / 1871da4e-4078-48d7-ac02-62ecb0662004 exists; remote migration listing has no pending migrations. No data dump or secret value read.

Public origin: https://projectmanager-control-plane.amirhoseinsalmani194728095.workers.dev. Version 6dbe1049-ace7-4b84-87e2-9777e41cb715; deployment 0460e9ac-cc1f-40f7-8416-cc03036d74b9 at 100 percent. Both /health and /healthz returned HTTP 200, ok=true, runtime=cloudflare, service=projectmanager, d1=available, version=PJ-012. Safe health service label differs from deployment name; no code change needed. Worker contains no legacy Node bot/Config DB bootstrap, no Postgres DSN requirement and no Render dependency. External legacy resources untouched.

Release: 196 passed, 0 failed, 0 skipped, 31.8742396 seconds; npm run check and git diff --check passed. Initial sandbox run failed because Wrangler/workerd were restricted; authorized host-context rerun passed. Wrangler identity verification succeeded with existing OAuth; no login or scope refresh requested. No browser or Excel probe.

RUNNER_SECRET_SCOPE_MISMATCH: canonical client defaults src/runner/client.js are PG_RUNNER_ID=windows-runner and PG_PROJECT_ID=daily-system. Test fixtures explicitly override windows-01; they do not establish deployment configuration. No current PG_RUNNER_ID/PG_PROJECT_ID override or PG_RUNNER_TOKEN exists. Owner-provisioned secret is stated to bind windows-01/daily-system; values were not inspected. No Runner E2E attempted. Either explicitly configure windows-01 locally to match the existing owner-selected secret, or owner aligns secret to windows-runner; do not change or rotate automatically. Token continuity remains unavailable; secure owner-coordinated reprovisioning is needed before E2E unless the existing token is retained outside this process. Rotation is not needed for Worker/webhook readiness.

Telegram token and webhook secret names are deployed; corresponding process environment names absent. WEBHOOK_ACTIVE is NOT claimed: no setWebhook/getWebhookInfo or real chat smoke performed. Owner-side steps below use the SAME existing Cloudflare webhook secret; no extraction/rotation. PJ-012 COMPLETE: NO; Worker deployment portion complete, webhook/live Telegram and optional Runner smoke outstanding.

## Final activation checkpoint — 2026-10-01

Started clean at a60a53f7b831b17d7f305dbf29523fd814f5e567 == origin/main. Wrangler confirmed account 9f12f5d584ab6b4bd94c66d4bf7f53dc and all four required secret names. Production /health returned HTTP 200, runtime cloudflare, D1 available. No deployment or rotation performed.

This process has no Telegram or Runner credential environment names. WEBHOOK_SECRET_ROTATION_REQUIRED if the owner has lost the matching secret; existing masked-input activation remains preferred if retained. OWNER-FINAL-ACTIVATION.ps1 supplies owner-only alternatives without printing/persisting generated values. Runner before/after unchanged: existing owner-stated windows-01/daily-system remains wrong; required windows-runner/daily-system. No authentication before rotation confirmation.

Webhook activation/getWebhookInfo and Telegram /start /status remain unverified; no live update/job D1 evidence. No Runner E2E. PJ-012 COMPLETE: NO. Pending owner-controlled activation/rotation and one safe-job smoke. Another owner shell's environment is not inherited by Codex; keep it open for execution. No workbook/VBA, Render or old DB activity.

Validation: 196 tests passed, 0 failed/skipped, 31.2321189 seconds; npm run check, owner-script parser check, diff check and Finalizer DryRun passed. No external Plans/Review writes. Updated runbook, owner procedure, handoff, current plan and two memory files only. Commit/push checkpoint hash recorded in final response; no secret/env/raw log staged.