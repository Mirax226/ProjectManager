# ProjectManager Current Plan

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


- Canonical repository: C:\Users\Amir\Documents\GitHub\cloned\ProjectManager.
- PJ-011B finalized at 7a2a6f010d3f4756aaf76816c0a1faa303839e99; normal push succeeded, working tree clean, HEAD == origin/main verified.
- PJ-012 owner decision: Cloudflare = designated production runtime; D1 = active PJ operational persistence; Windows Runner = local execution plane. Render and old Postgres = DEPRECATED / UNUSED / NON-AUTHORITATIVE; NO MIGRATION REQUIRED. External resources are not deleted.
- Runtime target: Telegram webhook -> unified projectmanager-control-plane Worker -> shared application/Ops Center -> D1/jobs -> project-bound authenticated Windows Runner. Production Worker imports no Node bot bootstrap/Config DB/Excel/shell execution.
- Implementation: bounded secret-verified webhook, private-chat admin authorization, durable replay leases, shared admin/status/incident/runner UI, safe typed diagnostic jobs; existing Control Plane routes preserved/aliased. Legacy Node polling and Config DB warmup explicitly opt-in.
- Persistence: D1-only migrations under migrations/d1; cross-isolate atomic claims, result compare-and-swap and attempt/terminal-owner verification. Managed config and audit reuse existing D1 store.
- Deployment status: CLOUDFLARE_DEPLOYMENT_PENDING_SECRETS: account confirmed by owner; D1 created and all three remote migrations applied. Worker deployment, webhook activation and live smoke checks await interactive secret provisioning.
- Excel: PJ-011B intermittent COM/open/read/quit evidence retained, underlying cause unresolved. PJ-012 keeps diagnostics and adds stage/provenance metadata; no authoritative XLSM/VBA changes, no new host probes, no sync.
- Validation and exact test totals: docs/handoffs/PJ-012/REVIEW.md. Activation commands/safety and parity limits: CLOUDFLARE-RUNBOOK.md.
- Finalizer: Codex DryRun only; owner publishes append-only external Review/Plans manually.
- Next milestone: authenticated Cloudflare activation and safe Telegram->D1->Windows Runner production health smoke; Excel host investigation separately observable. No additional architecture/paid-service decision needed.

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
