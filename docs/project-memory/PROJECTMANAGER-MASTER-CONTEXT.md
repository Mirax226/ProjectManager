# ProjectManager Master Context

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

- Canonical memory path: `docs/project-memory/` in the canonical Git repository (`C:\Users\Amir\Documents\GitHub\cloned\ProjectManager`).
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
