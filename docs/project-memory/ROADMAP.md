# Roadmap

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


## Completed Foundations

- PJ-001: audit checkpoint and architecture baseline.
- PJ-002: Control Plane contract and multi-project boundary hardening.
- PJ-003: Telegram operations center views and safe actions.
- PJ-004: runner and integration boundary audit.
- PJ-005: typed Excel job design.
- PJ-006: desktop profiles and registered Excel asset validation.
- PJ-007: provider metadata, operational event categories, DailySystem status model, and validation-only jobs.

## Next Milestones

### PJ-008 - Repository Provenance, Configuration, And Archive Foundation (completed)

Managed configuration, shared archive contracts, backup metadata, retention candidates, safe admin summaries, and provenance investigation are complete. Production Telegram transport and physical Excel execution remain disabled.

### PJ-009 - Durable Operations And Runner/Excel Integration (completed)

Additive D1 operational persistence, typed runner Excel health/snapshot/backup/reconciliation operations, provider health fixtures, allowlisted job-result events, and safe Telegram typed-job surfaces are complete. Production Excel publication, Telegram transport, and deletion remain disabled.

### PJ-010 - DailySystem Adapter Verification And Controlled Non-Production Execution (implemented; independent closure review pending)

Typed DailySystem health/status/Excel/reconciliation adapter, bounded failure classification, fixture coverage, controlled disposable-copy Excel evidence, source fingerprint verification, event/incident lifecycle integration, and safe Telegram operational views are implemented. Production connectivity, authoritative publication, and real Telegram transport remain disabled.

### PJ-010 - Persistence and Recovery Policy

Decide durable ownership for provider metadata, desktop profiles, and Excel assets. Define retention, archive, restore, migration, and recovery evidence before any production enablement.

### Later Enablement Gate

Only after explicit review: enable a narrowly scoped non-production runner adapter, then separately review production connectivity, approval policy, secrets handling, rollback, and monitoring. No milestone authorizes arbitrary execution or unrestricted deployment.

## Ordering Constraints

1. Close contract and project-scope evidence.
2. Add fixture and failure-path coverage.
3. Define persistence and archive/recovery policy.
4. Review adapter permissions and operational approvals.
5. Consider integrations only after non-production evidence is complete.

## PJ-011B desktop resume — 2026-09-30

Canonical baseline 6d4169e; five interrupted dirty files preserved. Repository hardening has typed Config DB preflight/classification, repeat-safe DSN repair, bounded transient retries, active incident warning dedupe, and one recovery contract. The reported Supabase tenant/user error is a provider routing/authentication class, not hostname evidence. README/Git history document Render as intended bot hosting, but ACTIVE_NODE_HOST_UNKNOWN remains. Worker/D1 is a separate Control Plane and does not execute legacy Telegram/Config DB warmup.

Today's host evidence supersedes any assumption that the older successful probe closed the incident. Mirax repeatedly timed out at WORKBOOK_OPEN_START; refresh suppression helped once but did not consistently resolve it, and a later COM_CREATE timeout prevents a confirmed underlying root cause. Gozareshkar opens/reads/closes; verified process cleanup recovers a quit timeout with an explicit warning. Original XLSM/VBA hashes remain unchanged. New automation PID 31620 has unverified ownership and remains untouched; PID 2868 was preserved. Overall no-orphan verification is incomplete.

CODEX_TASK project/mode/sandbox/environment/output safety is tightened. Local Control Plane/claim/lease/real Excel/typed result was exercised; cloud runtime verification and authoritative sync remain disabled/pending. Detailed review, Config DB diagnosis, Excel diagnosis and safe JSON evidence live under docs/handoffs/PJ-011B. Next milestone: PJ-012 runtime provenance plus interactive-host COM/Mirax closure, without production changes until separately approved.

Final verification: Gozareshkar also timed out at WORKBOOK_READ_PROBE after successful open; its owned process was cleaned up and source unchanged. Host instability remains unresolved. Release suite: 180/180 passed; syntax/check and diff checks passed.

## PJ-012 Cloudflare production direction — 2026-09-30

PJ-011B finalized/pushed at 7a2a6f010d3f4756aaf76816c0a1faa303839e99 with clean synchronized main. Owner superseded legacy runtime provenance discovery: Render and old external Config DB are DEPRECATED / UNUSED; NO MIGRATION REQUIRED; no deletion. Cloudflare is the designated production runtime, D1 the operational persistence, Windows Runner the local execution plane. Implementation reuses the existing Control Plane, Ops Center models and typed jobs. Node polling/legacy DB warmup are explicit compatibility opt-ins; Worker has no legacy DSN dependency.

PJ-012 local Worker/D1/Runner validation is recorded under docs/handoffs/PJ-012. CLOUDFLARE_DEPLOYMENT_PENDING_SECRETS: account confirmed by owner; D1 created and all three remote migrations applied. Worker deployment, webhook activation and live smoke checks await interactive secret provisioning. No production webhook activation or live smoke success is claimed. Safe activation runbook is repository-local. Excel intermittent-host evidence remains unresolved and observable; source workbooks/VBA untouched, sync disabled. Next: authenticated activation and safe production health E2E, then separate Excel host closure. Historical memory remains preserved above.

PJ-012 owner follow-up: bootstrap Telegram administrator 843686302 configured; no secret value supplied in chat.
