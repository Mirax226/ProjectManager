# Roadmap

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

PJ-012 local Worker/D1/Runner validation is recorded under docs/handoffs/PJ-012. CLOUDFLARE_DEPLOYMENT_PENDING_SECRETS: account confirmed by owner; D1 created and all three remote migrations applied. Worker deployment, webhook activation and live smoke checks await administrator IDs and interactive secret provisioning. No production webhook activation or live smoke success is claimed. Safe activation runbook is repository-local. Excel intermittent-host evidence remains unresolved and observable; source workbooks/VBA untouched, sync disabled. Next: authenticated activation and safe production health E2E, then separate Excel host closure. Historical memory remains preserved above.
