# Current State

## Control Plane

Status: **implemented and test-covered foundation**.

- Operational event envelopes are versioned and normalized.
- Authentication and project scope are enforced for events, jobs, runners, alerts, and status reads.
- Event idempotency, correlation IDs, bounded context, redaction, alert fingerprints, recovery handling, and D1 persistence are implemented.
- Event categories include provider/API failures, AI timeouts, provider unavailability, Excel sync failures, backup failures, and runner offline conditions.
- Transport retry/backoff remains a caller responsibility; the Control Plane provides safe idempotent replay handling.

## Runner

Status: **local Windows Runner boundary implemented; production/cloud verification pending**.

- Heartbeats transition through healthy, stale/offline, and recovery states.
- Runner identity and repository roots are project-bound.
- Execution uses an allowlisted typed job model and argument arrays with `shell: false`.
- Arbitrary shell, unrestricted PowerShell, secret extraction, destructive database work, and unrestricted deployment are outside the runner contract.
- Cloud Runner connectivity and production verification remain open.

## Job System

Status: **typed non-production execution boundary implemented; production publication remains disabled**.

- Existing runner jobs have allowlists, leases, attempts, timeouts, permissions, and bounded results.
- Validation-only schemas cover `HEALTHCHECK`, `PROJECT_STATUS`, `GIT_STATUS`, `RUN_TESTS`, `RUN_TYPECHECK`, `DATABASE_DIAGNOSTIC`, `EXCEL_HEALTHCHECK`, `EXCEL_SNAPSHOT`, `EXCEL_BACKUP`, `EXCEL_SYNC`, and related prepared Excel contracts.
- PJ-007 adds validation for `HEALTHCHECK`, `EXCEL_HEALTHCHECK`, `EXCEL_BACKUP`, `EXCEL_SYNC`, and `RUN_TESTS` with `executionAllowed: false`.
- PJ-009/PJ-010 execute only typed local non-production Excel health, snapshot, backup, and reconciliation operations through the runner. Authoritative sync/publication remains disabled.

## Provider/API

Status: **metadata and sanitization foundation only**.

- Project-scoped provider metadata supports Gemini-style AI providers and future services.
- Status, health, service type, usage metadata, and Telegram-safe presentation are validated.
- Secret-shaped fields are rejected or removed from operator-facing output.
- No durable provider registry/API, external provider connection, HMAC integration, or token revocation flow is enabled.

## DailySystem View

Status: **sanitized operational view with non-production adapter evidence**.

The view can represent Excel, runner, backup, sync, reconciliation, and provider status. DailySystem remains the owner of workbook semantics, reconciliation rules, backup policy, and business data.

## PJ-010 DailySystem Adapter And Windows Evidence

Status: **implemented, fixture-tested, and non-production bounded**.

- `src/dailySystemAdapter.js` provides typed health, project-status, Excel diagnostic, and reconciliation calls with correlation propagation, bounded timeouts/body size, deterministic classification, and allowlisted redaction.
- The runner records source hashes before/after Excel health probes, uses disposable copies for openability checks, and has a fixed Windows COM probe with macros, events, link updates, and prompts disabled.
- Reconciliation metadata is project-scoped and operational-only. Duplicate evidence is idempotent and failures/recoveries use the existing event/incident engine.

## Verification Baseline

- `npm test`: 150 passed, 0 failed (latest recorded duration approximately 31 seconds).
- `npm run check`: passed.
- Git status/identity cannot be verified because the current tree has no `.git` metadata.

## PJ-008 Configuration And Archive Foundation

Status: **implemented, non-production, test-covered foundation**.

- Managed operational configuration validates admin/archive settings, project bindings, desktop profiles, manually configured Excel assets, and provider metadata.
- Configuration changes emit attributable, timestamped, sanitized `CONFIG_CHANGED` events.
- One shared private Telegram archive channel is modeled through project-tagged archive records and a disabled Telegram adapter contract.
- Backup metadata, restore transitions, and retention eligibility are modeled without automatic destructive deletion.
- Latest verification: `npm test` 132 passed, 0 failed; `npm run check` passed.

## PJ-009 Durable Operations And Runner Integration

Status: **implemented, non-production, test-covered foundation**.

- D1 migration `20260925_pj009_operational.sql` persists managed config, project config, desktop profiles, Excel assets, provider operations, archives, backups, retention candidates, reconciliation metadata, and audit history.
- D1 and in-memory stores share the operational-state contract; malformed operational JSON is ignored rather than trusted.
- Windows Runner supports typed Excel health, snapshot, backup, and structured reconciliation operations. Authoritative Excel sync publication remains disabled.
- Job results emit allowlisted events, preserve project scope, reject stale leases, support retryable failures, and persist verified backup/reconciliation metadata.
- Latest verification: `npm test` 150 passed, 0 failed; `npm run check` passed.

## PJ-011B desktop resume — 2026-09-30

Canonical baseline 6d4169e; five interrupted dirty files preserved. Repository hardening has typed Config DB preflight/classification, repeat-safe DSN repair, bounded transient retries, active incident warning dedupe, and one recovery contract. The reported Supabase tenant/user error is a provider routing/authentication class, not hostname evidence. README/Git history document Render as intended bot hosting, but ACTIVE_NODE_HOST_UNKNOWN remains. Worker/D1 is a separate Control Plane and does not execute legacy Telegram/Config DB warmup.

Today's host evidence supersedes any assumption that the older successful probe closed the incident. Mirax repeatedly timed out at WORKBOOK_OPEN_START; refresh suppression helped once but did not consistently resolve it, and a later COM_CREATE timeout prevents a confirmed underlying root cause. Gozareshkar opens/reads/closes; verified process cleanup recovers a quit timeout with an explicit warning. Original XLSM/VBA hashes remain unchanged. New automation PID 31620 has unverified ownership and remains untouched; PID 2868 was preserved. Overall no-orphan verification is incomplete.

CODEX_TASK project/mode/sandbox/environment/output safety is tightened. Local Control Plane/claim/lease/real Excel/typed result was exercised; cloud runtime verification and authoritative sync remain disabled/pending. Detailed review, Config DB diagnosis, Excel diagnosis and safe JSON evidence live under docs/handoffs/PJ-011B. Next milestone: PJ-012 runtime provenance plus interactive-host COM/Mirax closure, without production changes until separately approved.

Final verification: Gozareshkar also timed out at WORKBOOK_READ_PROBE after successful open; its owned process was cleaned up and source unchanged. Host instability remains unresolved. Release suite: 180/180 passed; syntax/check and diff checks passed.

## PJ-012 Cloudflare production direction — 2026-09-30

PJ-011B finalized/pushed at 7a2a6f010d3f4756aaf76816c0a1faa303839e99 with clean synchronized main. Owner superseded legacy runtime provenance discovery: Render and old external Config DB are DEPRECATED / UNUSED; NO MIGRATION REQUIRED; no deletion. Cloudflare is the designated production runtime, D1 the operational persistence, Windows Runner the local execution plane. Implementation reuses the existing Control Plane, Ops Center models and typed jobs. Node polling/legacy DB warmup are explicit compatibility opt-ins; Worker has no legacy DSN dependency.

PJ-012 local Worker/D1/Runner validation is recorded under docs/handoffs/PJ-012. CLOUDFLARE_DEPLOYMENT_PENDING_SECRETS: account confirmed by owner; D1 created and all three remote migrations applied. Worker deployment, webhook activation and live smoke checks await interactive secret provisioning. No production webhook activation or live smoke success is claimed. Safe activation runbook is repository-local. Excel intermittent-host evidence remains unresolved and observable; source workbooks/VBA untouched, sync disabled. Next: authenticated activation and safe production health E2E, then separate Excel host closure. Historical memory remains preserved above.

PJ-012 owner follow-up: bootstrap Telegram administrator 843686302 configured; no secret value supplied in chat.
