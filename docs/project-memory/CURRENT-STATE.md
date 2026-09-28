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
