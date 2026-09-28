# PJ-007 Operational Platform Expansion

## Scope

PJ-007 adds validation and presentation foundations for external providers, operational event categories, DailySystem status, and typed operational jobs. No production job was executed, no provider integration was enabled, and no secret is stored or shown in Telegram.

## API Provider Management

Added `src/providerRegistry.js` with validation for project-scoped provider metadata:

- provider name
- service type (`AI`, `DATABASE`, `TELEGRAM`, `EXCEL`, `BACKUP`, `OTHER`)
- status (`ENABLED`, `DISABLED`, `DEGRADED`, `UNAVAILABLE`, `UNKNOWN`)
- health (`HEALTHY`, `DEGRADED`, `DOWN`, `UNKNOWN`)
- timestamp
- usage metadata: requests, errors, units, last-used timestamp

Gemini API is represented as a valid `AI` provider example. Future AI providers use the same metadata boundary.

Provider validation rejects secret-shaped fields such as tokens, API keys, credentials, passwords, DSNs, and private keys. `sanitizeProviderForTelegram` strips those fields before presentation and keeps only operational usage/health metadata.

Provider access is project-scoped; admin/owner actors can inspect any project, while project/runner actors must match the provider project.

## Operational Events

The Control Plane event contract now supports:

- `API_FAILED`
- `AI_TIMEOUT`
- `PROVIDER_UNAVAILABLE`
- `EXCEL_SYNC_FAILED`
- `BACKUP_FAILED`
- existing `RUNNER_OFFLINE`

Events now include a normalized `component` field, along with project, severity, timestamp, source, correlation ID, and redacted context. The D1 event schema and loader persist the component without changing the existing project/event ownership boundary.

## DailySystem Operations View

Added `src/dailySystemOps.js` with a sanitized read-only view model for:

- Excel status
- Runner status
- Backup status
- Sync status
- Reconciliation status
- provider health/status summaries

The view contains status and bounded timestamps/messages only. It does not include workbook contents, credentials, or raw provider configuration.

## Typed Operational Jobs

Added `src/operationalJobs.js` with non-executing schema validation for:

- `HEALTHCHECK`
- `EXCEL_HEALTHCHECK`
- `EXCEL_BACKUP`
- `EXCEL_SYNC`
- `RUN_TESTS`

The validator enforces project scope, asset/destination requirements, dry-run mode for Excel sync, idempotency keys for side-effect-shaped jobs, timeout bounds, and rejection of command/shell/PowerShell/secret fields. Every validated result has `executionAllowed: false`.

These types were not added to the live executable runner allowlist. Execution remains a future, separately reviewed step.

## Tests

Added `test/providerAndOps.test.js` covering:

- provider validation and Telegram sanitization
- provider/job project isolation
- new operational event categories and component redaction
- typed job validation and unsafe-field rejection
- DailySystem operations view rendering

Verification:

- `npm test`: PASS, 126 total, 126 passed, 0 failed.
- Node test duration: 31,020.434 ms; shell-measured elapsed time approximately 31.60 seconds.
- `npm run check`: PASS.
- `node --check src/providerRegistry.js`: PASS.
- `node --check src/operationalJobs.js`: PASS.
- `node --check src/dailySystemOps.js`: PASS.

## Architecture Impact

- Provider metadata is an operational control-plane concern; provider credentials remain outside these models.
- DailySystem remains the owner of business data and Excel semantics.
- ProjectManager owns project-scoped authentication, job validation, event redaction, status summaries, and operator visibility.
- The Windows Runner remains the only future local execution boundary.
- No arbitrary shell transport, Cloudflare Service Binding, or production integration was introduced.

## Remaining Gaps

- Provider metadata has no durable registry/API yet.
- HMAC authentication and explicit provider-token revocation remain future work.
- Operational view data is model-only; it is not connected to live DailySystem, Excel, backup, or provider systems.
- Typed job schemas are not executable and require fixture adapters, policy, and non-production contract tests before enablement.
- Event migration deployment and production event producers remain unverified.
- The current tree has no `.git` metadata, so Git status and commit identity cannot be independently verified.

## Safety State

No production jobs were executed. No secrets were exposed, changed, or rotated. No arbitrary shell was enabled. No deployment, commit, or push was performed.

## Next Phase

PJ-008 — Non-Production Provider and Operational Event Fixtures

Add in-memory provider/event stores and fixture producers, then test lifecycle transitions, revocation behavior, event routing, and project isolation without connecting external providers or DailySystem.
