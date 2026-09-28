# PJ-006 Desktop Profile and Excel Asset Foundation

## Scope

PJ-006 adds validation-only foundation models for trusted desktop environments, registered Excel assets, and future typed Excel jobs. It does not execute Excel, open or modify workbooks, run commands, deploy, or connect production systems.

## Desktop Profile Model

Implemented in `src/excelAssets.js` as `validateDesktopProfile`.

Fields:

- `id`: stable profile key matching the ProjectManager identifier format.
- `name`: human-readable desktop profile name.
- `runnerId`: bound Windows Runner identity.
- `status`: `ONLINE`, `STALE`, `OFFLINE`, or `UNKNOWN`.
- `heartbeat.lastSeenAt`: last observed heartbeat timestamp.
- `heartbeat.lastSuccessfulAt`: last successful communication timestamp.
- `heartbeat.intervalMs`: bounded expected heartbeat interval.

The model does not probe the desktop or establish a runner connection. Runner authentication and heartbeat ownership remain in the existing Control Plane.

## Excel Asset Model

Implemented as `validateExcelAsset`.

Fields:

- `project`: normalized project key.
- `assetName`: operator-facing asset name.
- `filename`: restricted to `Mirax.xlsm` or `Gozareshkar.xlsm`.
- `manualPath`: optional operator-provided path metadata, stored as text only.
- `backupPath`: optional backup path metadata, stored as text only.
- `status`: `UNVERIFIED`, `VERIFIED`, `MISSING`, `STALE`, or `ERROR`.
- `verificationTimestamp`: normalized ISO timestamp or `null`.

The validator rejects unsupported filenames, invalid project keys, invalid timestamps, and control characters in paths. It does not read, verify, copy, or modify any path.

## Prepared Typed Jobs

`validatePreparedExcelJob` prepares schemas for:

- `EXCEL_HEALTHCHECK`
- `EXCEL_SNAPSHOT`
- `EXCEL_BACKUP`

Each prepared job requires a valid project and asset reference, uses project/runner scope checks, rejects command-like payload fields, bounds the timeout, and returns `executionAllowed: false`.

### `EXCEL_HEALTHCHECK`

- Read-only status probe schema.
- Requires `assetId`; optional checks are metadata only.
- Maximum prepared timeout: 60 seconds.

### `EXCEL_SNAPSHOT`

- Read-only metadata/checksum snapshot schema.
- Requires `assetId`, `idempotencyKey`, and `mode: read_only`.
- Maximum prepared timeout: 180 seconds.

### `EXCEL_BACKUP`

- Controlled future artifact schema.
- Requires `assetId`, `destinationRef`, and `idempotencyKey`.
- Maximum prepared timeout: 300 seconds.
- No backup destination is accessed by PJ-006.

These job types are deliberately not added to the executable `JOB_TYPES` list or runner executor. A later implementation must add explicit executor functions, tests, approval policy, and artifact handling before enablement.

## Permission Boundaries

- `admin` and `owner` actors may validate any registered project job.
- `project` actors may validate jobs for their own project.
- `runner` actors may validate execution-shaped jobs only for their bound project and cannot act as job requesters.
- Cross-project asset and job validation fails with `project scope denied`.
- No validator accepts shell, PowerShell, exec, token, secret, password, or DSN fields.

## Windows Runner Responsibility

Future execution belongs to the project-bound Windows Runner. It must resolve only registered asset references inside the configured project root, enforce the Control Plane lease and timeout, use fixed reviewed Excel/COM adapter code, release workbook/COM resources on all paths, and return bounded sanitized metadata. It must never expose arbitrary commands, raw workbook contents, credentials, or unrestricted filesystem access.

## Integration Boundary

```text
DailySystem workbook assets
          |
  authenticated HTTPS API
          |
ProjectManager Control Plane
          |
project-bound Windows Runner
```

DailySystem owns workbook semantics, sync/reconciliation rules, backup policy, and business data. ProjectManager owns project-scoped identity, job lifecycle, permissions, heartbeat state, sanitized operational events, and operator visibility. No Cloudflare Service Binding or production endpoint is required by this foundation.

## Tests

- `test/excelAssets.test.js` covers desktop profile validation, supported asset validation, project permission boundaries, prepared job schemas, timeout bounds, idempotency requirements, and unsafe field rejection.
- `npm test`: PASS, 122 total, 122 passed, 0 failed.
- Node test duration: 31,064.868 ms; shell-measured elapsed time approximately 31.78 seconds.
- `npm run check`: PASS.
- `node --check src/excelAssets.js`: PASS.

## Safety State

- Excel execution was not enabled.
- No workbook was opened or modified.
- No arbitrary command path was added.
- No deployment, production integration, secret change, commit, or push was performed.

## Remaining Gaps

- Persisted desktop-profile and asset storage is not yet connected to D1/PostgreSQL.
- Prepared jobs are not executable and are absent from the live Control Plane allowlist.
- Desktop heartbeat and asset verification require a future non-production adapter.
- Backup retention, restore verification, and artifact lifecycle policy remain undefined.
- HMAC/revocation integration work from PJ-004 remains open.

## Next Phase

PJ-007 — Non-Production Excel Fixtures and Contract Tests

Add fixture-only adapters and contract tests for validation, authorization, idempotency, timeout, redaction, and failure reporting without starting Excel or connecting DailySystem.
