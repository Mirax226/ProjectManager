# PJ-005 Operational Excel Job Foundation Design

## Design Status

Documentation-only design. No Excel job was added to `JOB_TYPES`, no executor was enabled, no workbook was opened, and no production integration was configured.

The design extends the existing typed-job model in `src/controlPlane/contracts.js`, `src/controlPlane/store.js`, and `src/runner/jobExecutor.js` without changing the current runtime behavior.

## Common Job Contract

All future Excel jobs use the existing job envelope plus a bounded, project-scoped payload:

```json
{
  "projectId": "daily-system",
  "type": "EXCEL_SNAPSHOT",
  "idempotencyKey": "stable-request-key",
  "payload": {
    "schemaVersion": 1,
    "correlationId": "corr-123"
  }
}
```

Common rules:

- `projectId` is required and must match the authenticated project/runner scope.
- `idempotencyKey` is required for operations that can create an artifact or external side effect.
- Workbook references are relative identifiers or pre-registered artifact IDs; arbitrary filesystem paths are not accepted.
- Payloads contain no passwords, tokens, DSNs, or raw credentials.
- Every job has a bounded timeout, lease, attempt count, and sanitized result.
- The runner reports a safe operational event with the job ID and `correlationId`; it does not send workbook contents to Telegram or the Control Plane.

## Typed Job Designs

### `EXCEL_SYNC`

Purpose: synchronize an explicitly registered workbook/data source under a future project adapter.

Input schema:

```json
{
  "schemaVersion": 1,
  "correlationId": "corr-123",
  "sourceRef": "registered-source-id",
  "workbookRef": "registered-workbook-id",
  "mode": "dry_run",
  "sheets": ["Sheet1"],
  "idempotencyKey": "sync-2026-09-25T10:00"
}
```

`mode` must initially be `dry_run`; a future write mode requires a separate explicit policy and approval gate.

- Permissions: project admin/requester may create; only a runner bound to the same project may claim; adapter policy must allow the source and workbook.
- Timeout: 10 minutes maximum, with a shorter project default.
- Output schema: `{ ok, projectId, jobId, mode, plannedRows, changedRows, skippedRows, warnings, artifactRefs, readOnly }`.
- Failure reporting: safe error code (`SOURCE_UNAVAILABLE`, `WORKBOOK_LOCKED`, `SCHEMA_MISMATCH`, `TIMEOUT`), phase, retryable flag, and correlation ID. No cell values or credentials.

### `EXCEL_SNAPSHOT`

Purpose: capture a metadata/checksum snapshot for a registered workbook without changing it.

Input schema:

```json
{
  "schemaVersion": 1,
  "correlationId": "corr-123",
  "workbookRef": "registered-workbook-id",
  "includeSheets": ["Sheet1", "Sheet2"],
  "includeFormulas": false,
  "idempotencyKey": "snapshot-workbook-2026-09-25"
}
```

- Permissions: project admin/requester may create; same-project runner only; read-only policy.
- Timeout: 3 minutes maximum.
- Output schema: `{ ok, projectId, jobId, workbookRef, snapshotRef, workbookVersion, sheetCount, rowCounts, checksum, capturedAt, readOnly }`.
- Failure reporting: `WORKBOOK_NOT_FOUND`, `EXCEL_UNAVAILABLE`, `LOCKED`, `SNAPSHOT_TOO_LARGE`, or `TIMEOUT`, with safe diagnostics only.

### `EXCEL_RECONCILIATION`

Purpose: compare two registered snapshots or workbook views and report differences without applying changes.

Input schema:

```json
{
  "schemaVersion": 1,
  "correlationId": "corr-123",
  "leftSnapshotRef": "snapshot-a",
  "rightSnapshotRef": "snapshot-b",
  "rulesetRef": "registered-ruleset-id",
  "idempotencyKey": "reconcile-snapshot-a-snapshot-b"
}
```

- Permissions: project admin/requester may create; same-project runner only; comparison ruleset must be registered and read-only.
- Timeout: 15 minutes maximum.
- Output schema: `{ ok, projectId, jobId, equal, differenceCount, differenceSummary, rulesetRef, reportRef, readOnly }`.
- Failure reporting: `SNAPSHOT_MISSING`, `RULESET_INVALID`, `SCHEMA_MISMATCH`, `DIFF_LIMIT`, or `TIMEOUT`; emit `EXCEL_RECONCILIATION_ERROR` with bounded summary and correlation ID.

### `EXCEL_HEALTHCHECK`

Purpose: verify desktop/Excel availability and the registered workbook adapter without reading business data.

Input schema:

```json
{
  "schemaVersion": 1,
  "correlationId": "corr-123",
  "workbookRef": "registered-workbook-id",
  "checks": ["desktop", "excel_com", "workbook_open"]
}
```

- Permissions: project admin/requester may create; same-project runner only; read-only.
- Timeout: 60 seconds maximum.
- Output schema: `{ ok, projectId, jobId, checks: [{ name, status, latencyMs, version }], runnerId, observedAt, readOnly }`.
- Failure reporting: `DESKTOP_UNAVAILABLE`, `EXCEL_COM_UNAVAILABLE`, `WORKBOOK_LOCKED`, or `TIMEOUT`; emit `EXCEL_OFFLINE` or `HEALTHCHECK_FAILED` without raw host secrets.

### `EXCEL_BACKUP`

Purpose: create a controlled backup artifact for a registered workbook.

Input schema:

```json
{
  "schemaVersion": 1,
  "correlationId": "corr-123",
  "workbookRef": "registered-workbook-id",
  "destinationRef": "registered-backup-location",
  "retentionClass": "standard",
  "idempotencyKey": "backup-workbook-2026-09-25T10:00"
}
```

- Permissions: project admin/requester plus an explicit project backup policy; same-project runner only. This is `CONTROLLED`, never implicit or public.
- Timeout: 5 minutes maximum.
- Output schema: `{ ok, projectId, jobId, backupRef, checksum, sizeBytes, completedAt, retentionClass }`.
- Failure reporting: `BACKUP_DESTINATION_DENIED`, `WORKBOOK_LOCKED`, `DISK_FULL`, `CHECKSUM_FAILED`, or `TIMEOUT`; emit a sanitized backup failure event and never expose destination credentials.
- Enablement rule: remain disabled until the destination policy, retention, restore test, and approval flow are implemented. No backup is executed by PJ-005.

## Risk and Job Classification

- `EXCEL_HEALTHCHECK`, `EXCEL_SNAPSHOT`, and dry-run `EXCEL_RECONCILIATION` are `READ_ONLY`.
- `EXCEL_SYNC` is `CONTROLLED` even when initially dry-run because it establishes a future write boundary.
- `EXCEL_BACKUP` is `CONTROLLED` because it creates an artifact and requires retention policy.
- None of these types may accept arbitrary commands, PowerShell text, workbook paths, credentials, or deployment instructions.

## Failure and Event Contract

Every completed job returns a bounded result and emits a sanitized event with:

- `schemaVersion: 1`
- stable `eventId`
- project/environment/severity/category
- `jobId` and `correlationId`
- safe message and bounded context

Retryable failures may be retried through the existing idempotent job/lease lifecycle. Non-retryable failures remain failed and require a new idempotency key only after operator review.

## Windows Runner Responsibility

The Windows Runner is responsible for local desktop and Excel adapter execution only:

- verify the runner is bound to the requested project;
- resolve registered workbook/artifact references to paths inside the configured project root;
- enforce per-job timeout, single-job lease ownership, and bounded output;
- use fixed, reviewed Excel/COM adapter functions rather than arbitrary PowerShell;
- close workbooks and release COM objects in success and failure paths;
- never return workbook contents, credentials, or unrestricted filesystem listings;
- report only sanitized metadata, checksums, counts, and failure codes.

ProjectManager remains responsible for authentication, authorization, job lifecycle, event storage, alerting, and operator views. The Windows Runner does not decide project permissions.

## DailySystem Integration Boundary

```text
DailySystem adapter / registered workbook
              |
       authenticated HTTPS API
              |
ProjectManager Control Plane -> project-bound Windows Runner
```

DailySystem owns workbook semantics, sync rules, reconciliation rules, backup policy, and business data. ProjectManager owns operational coordination and safe status summaries. The integration must report:

- Excel sync status;
- desktop/Excel availability;
- backup completion or failure notification;
- reconciliation failure summaries.

It must not copy DailySystem business data into ProjectManager, expose workbook paths or credentials, or require a Cloudflare Service Binding.

## Explicit Non-Goals

- No `JOB_TYPES` or executor implementation was changed.
- No Excel process was started.
- No workbook, backup, reconciliation, or sync operation was executed.
- No production endpoint, token, secret, or deployment configuration was changed.

## Validation

- `npm test`: PASS, 119 total, 119 passed, 0 failed.
- Node test duration: 31,025.650 ms; shell-measured elapsed time approximately 31.55 seconds.
- `npm run check`: PASS.

## Next Phase

PJ-006 — Typed Excel Contract Review and Non-Production Fixtures

Define fixture-only schemas and mock adapters for the five job types, then test validation, authorization, timeout, redaction, idempotency, and failure reporting without starting Excel or connecting DailySystem.
