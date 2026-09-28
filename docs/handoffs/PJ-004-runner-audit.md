# PJ-004 Runner and Integration Foundation Audit

## Scope

This audit covers the current Windows Runner, the Cloud Runner/control-plane boundary, project authentication, heartbeat monitoring, typed job execution, and preparation for a future private-project integration. No production system was connected or enabled.

## Current Runner Implementation

### Authentication

- `src/runner/client.js` sends an HTTPS bearer token from `PG_RUNNER_TOKEN` to `PG_CONTROL_PLANE_URL`.
- Runner identity comes from `PG_RUNNER_ID`; project identity comes from `PG_PROJECT_ID`.
- `src/controlPlane/auth.js` compares bearer secrets without logging them and returns a runner identity with its configured project binding.
- `src/controlPlane/handler.js` requires project-bound runner credentials and rejects cross-project heartbeat, claim, result, and status access.
- Authentication is currently bearer-token based. There is no HMAC verification or token-revocation API yet.

### Heartbeat Flow

1. `src/runner/index.js` sends an initial heartbeat and repeats it every 30 seconds by default.
2. The heartbeat contains runner ID, project ID, version, host label, and capabilities.
3. `ControlPlaneStore.heartbeat()` stores the latest successful communication timestamp.
4. A runner is `STALE` after 90 seconds by default and `OFFLINE` after 300 seconds by default.
5. `refreshRunnerStates()` emits one `RUNNER_OFFLINE` event on transition.
6. The next authenticated heartbeat changes the runner to `ONLINE` and emits `RUNNER_RECOVERED`.

Heartbeat fields:

- runner ID
- assigned project
- status (`ONLINE`, `STALE`, or `OFFLINE`)
- last-seen timestamp
- version
- host label
- declared capabilities

### Job Lifecycle

- An authenticated admin or project-scoped caller creates a job.
- The Control Plane validates the project and allowlisted type, assigns a risk class, and stores `PENDING` state.
- Optional `(project_id, idempotency_key)` uniqueness prevents duplicate requests.
- A project-bound runner claims the oldest pending or expired lease job.
- Claims become `CLAIMED`, increment `attemptCount`, and receive a 120-second default lease.
- The runner executes the typed operation and submits a result.
- Results become `SUCCEEDED` or `FAILED`; duplicate result delivery is idempotent.
- Lease ownership and expiry prevent another runner from completing a live lease.

### Permission Model

- Project keys are normalized and must be registered in the Control Plane.
- Project tokens can read/write only their own project scope.
- Runner tokens must declare one project and cannot override it in request payloads.
- Admin tokens may inspect all configured projects and create jobs for a selected project.
- Job types are allowlisted in `src/controlPlane/contracts.js`; unknown and `DANGEROUS` types are rejected.
- Repository paths are absolute and constrained to the configured project path/root.

### Storage Model

- Local development/tests use `ControlPlaneStore` in memory.
- Cloud deployment uses `D1ControlPlaneStore` and `migrations/20260924_control_plane.sql`.
- D1 owns only control-plane projects, events, jobs, runners, and alerts.
- The legacy PostgreSQL/config database remains authoritative for the existing Node runtime and project configuration.
- Runner processes do not own durable state; they report state and results to the Control Plane.

## Runner Security Review

### Verified Protections

- No arbitrary shell command field is accepted by the job executor.
- Child processes use argument arrays and `shell: false`.
- PowerShell is used only for a fixed, read-only Excel connectivity diagnostic with `-NoProfile` and `-NonInteractive`.
- Git, npm, typecheck, health, project status, and Excel diagnostics are typed operations.
- Output is truncated to a bounded size and result data is redacted before persistence.
- Codex edit mode is disabled unless the explicit `PG_ALLOW_CODEX_EDITS=true` policy is present.
- No database destruction or direct production deployment job is defined.

### Current Typed Jobs

`HEALTHCHECK`, `PROJECT_STATUS`, `GIT_STATUS`, `RUN_TESTS`, `RUN_TYPECHECK`, `DAILYSYSTEM_HEALTHCHECK`, `EXCEL_CONNECTIVITY_TEST`, `EXCEL_SYNC_DIAGNOSTIC`, `EXCEL_RECONCILIATION_CHECK`, and read-only-by-default `CODEX_TASK`.

`DATABASE_DIAGNOSTIC` is a future typed read-only job, not a current implementation. It must be added with an explicit executor and tests before use.

### Out of Scope / Not Enabled

- arbitrary shell execution
- unrestricted PowerShell
- secret extraction or secret display
- database destruction
- direct production deployment
- production runner or Cloud Runner connectivity

## Integration Contract

The intended boundary is:

```text
Private Project
      |
Authenticated HTTPS API
      |
ProjectManager Control Plane
```

Contract requirements:

- Every request carries a project identity and is checked against project-scoped credentials.
- Credentials are unique to a project and are never shared across projects.
- Credentials must be revocable by removing/disabling the project token in the deployment secret/configuration store; an explicit revocation registry is a future hardening item.
- The bearer contract should remain compatible with a future HMAC mode using timestamp, nonce, body hash, and key ID without requiring a Cloudflare Service Binding.
- The integration uses ordinary authenticated HTTPS and remains portable between local Node, Windows Runner, and Cloudflare Worker deployments.
- Responses and operational events contain sanitized summaries, correlation IDs, and no raw credentials or business data.

## Windows Runner Role

The Windows Runner is the trusted local execution boundary for approved Git/npm/typecheck/Excel/Codex diagnostics against a configured project workspace. It owns local process invocation and filesystem checks; it does not expose an inbound arbitrary-command interface.

## Cloud Runner Role

The Cloud Runner is the authenticated Worker/D1 control-plane side responsible for project state, event ingestion, leases, runner registry, job dispatch, and status reads. It should remain independent of Cloudflare Service Bindings so the contract can be hosted or tested through ordinary HTTPS.

## DailySystem Preparation Only

No DailySystem integration was implemented. Future project-specific requirements are:

- Excel sync status reporting
- Windows desktop/Excel availability reporting
- backup notification status
- reconciliation failure events with sanitized context and correlation IDs

These should be expressed as typed diagnostics and operational events, not arbitrary remote commands or copied business data.

## Gaps

- No HMAC authentication or explicit token revocation endpoint.
- No durable runner registry beyond the D1/in-memory control-plane stores.
- No production Cloud Runner or Windows Runner verification.
- `DATABASE_DIAGNOSTIC` is not yet implemented.
- Heartbeat transitions depend on a scheduler calling `refreshRunnerStates()`; there is no separately verified Cloudflare alarm/cron trigger in this audit.
- Repository Git identity is unavailable in this tree because `.git` metadata is absent.
