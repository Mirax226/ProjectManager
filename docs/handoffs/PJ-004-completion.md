# PJ-004 Runner and Integration Foundation Completion

## Architecture Decisions

- Keep the Windows Runner as the only local process/filesystem execution boundary.
- Keep the Cloud Runner as the HTTPS Worker/D1 control-plane boundary for state, leases, events, and dispatch.
- Keep project identity in authenticated credentials and enforce it server-side for every runner operation.
- Use ordinary HTTPS for the integration contract; do not introduce a Cloudflare Service Binding dependency.
- Preserve typed jobs and explicit risk classification. Do not add a general command transport.
- Treat HMAC authentication, revocation registry, and `DATABASE_DIAGNOSTIC` as future hardening work.

## Current Capabilities

- Bearer-authenticated runner client with project-bound identity.
- Initial and periodic heartbeat with `ONLINE`, `STALE`, and `OFFLINE` states.
- Offline and recovery operational events.
- Typed job creation, idempotency, leasing, claim ownership, result submission, and duplicate-result handling.
- Project and runner scope enforcement in the Control Plane handler.
- Absolute repository path and configured-root checks.
- `shell:false`, fixed PowerShell diagnostics, bounded output, redaction, and Codex edit policy.
- In-memory local store and D1-backed control-plane store with separate legacy PostgreSQL ownership.

## Audit Evidence

- Runner implementation: `src/runner/client.js`, `src/runner/index.js`, `src/runner/jobExecutor.js`.
- Authentication and API boundary: `src/controlPlane/auth.js`, `src/controlPlane/handler.js`.
- Lifecycle and heartbeat state: `src/controlPlane/store.js`, `src/controlPlane/d1Store.js`.
- Durable schema: `migrations/20260924_control_plane.sql`.
- Existing tests: `test/controlPlane.test.js`, `test/runner.test.js`.

## Tests

- `npm test`: PASS, 119 total, 119 passed, 0 failed.
- Node test duration: 31,025.650 ms; shell-measured elapsed time approximately 31.55 seconds.
- `npm run check`: PASS.

## Remaining Gaps

- HMAC compatibility is only a documented design target; no signing/nonce implementation exists.
- Token revocation is operational/configuration-based today; there is no revocation API or durable denylist.
- Cloud Runner and Windows Runner production connectivity were not enabled or verified.
- `DATABASE_DIAGNOSTIC` remains a future typed job.
- The scheduler/trigger for periodic offline evaluation in a deployed Worker remains to be verified.
- DailySystem Excel, desktop, backup, and reconciliation integrations remain unimplemented by design.
- The current tree has no `.git` metadata, so branch, commit, and remote identity cannot be verified.

## Safety State

No production system was connected, deployed, mutated, or enabled. No secrets were rotated, and no destructive job was executed.

## Next Steps

PJ-005 — Integration Contract and Revocation Hardening

- define the project credential registry and revocation lifecycle
- add optional HMAC request authentication without breaking bearer clients
- specify and test `DATABASE_DIAGNOSTIC` as a read-only typed job
- verify heartbeat scheduling and recovery evidence in a non-production environment
