# PJ-002 Operational Control Plane Hardening Completion

## Baseline Commit

Unavailable. The current ProjectManager audit tree has no `.git` metadata, so a branch, commit hash, and remote cannot be verified. No alternate checkout was used as the audit baseline.

## Architecture Findings

- The legacy Node runtime remains responsible for PostgreSQL/config DB integration, Telegram admin UX, GitHub, deployments, logs, Safe Mode, Ops Timeline, and the existing dashboard.
- The Control Plane remains a Fetch-compatible service for project-scoped operational events, alerts, jobs, leases, runners, and status reads. Its D1 schema is isolated from the legacy PostgreSQL ownership boundary.
- The Windows Runner is the execution boundary for Git, npm, PowerShell/Excel, filesystem checks, and Codex diagnostics. The Cloud Runner role is the future Worker/D1-hosted control-plane side; this audit did not enable or verify a production Cloud Runner.
- ProjectManager stores operational facts and coordinates work; external project business logic remains outside its ownership.

## Contract Audit Results

- PASS: operational events now require and normalize `schemaVersion: 1`.
- PASS: event IDs remain idempotent for duplicate delivery; retries return the prior event.
- PASS: correlation IDs are retained from the envelope and promoted from `context.correlationId` when needed.
- PASS: event context is bounded and redacted for token, secret, password, authorization, API-key, cookie, DSN, and private-key fields.
- PASS: D1 event persistence now stores and restores `schema_version`.
- LIMITATION: transport retry/backoff is still the responsibility of callers; the Control Plane provides idempotent replay handling rather than a background retry queue.

## Multi-Project Boundary Results

- PASS: project keys are normalized and events/jobs reject unknown projects.
- PASS: project tokens cannot write events or jobs for another project.
- PASS: project-scoped reads filter projects, jobs, status, events, alerts, and runners.
- PASS: runner tokens must declare a project; heartbeat, job claims, result submission, and status reads are bound to that project.
- PASS: runner repository paths remain constrained by the configured project path/root.

## Telegram Review

- PASS: existing admin flows expose incident/log handling, health guidance, timeline access, and project navigation with test coverage for the relevant operational paths.
- PARTIAL: runner visibility is available through the Control Plane status surface and runner heartbeat model, but there is no dedicated Telegram runner dashboard verified in this audit.
- PASS: operational log/diagnostic paths use masking and correlation metadata where available.
- FINDING: the legacy admin bot intentionally contains privileged SQL, deploy, project deletion, and secret/token management workflows. These remain admin-gated existing features and are outside typed Control Plane jobs; they were not redesigned or enabled by PJ-002.

## Runner Review

- PASS: jobs are allowlisted by `JOB_TYPES` and classified by risk.
- PASS: command execution uses argument arrays with `shell: false`; arbitrary shell input is not accepted as a job type.
- PASS: repository path checks prevent configured project escapes.
- PASS: Codex edit mode is disabled unless explicitly enabled by environment policy.
- PASS: heartbeat stale/offline detection and one-time recovery event behavior are covered.
- Windows Runner role: local execution of approved diagnostics and typed jobs against the configured Windows project workspace.
- Cloud Runner role: future authenticated Worker/D1 control-plane endpoint for state, leases, and dispatch; production connectivity remains unverified.

## Files Changed

- `src/controlPlane/contracts.js`
- `src/controlPlane/handler.js`
- `src/controlPlane/store.js`
- `src/controlPlane/d1Store.js`
- `migrations/20260924_control_plane.sql`
- `test/controlPlane.test.js`
- `docs/DAILYSYSTEM-INTEGRATION.md`
- `docs/handoffs/PJ-002-audit-plan.md`
- `docs/handoffs/PJ-002-completion.md`

## Tests

- `npm test`: PASS, 116 total, 116 passed, 0 failed; Node test duration 30,680.526 ms; shell elapsed time approximately 31.19 seconds.
- `npm run check`: PASS (`node --check` for the Control Plane handler and runner executor).

## Decisions

- Make the final event envelope explicit and versioned without introducing a new event subsystem.
- Enforce project binding at the authenticated runner boundary instead of trusting request payload project IDs.
- Keep existing PostgreSQL and Telegram ownership boundaries intact.
- Keep production integrations, secrets, deployment, and Cloud Runner activation out of this audit.

## Remaining Gaps

- Git repository identity and baseline commit cannot be verified until `.git` metadata is available in this audit tree.
- Telegram production connection and dedicated runner dashboard verification remain open.
- Cloud Runner production verification remains open.
- A final review is still needed for transport retry/backoff policy and the complete operational-event contract across every external producer.
- Legacy privileged Telegram workflows remain an explicit admin surface and should be reviewed separately if the product requires a stricter control-plane-only interface.

## Next Phase

PJ-003 — Production Readiness Evidence and Integration Verification, subject to repository identity, environment, and approval prerequisites.
