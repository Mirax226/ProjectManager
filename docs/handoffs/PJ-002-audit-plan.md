# PJ-002 Operational Control Plane Hardening Audit Plan

## Scope

Audit ProjectManager as a reusable private operational control plane for multiple projects, preserving existing working systems and applying only incremental hardening.

## Inspection Baseline

- Source boundaries: `src/controlPlane`, `src/runner`, legacy root Node stores, `bot.js`, Telegram routes, and `src/web`.
- Database boundaries: PostgreSQL/config DB migrations (`20250101_env_vault.sql`, `20250115_add_log_alerts.sql`) and the isolated control-plane D1 migration (`20260924_control_plane.sql`).
- API surface: authenticated Fetch-compatible Control Plane routes in `src/controlPlane/handler.js` plus the legacy Telegram/admin and web routes.
- Validation commands: `npm test` and `npm run check`.
- Git baseline: unavailable in this audit tree because it has no `.git` metadata.

## Audit Checks

### Operational Event Contract

- Confirm the complete envelope: `schemaVersion`, `eventId`, `project`, `environment`, `severity`, `category`, `timestamp`, `message`, `context`, `source`, and `correlationId`.
- Verify event ID idempotency and safe retries.
- Verify correlation propagation and sensitive context sanitization.
- Verify D1 persistence matches the in-memory contract.

### Multi-Project Boundary

- Confirm project identities are normalized and stored independently.
- Confirm project credentials and event/job reads are scoped.
- Confirm events cannot be written for another project through a project token.
- Confirm runner credentials bind heartbeat, claims, results, and status reads to one project.

### Telegram Operations

- Inventory incident/log, health, timeline, project, and runner visibility.
- Confirm operational messaging masks secrets and carries correlation data where available.
- Record privileged legacy admin workflows separately from typed Control Plane jobs.

### Runner Review

- Verify allowlisted typed jobs and argument-array execution.
- Verify repository path boundaries, read-only defaults, and edit restrictions.
- Verify heartbeat, stale/offline transitions, and recovery events.
- Document Windows Runner and Cloud Runner responsibilities without enabling production integrations.

## Deliverables

- This audit plan.
- Focused contract and project-boundary hardening where evidence identifies a gap.
- `docs/handoffs/PJ-002-completion.md` with findings, changes, tests, decisions, and remaining gaps.
