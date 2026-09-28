# PJ-003 Telegram Operations Center Hardening

## Changes

- Added `src/telegramOpsCenter.js` with pure, testable models for incident lists, incident details, runner dashboards, message sanitization, and safe-action allowlisting.
- Added Incident Center access to the existing Ops menu with active, acknowledged, and recovered views.
- Added incident detail rendering with project, environment, severity, category, component, first seen, last seen, occurrence count, status, and sanitized message.
- Added safe incident actions: Details, read-only Health Check dispatch through the configured Control Plane, Timeline, Acknowledge, and Codex Task creation.
- Added an optional Runner Dashboard using the configured Control Plane status/jobs endpoints. Without configuration it stays read-only and reports that the dashboard is unavailable.
- Added focused tests in `test/telegramOpsCenter.test.js`.

## Safety Decisions

- No arbitrary shell action, database destruction, secret editing, unrestricted deployment, or delete action was added to the Operations Center.
- Health checks create only the existing typed `HEALTHCHECK` job and require the configured Control Plane endpoint/token; no integration was enabled by this change.
- Incident messages and Codex task bodies use sanitized, bounded text.
- Existing legacy admin workflows remain outside the new Operations Center and were not redesigned.

## Architecture Impact

- The existing control-plane, event, alert, runner, and Telegram boundaries remain intact.
- The bot is a read/triage client of existing local incident storage and optional Control Plane status/job APIs.
- Incident acknowledgement continues to use `src/logsHubStore.js` status transitions.
- Runner identity and execution policy remain owned by the Control Plane and Windows Runner; Telegram only requests typed read-only work.

## Tests

- `npm test`: PASS, 119 total, 119 passed, 0 failed.
- Node test duration: 31,955.013 ms; shell-measured elapsed time approximately 33.39 seconds.
- `npm run check`: PASS.
- `node --check bot.js`: PASS.
- `node --check src/telegramOpsCenter.js`: PASS.

## Remaining Risks

- Runner dashboard and Health Check dispatch require a configured Control Plane endpoint and admin token; production connectivity was not verified or enabled.
- Telegram production delivery remains unverified.
- The legacy admin bot still contains privileged database, deployment, deletion, and secret-management workflows outside this Operations Center. They remain protected by existing admin/role controls but should be reviewed separately if a stricter read-only Telegram surface is required.
- The current audit tree has no `.git` metadata, so a commit baseline and Git status cannot be independently verified.

## Next Phase

PJ-004 — Telegram Production Evidence and Control-Plane Integration Verification

Focus on environment-backed verification of runner status, health-check job dispatch, incident lifecycle updates, and Telegram delivery without enabling destructive actions.
