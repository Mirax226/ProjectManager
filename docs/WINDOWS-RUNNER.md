# Windows runner

Set `PG_CONTROL_PLANE_URL`, `PG_RUNNER_ID`, `PG_RUNNER_TOKEN`, `PG_PROJECT_ID=daily-system`, and `DAILYSYSTEM_REPO_PATH` in a local `.env` or service environment. Optional settings are `CODEX_BIN`, `CODEX_PROFILE`, `PG_ALLOW_CODEX_EDITS`, heartbeat/poll intervals, and `PG_RUNNER_HOST_LABEL`.

Start it with `npm.cmd run runner`. The runner sends authenticated heartbeats, claims one leased typed job at a time, executes only the allowlisted job types, and reports a truncated result. A temporary control-plane outage does not expose an inbound port; the process retries on its normal polling/heartbeat loop.

Initial job types are HEALTHCHECK, PROJECT_STATUS, GIT_STATUS, RUN_TESTS, RUN_TYPECHECK, DAILYSYSTEM_HEALTHCHECK, EXCEL_CONNECTIVITY_TEST, EXCEL_SYNC_DIAGNOSTIC, EXCEL_RECONCILIATION_CHECK, and CODEX_TASK. Push, deploy, arbitrary shell, secret mutation, and authoritative Excel writes are intentionally unavailable.
