# PJ-011B Review

## Historical result (prior checkpoint; superseded by desktop resume below)

The real Windows probe succeeded for both authoritative macro-enabled workbooks using disposable copies. There is no observed first failing stage, HRESULT, or typed failure in this run. The prior `EXCEL_OPEN_FAILED` is therefore not confirmed as a workbook or COM root cause.

## Evidence

- Mirax: `OPENABLE`, Excel `16.0`, source unchanged, probe-owned process cleaned up.
- Gozareshkar: `OPENABLE`, Excel `16.0`, source unchanged, probe-owned process cleaned up.
- One Excel process existed before the probe and remained afterward; it was not created by the probe.
- Detailed stage and hash evidence: `EXCEL-DIAGNOSIS.md`.

## Remediation

The smallest evidence-backed code change was to return and preserve the internal PowerShell lifecycle stage list. No global timeout change, workbook edit, Trust Center change, COM registration change, or sync enablement is justified by successful runs.

## Runner

The typed health path was exercised end-to-end with Gozareshkar: Control Plane create/claim, runner `windows-01` execution with `requireExcelDesktop`, `OPENABLE` stage-aware result, and `SUCCEEDED` recorded job status. `EXCEL_SYNC` remains disabled.

## Verification

- `npm test`: **156 passed, 0 failed** using an isolated temporary directory.
- `npm run check`: passed.
- `git diff --check`: passed.
- Finalizer `-DryRun -Project PJ -Task PJ-011B`: passed; no external writes.

## Deferred

If `EXCEL_OPEN_FAILED` recurs, capture the first failed stage and safe HRESULT from this version before classifying it as code, environment, configuration, permissions, registration, openability, or timeout.

## PJ-011B desktop resume report — 2026-09-30

Repository hardening is implemented; **live incident closure remains incomplete**. No deployment, production database/configuration mutation, authoritative workbook write, XLSM conversion, VBA edit, external Plans/Review publication, reset, clean, stash, or history rewrite occurred.

| # | Requested item | Evidence/result |
|---|---|---|
| 1 | Starting HEAD | 6d4169e82e889f90f5390c9eb29bff307d01e902; main == origin/main. |
| 2 | Initial working tree (historical) | Expected dirty state, five tracked modifications, no untracked files; diff --check passed. At the time, commands used the then-current cloned/ProjectManager checkout. This is historical evidence, not the current canonical path. |
| 3 | Interrupted files | bot.js, configDb.js, configDbErrors.js, test/configDbSafety.test.js, test/logsHubStore.test.js. |
| 4 | Retained work | All five files' intended work retained. Parser tests/source precedence and incident-store repeat test were valid but needed broader testing. Typed preflight and bot classification integration were partial; existing prior Excel/PJ-011A/Finalizer checkpoints preserved. |
| 5 | Corrected/discarded | No unrelated work discarded. Corrected incomplete host validation, missed typed catches, one-time repair reuse, false query inference, legacy tenant/DNS confusion, missing actual routing/recovery dedupe coverage. Broader masking intentionally removes password suffix/query disclosure. |
| 6 | Active Config DB ENV | UNVERIFIED in deployed runtime. Code selects DATABASE_URL_PM, then PATH_APPLIER_CONFIG_DSN. Neither configured locally. |
| 7 | Safe parsed hostname | Deployed hostname UNKNOWN; no production DSN accessed. |
| 8 | Meaning of reported identifier | postgres.bqvyprmlqcbrwepnrwoc is the tenant-qualified user in the corrected provider message, not hostname evidence. Parser/encoding tests preserve username-host separation. |
| 9 | Config DB root cause | Provider cannot match host/user to a tenant/project. Exact deployed mismatched field/root configuration is NOT CONFIRMED. See CONFIG-DB-DIAGNOSIS.md and Supabase source below. |
| 10 | Code vs external configuration | Code misclassification/repair/routing gaps fixed. External tenant-routing/authentication failure evidenced; actual service/selected ENV/host and config correction unverified. |
| 11 | Error model | Typed DSN_MISSING, DSN_INVALID, HOST_INVALID, DNS_FAILED, AUTH_FAILED, CONNECT_TIMEOUT, TLS_FAILED, QUERY_FAILED with CONFIG_DB_ prefix; preserve UNKNOWN_DB_ERROR for unknown evidence and legacy classification compatibility. |
| 12 | Retry | Structural/auth/TLS/known query failures halt boot retry. DNS/connect-timeout/unknown: exponential 500ms..60s + <=250ms jitter, default cap20, constrained cap1..100. Missing DSN also halts. |
| 13 | Telegram dedupe | Stable project + selected ENV + class/host/root fingerprint updates same active/acknowledged incident count. Repeats do not route again. Distinct roots/sources/classes remain separate. Successful warmup resolves matching incidents; recovery notification uses one outage transition. Tested at actual appendEvent/shouldRouteEvent boundary without sending Telegram messages. |
| 14 | Owner config action | Review required; secret change NOT justified until exact mismatch is confirmed. No change made. |
| 15 | Mirax first failing stage | Sandbox COM_CREATE (0x80070520); initial/repeated host WORKBOOK_OPEN_START (TIMEOUT). Latest host experiment COM_CREATE timeout before calculation could be evaluated. |
| 16 | Gozareshkar stage | Initial host passed all stages. Intermediate run passed with recovered EXCEL_QUIT timeout. Final verification opened successfully but timed out at WORKBOOK_READ_PROBE (68,905ms); owned PID16488 cleaned up and source unchanged. |
| 17 | Safe error evidence | COMException 0x80070520 in sandbox. Timeouts have no HRESULT; TimeoutError for recovered quit. Per-run durations/stages/hashes are in safe JSON evidence. |
| 18 | Excel root cause | Sandbox ENVIRONMENT/logon context. Host TIMEOUT; underlying Mirax cause UNKNOWN. Connection/query refresh is a candidate, not a proven sole cause because disabling it did not consistently resolve open. |
| 19 | Remediation | Atomically saved stages, independent HWND/PID/creation-time cleanup, no duplicate Quit call, five-second quit grace with strict recovery requirements; disposable refresh/query/calculation suppression for safe diagnosis. No timeout increase/Trust Center/COM registration/source setting change. |
| 20 | XLSM/VBA/hash safety | Both source whole-file hashes unchanged in every probe; same .xlsm extension. Copy-only ZIP metadata edits preserve VBA payload hash and other entries. No source values/VBA/rich-data changed or business contents persisted. |
| 21 | Process cleanup | All HWND-verified probe processes exited. Existing PID2868 untouched. New automation PID31620 appeared before HWND return, ownership unverified; left untouched. Overall no-orphan verification INCOMPLETE. |
| 22 | Runner E2E | Real Windows Excel through local in-memory Control Plane -> scoped authentication -> typed creation/claim/lease -> runOnce -> typed result/status. Mirax FAILED; Gozareshkar has SUCCEEDED and FAILED observations. Final verification FAILED at metadata read after successful open. Live Cloudflare E2E not verified; stale/lease/idempotency/retry contract tests retained. |
| 23 | CODEX_TASK/security | Explicit project binding, allowed cwd contract, strict mode and read-only/workspace-write sandbox args, edit disabled by default, runtime environment allowlist, capped capture, DSN/generic output redaction. Argument arrays/shell:false retained. Real CLI task execution and symlink containment not newly certified. |
| 24 | Files changed | Complete intended file manifest below. No secret, workbook, DB, log, ZIP, or temporary file staged. |
| 25 | Added tests | 24 additional tests versus baseline156: recovered parser4 + store1, pool boundary3, backoff1, typed/config/incident/security10, Excel lifecycle/package preservation5. Existing mask test updated. |
| 26 | Final test result | 180 passed, 0 failed, 0 skipped (31.78s; resume release rerun). Isolated TEMP/TMP plus per-worker WORKDIR; initial shared-root cleanup failures were fixed rather than suppressed. |
| 27 | npm run check | Passed. |
| 28 | git diff --check | Passed; intended text edits normalized to UTF-8/LF. |
| 29 | Current plan | docs/project-plan/PJ_CURRENT_PLAN.md updated around actual baseline, runtime split, current failures and next milestone. |
| 30 | Memory | Existing MASTER-CONTEXT, CURRENT-STATE, ROADMAP updated; historical success retained and distinguished from today's failures. No duplicate memory system. |
| 31 | Handoffs | This REVIEW, CONFIG-DB-DIAGNOSIS, appended EXCEL-DIAGNOSIS, first/repeat/refresh-isolation/latest safe JSON evidence under docs/handoffs/PJ-011B. |
| 32 | Finalizer | Passed; real publication remains owner-only and append-only. |
| 33 | Commit | Engineering checkpoint containing this report; exact hash supplied in final chat after commit. |
| 34 | Push | Quality-gated normal origin/main push authorized; final result supplied in chat after verification. No force push. |
| 35 | Final Git status | Supplied after push; requirement clean and HEAD == origin/main. |
| 36 | Blockers | ACTIVE_NODE_HOST_UNKNOWN; deployed variable presence/hostname unverified; Mirax and latest Gozareshkar underlying timeout causes unresolved; COM-created process ownership/no-orphan verification incomplete. |
| 37 | Next milestone | PJ-012 runtime provenance and read-only deployed-config verification plus interactive-host COM/Mirax/lifecycle closure. |
| 38 | Owner decisions | Actual production secret correction or deployment is separately gated; owner runs real Finalizer manually. No paid-hosting/architecture/destructive decision was taken. |

### Runtime clarification

Configured Worker: projectmanager-control-plane. Actual deployment is unverified. It is a separate Fetch/D1 Control Plane, with no Telegram polling or Config DB warmup code. README and ff15588 identify **Render as intended Node hosting**, not proof of the active service. Exact warmup text originates in root bot.js; the checked-in Worker cannot generate/send this warning itself. Repeated warnings are consistent with that legacy Node path or an equivalent deployed older version, but do not identify the provider, revision, secret names, or hostname. No live Cloudflare secret names were read because Wrangler is unavailable and dashboard requires sign-in.

The corrected tenant/user error matches [Supabase's official troubleshooting article](https://supabase.com/docs/guides/troubleshooting/tenant-or-user-not-found). That establishes a shared-pooler host/user lookup failure; this review does not claim the specific wrong deployed field or suggest a guessed host. The safe next step is to locate the Node service, inspect configuration NAMES, and run tools/diagnose-config-db.js there for safe parsed fields before any owner-approved change.

### Intended changed-file manifest

- `bot.js`
- `configDb.js`
- `configDbErrors.js`
- `docs/handoffs/PJ-011B/CONFIG-DB-DIAGNOSIS.md`
- `docs/handoffs/PJ-011B/EXCEL-DIAGNOSIS.md`
- `docs/handoffs/PJ-011B/EXCEL-HOST-EVIDENCE.json`
- `docs/handoffs/PJ-011B/EXCEL-HOST-FIRST-RUN.json`
- `docs/handoffs/PJ-011B/EXCEL-HOST-REFRESH-ISOLATION.json`
- `docs/handoffs/PJ-011B/EXCEL-HOST-REPEAT-RUN.json`
- `docs/handoffs/PJ-011B/REVIEW.md`
- `docs/project-memory/CURRENT-STATE.md`
- `docs/project-memory/PROJECTMANAGER-MASTER-CONTEXT.md`
- `docs/project-memory/ROADMAP.md`
- `docs/project-plan/PJ_CURRENT_PLAN.md`
- `docs/project-plan/versions/PJ-011B/CHANGELOG.md`
- `opsReliability.js`
- `src/logsHubStore.js`
- `src/runner/excelOperations.js`
- `src/runner/jobExecutor.js`
- `test/configDbPool.test.js`
- `test/configDbSafety.test.js`
- `test/configDbWarmupRetry.test.js`
- `test/excelProbeLifecycle.test.js`
- `test/logsHubStore.test.js`
- `test/opsReliability.test.js`
- `test/pj011b.test.js`
- `test/setup-env.js`
- `tools/diagnose-config-db.js`
- `tools/diagnose-excel-host.js`
- `tools/excel-desktop-probe.ps1`

### Resume from usage-limit interruption

Resumed HEAD: 6d4169e82e889f90f5390c9eb29bff307d01e902. Recovered 19 modified tracked files and 11 intended untracked files, matching the manifest above. Classification: code, tests, tools and evidence were complete; final release validation and Git finalization remained; no unexpected files or incomplete diagnosis work requiring new host probes. Existing runtime/Excel evidence was reused. No Excel host probes were repeated during this resume.

Final review corrected one narrow gap: UNKNOWN_DB_ERROR Config DB incidents now remain active until successful warmup and resolve on recovery, consistent with other Config DB incidents. Extended the existing incident-boundary test; total unchanged. Focused validation 26/26 passed. Required release rerun: npm test 180 passed, 0 failed, 0 skipped, 31.78s; npm run check and additional changed-entrypoint syntax checks passed; git diff --check passed. Finalizer DryRun rerun passed with no external writes. Normal commit/push results and exact final hash are supplied in the final response.
