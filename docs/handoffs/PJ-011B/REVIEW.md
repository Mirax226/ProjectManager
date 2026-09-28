# PJ-011B Review

## Result

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
