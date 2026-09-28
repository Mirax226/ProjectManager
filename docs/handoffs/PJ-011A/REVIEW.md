# PJ-011A Review

## Scope

Repository-local Finalizer workflow and stage-aware Excel COM diagnostics. Production Excel sync remains disabled.

## Baseline

- Branch: `main`
- Starting HEAD and `origin/main`: `c0a9498028c495b36c7c5ea3ed9857947a949f02`
- Working tree: clean before changes; repository write probe succeeded.

## Changes

- `tools/finalize-task.ps1` is the owner-executed publication boundary. It reads repository-local plan/changelog/handoff sources, rejects unsafe evidence, uses temporary local staging, validates ZIP contents, avoids Review overwrites, and supports `-DryRun`.
- Excel health probes now carry stage, safe error class/HRESULT/message, duration, disposable-copy use, and source-hash-before/after evidence while preserving existing event compatibility.

## Verification

- Finalizer DryRun: passed with repository-local fake destination arguments; no external writes.
- Tests and exact counts are recorded in `TEST-EVIDENCE.md`.
- Excel stage details are recorded in `EXCEL-DIAGNOSIS.md`.

## Owner usage

`powershell -ExecutionPolicy Bypass -File .\tools\finalize-task.ps1 -Project PJ -Task PJ-011A`

Codex did not execute the real external publication workflow.

## Deferred

PJ-011B should investigate the stage identified by actual Windows COM evidence. No root cause is asserted by this task without that evidence.
