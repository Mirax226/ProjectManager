# ProjectManager Master Context

## Purpose

ProjectManager is a private operational control plane for multiple external projects, including future DailySystem integration. It coordinates authentication, operational events, alerts, runners, typed job validation, provider metadata, and operator views. It does not own the business logic or business data of external projects.

## Recovery Baseline

- Checkpoint: PJ-Checkpoint-003
- Coverage: PJ-001 through PJ-009 handoffs and current source contracts
- Date: 2026-09-25
- Git identity: unavailable in this tree because `.git` metadata is absent
- Production state: unchanged; no deployment, production mutation, integration enablement, secret change, commit, or push performed

## Current System Shape

The legacy Node runtime owns the existing Telegram bot, PostgreSQL/config database integrations, GitHub and deployment workflows, logs, Safe Mode, Ops Timeline, and web dashboard. The Fetch-compatible Control Plane under `src/controlPlane` owns project-scoped operational events, alerts, runner state, jobs, leases, status reads, and PJ-009 durable operational state in additive D1 tables. The Windows Runner is the only approved local execution boundary for typed Excel health, snapshot, backup, and reconciliation operations; authoritative Excel publication remains disabled.

## Source Of Truth

The PJ handoffs document decisions and audit evidence. Source contracts and tests are authoritative for behavior. This memory set is a recovery index, not a replacement for implementation tests or the handoff history in `docs/handoffs/`.

## Immediate Operating Rule

Continue with fixture-based, non-production hardening. Keep typed jobs validation-only until execution policy, adapters, approval, timeout, artifact, and rollback contracts are separately reviewed.

## Project Memory Location

- Canonical memory path: `docs/project-memory/` in the canonical Git repository (`C:\Users\Amir\Documents\GitHub\cloned\ProjectManager`).
- Purpose: recovery index for current architecture, verified state, decisions, milestones, and safe operating boundaries.
- Update protocol: update `CURRENT-STATE.md`, `RECOVERY-CHECKPOINT.md`, or `ROADMAP.md` when verified state changes; add immutable phase evidence under `docs/handoffs/`; keep source contracts and tests authoritative for behavior.
- Plans/PJ relationship: `C:\Users\Amir\Documents\GitHub\Plans\PJ\` is the owner-facing current-plan and transition log mirror. It summarizes the canonical repository state and does not replace repository memory or tests.
- Repository handoff relationship: `docs/handoffs/` contains historical phase/checkpoint evidence. Handoffs are preserved rather than rewritten; the project-memory files point to the current recovery position.

## Permanent Finalizer Workflow

- Codex writes generated plans, changelogs, evidence, and handoffs only inside the canonical repository.
- The owner explicitly executes `tools/finalize-task.ps1`; Codex does not publish directly to Desktop Review or external Plans.
- The Finalizer publishes external Plans and Review artifacts from repository-local plan sources after validation. External Plans are generated mirrors, not a second source of truth.
- Desktop Review is append-only for normal finalization; previous Review artifacts are never deleted and timestamp collisions are handled safely.
- Finalizer staging is created under the user's normal temporary directory and is deleted only after successful final artifact validation. Failed runs retain staging for diagnosis.

## PJ-011B Real Excel Probe

- The real Windows Excel host successfully opened disposable copies of Mirax.xlsm and Gozareshkar.xlsm through COM creation, configuration, `Workbooks.Open`, read probe, close, and quit.
- Source hashes were unchanged before/after both probes, and probe-owned Excel processes exited. A pre-existing Excel process was left untouched.
- The historical `EXCEL_OPEN_FAILED` did not reproduce; classify its root cause as unknown until a future recurrence supplies a first failing stage and safe HRESULT. Authoritative sync remains disabled.
