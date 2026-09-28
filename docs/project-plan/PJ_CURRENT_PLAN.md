# ProjectManager Current Plan

- Canonical repository: `C:\Users\Amir\Documents\GitHub\cloned\ProjectManager`
- Current commit: `645362a` plus PJ-011B pending commit (real-host Excel probe evidence)
- Architecture: Control Plane plus a project-scoped Windows Runner; Excel operations remain typed, disposable-copy, non-production diagnostics.
- Completed recovery: PJ-001 through PJ-010 recovery and operational foundations are preserved in `docs/handoffs/` and `docs/project-memory/`.
- Current Excel diagnosis: real Windows probes for Mirax.xlsm and Gozareshkar.xlsm reached COM creation, configuration, workbook open, read probe, close, and quit successfully on disposable copies; both source hashes remained unchanged. The historical EXCEL_OPEN_FAILED remains unreproduced.
- Finalizer architecture: `tools/finalize-task.ps1` validates repository-local plan, changelog, and handoff evidence, stages locally, validates a ZIP, and publishes only when the owner explicitly runs it.
- Review/Plans workflow: repository-local sources are authoritative; external Plans are mirrors and Desktop Review is append-only output. Codex uses `-DryRun` or fake destinations only.
- Runner status: local Windows Runner boundary implemented; authoritative Excel sync/publication remains disabled.
- DS adapter status: typed, sanitized, non-production adapter evidence only.
- Telegram Ops status: operational status presentation and event contracts exist; no production enablement in this milestone.
- Open security risks: owner review of external publication paths, Windows COM behavior, and production approval controls remain open; no secrets or workbooks belong in handoffs.
- Next milestone: PJ-011B should use the stage evidence to resolve the confirmed COM/openability sub-stage and add only evidence-backed remediation.
