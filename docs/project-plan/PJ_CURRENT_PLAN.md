# ProjectManager Current Plan

- Canonical repository: `C:\Users\Amir\Documents\GitHub\cloned\ProjectManager`
- Current commit: `c0a9498028c495b36c7c5ea3ed9857947a949f02` (baseline for PJ-011A)
- Architecture: Control Plane plus a project-scoped Windows Runner; Excel operations remain typed, disposable-copy, non-production diagnostics.
- Completed recovery: PJ-001 through PJ-010 recovery and operational foundations are preserved in `docs/handoffs/` and `docs/project-memory/`.
- Current Excel diagnosis: source paths, existence, disposable copies, and source hashes are verified; COM/openability is the remaining boundary. PJ-011A records exact COM sub-stage evidence without claiming a root cause prematurely.
- Finalizer architecture: `tools/finalize-task.ps1` validates repository-local plan, changelog, and handoff evidence, stages locally, validates a ZIP, and publishes only when the owner explicitly runs it.
- Review/Plans workflow: repository-local sources are authoritative; external Plans are mirrors and Desktop Review is append-only output. Codex uses `-DryRun` or fake destinations only.
- Runner status: local Windows Runner boundary implemented; authoritative Excel sync/publication remains disabled.
- DS adapter status: typed, sanitized, non-production adapter evidence only.
- Telegram Ops status: operational status presentation and event contracts exist; no production enablement in this milestone.
- Open security risks: owner review of external publication paths, Windows COM behavior, and production approval controls remain open; no secrets or workbooks belong in handoffs.
- Next milestone: PJ-011B should use the stage evidence to resolve the confirmed COM/openability sub-stage and add only evidence-backed remediation.
