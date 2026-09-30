# PJ-011B Changelog

- Ran the stage-aware disposable-copy Excel probe against the real Windows Excel host for Mirax.xlsm and Gozareshkar.xlsm.
- Both workbooks reached COM creation, configuration, open, read probe, close, and quit successfully.
- Added propagation of the PowerShell probe's internal lifecycle stage list into the runner result.
- No workbook/VBA or production sync changes were made. No external Plans or Desktop Review writes were performed.

## Desktop resume — 2026-09-30

- Recovered all five interrupted changes from baseline 6d4169e; corrected incomplete parser/repair/classification/retry and alert integration.
- Added stable incident fingerprints, acknowledgment-aware dedupe, explicit recovery resolution, and bounded boot retry policy.
- Added safe runtime diagnostics and provenance evidence: Render intended; ACTIVE_NODE_HOST_UNKNOWN; Worker separate from Node Telegram.
- Added independent owned-process cleanup and durable Excel stage capture, controlled disposable refresh/query/calculation suppression, VBA preservation and explicit quit-timeout recovery.
- Retained failure evidence honestly: Mirax unresolved, sandbox logon-context failure, and unknown-ownership automation process cleanup exception.
- Tightened CODEX_TASK project, mode/sandbox, environment, output capture and redaction.
- Isolated test checkout roots per worker; no global EPERM suppression.
- Updated repository plan/memory/handoffs. External publication remains owner-only Finalizer; Codex DryRun only.

Final verification: Gozareshkar also timed out at WORKBOOK_READ_PROBE after successful open; its owned process was cleaned up and source unchanged. Host instability remains unresolved. Release suite: 180/180 passed; syntax/check and diff checks passed.

Resume final review: extended unknown-incident recovery/no-silence-resolution handling; existing test extended, total 180. Focused 26/26 and full release 180/180 passed. No additional real Excel host probes.
