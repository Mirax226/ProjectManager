# PJ-011B Excel Diagnosis

## Mirax.xlsm

- Path: `C:\Users\Amir\Documents\GitHub\ExcelMirror\Mirax.xlsm`
- Result: `OPENABLE`; Excel `16.0`; no failed stage; no HRESULT/error class.
- Source hash before/after: `72302572d4965a5d0ba2571d1fdcadfe6b197873cbc9adeeb0985237398b4d77` / same; `sourceUnchanged=true`.
- Disposable copy: created and deleted; probe-owned Excel PID `47172` exited. Probe duration was approximately 15.5 seconds on the captured run.
- Stages: `ASSET_RESOLVE`, `SOURCE_EXISTS`, `SOURCE_HASH_BEFORE`, `COPY_CREATE`, `COM_CREATE`, `EXCEL_CONFIGURE`, `WORKBOOK_OPEN_START`, `WORKBOOK_OPEN_SUCCESS`, `WORKBOOK_READ_PROBE`, `WORKBOOK_CLOSE`, `EXCEL_QUIT`, `COPY_DELETE`, `SOURCE_HASH_AFTER`.

## Gozareshkar.xlsm

- Path: `C:\Users\Amir\Documents\GitHub\ExcelMirror\Gozareshkar.xlsm`
- Result: `OPENABLE`; Excel `16.0`; no failed stage; no HRESULT/error class.
- Source hash before/after: `2fd360e08b92c37c42a25c185b340e72cfe2dc43d6724d47144d83a6853cbe1d` / same; `sourceUnchanged=true`.
- Disposable copy: created and deleted; probe-owned Excel PID `41940` exited. Probe duration was approximately 3.8 seconds on the captured run.
- Stages: `ASSET_RESOLVE`, `SOURCE_EXISTS`, `SOURCE_HASH_BEFORE`, `COPY_CREATE`, `COM_CREATE`, `EXCEL_CONFIGURE`, `WORKBOOK_OPEN_START`, `WORKBOOK_OPEN_SUCCESS`, `WORKBOOK_READ_PROBE`, `WORKBOOK_CLOSE`, `EXCEL_QUIT`, `COPY_DELETE`, `SOURCE_HASH_AFTER`.

## Classification

Confirmed root-cause class: `UNKNOWN` for the historical failure because it did not reproduce. Current host evidence confirms neither `COM_CREATE_FAILED` nor `WORKBOOK_OPEN_FAILED`.

## Safety

Both sources remained `.xlsm`; no SaveAs, XLSX conversion, macro stripping, VBA read/write, authoritative write, or publication occurred.

## Desktop resume — 2026-09-30

Prior OPENABLE evidence above is retained as historical PJ-011B evidence; it is not substituted for today's results.

- Sandbox: both assets failed at COM_CREATE, COMException 0x80070520. Microsoft maps low Win32 code 0x520 to ERROR_NO_SUCH_LOGON_SESSION. Host-context COM creation subsequently succeeded, confirming an environment/context boundary rather than missing Excel registration: https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--1300-1699-
- Initial real host: Mirax TIMEOUT at WORKBOOK_OPEN_START, ~64.1s; Gozareshkar OPENABLE, ~26.9s. First-run evidence is EXCEL-HOST-FIRST-RUN.json. Source hashes unchanged and both HWND-identified processes exited; PID 2868 was preserved.
- Isolated Mirax repeat without refresh suppression: TIMEOUT at WORKBOOK_OPEN_START, ~73.4s; verified process cleanup. EXCEL-HOST-REPEAT-RUN.json.
- Safe package metadata: Mirax has connection/query-table refresh-on-load flags and synchronous query metadata; Gozareshkar has none. Neither source has fileSharing read-only recommendation or Zone.Identifier. Both contain formulas; no cell values, formulas, connection strings, VBA source, or business content were printed or persisted.
- Controlled refresh-flag suppression on COPY: Mirax opened, read, and closed once, then stalled at EXCEL_QUIT; Gozareshkar passed normally. EXCEL-HOST-REFRESH-ISOLATION.json. Subsequent flag-only repeats timed out again at WORKBOOK_OPEN_START. Therefore refresh is a candidate contributor, not a proven sole root cause.
- Controlled query-disable extension on COPY: Mirax still timed out at WORKBOOK_OPEN_START (~65.3s), source unchanged, VBA payload unchanged, owned PID 47196 exited. disableRefresh semantics: https://learn.microsoft.com/en-us/dotnet/api/documentformat.openxml.spreadsheet.querytable
- Last calculation-control experiment: Mirax timed out at COM_CREATE before manual calculation could be evaluated. Gozareshkar opened/read/closed with Excel 16.0; EXCEL_QUIT stalled and was recovered by independent verified cleanup. EXCEL-HOST-EVIDENCE.json. The state/lease result was Mirax FAILED and Gozareshkar SUCCEEDED, with explicit EXCEL_QUIT_TIMEOUT_RECOVERED warning for Gozareshkar.

Current root-cause classification: sandbox ENVIRONMENT (logon-session failure); host TIMEOUT, underlying Mirax cause UNKNOWN. No Trust Center, COM registration, timeout expansion, authoritative connection setting, or source workbook modification is justified by this evidence. Manual calculation and refresh suppression are controlled diagnostic policies, not proof of incident closure.

### Implemented diagnostic safety/lifecycle changes

PowerShell helper persists stages atomically outside the COM worker. Query refresh flags/disableRefresh are modified only under an operation-created pj-excel-probe-* directory. The VBA ZIP payload is hash-checked before/after. Other entries are preserved; unit tests check unaffected workbook and VBA bytes. A separate unsaved blank workbook establishes manual calculation in the new instance before target open; no authoritative workbook is written or saved. All source hashes remain the historical hashes above, guaranteeing unchanged VBA/rich-data/source bytes.

Known-owned PID is obtained from Excel HWND and recorded with process creation ticks. Existing processes are refused before configuration and never quit. Parent timeout cleanup uses a separate process because a killed COM worker cannot execute finally. Quit gets a five-second grace; recovery requires recorded successful open/read/close stages plus verified owned-process cleanup. Earlier stage timeouts stay failed. Quit is requested only once, avoiding a second call to a disconnected COM server. Diagnostic output is bounded and business-error text is replaced with a safe stage message/HRESULT/class.

### Cleanup exception — unresolved

A COM_CREATE timeout spawned an observed new automation PID 31620 before returning a HWND. Read-only host metadata shows DCOM broker parent svchost PID 1680 and creation 2026-09-30T15:48:07.8061160Z. Ownership cannot be conclusively tied to the probe using the approved HWND/creation-time contract. The process was left untouched rather than terminating a potentially unrelated instance. PID 2868 (pre-existing) was preserved. All HWND-verified owned processes exited, but overall no-orphan verification is **NOT COMPLETE**. Current evidence must not interpret an empty owned-ID list as full cleanup success.

Next investigation needs an interactive host with reliable COM activation and process attribution/dialog visibility. No authoritative EXCEL_SYNC is enabled. The current host-context diagnostic is a cached-read, refresh/recalculation-disabled check; it does not certify production refresh behavior.

### Final lifecycle verification

Final Gozareshkar-only host run: WORKBOOK_OPEN_SUCCESS then TIMEOUT at WORKBOOK_READ_PROBE, 68,905ms. Owned PID16488 was verified and cleaned up; preexisting PID2868 and unverified PID31620 remained untouched. Source SHA-256 before/after: 2fd360e08b92c37c42a25c185b340e72cfe2dc43d6724d47144d83a6853cbe1d. Copy refresh suppression/manual calculation and unchanged VBA recorded. This supersedes any implication of consistently successful Gozareshkar automation and suggests broader host/COM instability; its underlying cause remains unconfirmed. No further COM probes were run.
