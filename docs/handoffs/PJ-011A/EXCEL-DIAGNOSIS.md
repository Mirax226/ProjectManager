# PJ-011A Excel Diagnosis

- Mirax.xlsm and Gozareshkar.xlsm remain macro-enabled; no SaveAs, VBA edit, authoritative write, or XLSM-to-XLSX conversion occurred.
- Existing verified boundary remains: path resolution, source existence, disposable copy, and source hash preservation. The failure boundary is Excel COM/workbook openability.
- The runner now emits `ASSET_RESOLVE`, `SOURCE_EXISTS`, `SOURCE_HASH_BEFORE`, `COPY_CREATE`, `COM_CREATE`, `EXCEL_CONFIGURE`, `WORKBOOK_OPEN_START`, `WORKBOOK_OPEN_SUCCESS`, `WORKBOOK_READ_PROBE`, `WORKBOOK_CLOSE`, `EXCEL_QUIT`, `COPY_DELETE`, and `SOURCE_HASH_AFTER` stage evidence.
- A real Windows probe is required to name the failing sub-stage. Until that run, the implementation deliberately does not call COM creation or `Workbooks.Open` the root cause.
- Safe classifications include `COM_CREATE_FAILED`, `EXCEL_CONFIGURATION_FAILED`, `WORKBOOK_OPEN_FAILED`, `WORKBOOK_READ_FAILED`, `WORKBOOK_CLOSE_FAILED`, `EXCEL_QUIT_FAILED`, `COPY_CLEANUP_FAILED`, and `TIMEOUT`.
