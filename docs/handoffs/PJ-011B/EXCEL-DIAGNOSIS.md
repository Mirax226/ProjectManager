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
