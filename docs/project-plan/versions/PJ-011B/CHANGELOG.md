# PJ-011B Changelog

- Ran the stage-aware disposable-copy Excel probe against the real Windows Excel host for Mirax.xlsm and Gozareshkar.xlsm.
- Both workbooks reached COM creation, configuration, open, read probe, close, and quit successfully.
- Added propagation of the PowerShell probe's internal lifecycle stage list into the runner result.
- No workbook/VBA or production sync changes were made. No external Plans or Desktop Review writes were performed.
