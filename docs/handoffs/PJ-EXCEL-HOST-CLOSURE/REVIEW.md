# Excel Host Closure review — 2026-10-01

EXCEL_HOST_CLOSURE = OPEN. Diagnostic defect corrected; intermittent Excel lifecycle failure root cause remains unconfirmed. PJ-012 remains COMPLETE.

## Baseline and scope

Starting HEAD 3ca7657c0bdc92b9eda84ca58929181830906635; clean main and equal origin/main verified before changes. Prior test baseline: 213/213. Reviewed PJ-012 and PJ-011B reviews, PJ-011B EXCEL-DIAGNOSIS, current plan, current state, master context and roadmap; inspected diagnose-excel-host.js, excel-desktop-probe.ps1, excelOperations.js, jobExecutor.js and lifecycle/Runner tests. Historical evidence retained without rewriting PJ-011B artifacts.

Approved source paths exist:

- C:\Users\Amir\Documents\GitHub\ExcelMirror\Mirax.xlsm
- C:\Users\Amir\Documents\GitHub\ExcelMirror\Gozareshkar.xlsm

Each run used healthcheck's isolated disposable copy and a new owned Excel instance, serially G/M/G/M, with the same existing cached-read policy before and after. Existing copy-only refresh suppression, manual calculation and disabled macros/events/links remained in place. Source files were never opened for modification, saved or altered. Read probe enumerates worksheet metadata/count and Excel version; it does not read business cells or UsedRange. No concurrent probe or DailySystem heavy Excel test.

## Matrix before diagnostic fix

| Run | Asset | Result / first failure | Total ms | Copy config | COM | Configure | Open | Read | Close | Quit | Owned PID |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Gozareshkar.xlsm | PASS with EXCEL_QUIT_TIMEOUT_RECOVERED | 13525 | 824 | 2538 | 657 | 1621 | 329 | 156 | incomplete | 9356 |
| 2 | Mirax.xlsm | PASS | 43578 | 4123 | 1929 | 622 | 29036 | 672 | 661 | 3341 | 20272 |
| 3 | Gozareshkar.xlsm | PASS | 31508 | 8884 | 5129 | 3097 | 3249 | 1808 | 272 | 4916 | 28516 |
| 4 | Mirax.xlsm | TIMEOUT: WORKBOOK_CLOSE | 74409 | 11215 | 5892 | 910 | 33278 | 4490 | incomplete | not reached | 10608 |

## Matrix after diagnostic fix

| Run | Asset | Result / first failure | Total ms | Copy config | COM | Configure | Open | Read | Close | Quit | Owned PID |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Gozareshkar.xlsm | PASS | 27226 | 1789 | 7561 | 1366 | 2728 | 1064 | 386 | 4625 | 23012 |
| 2 | Mirax.xlsm | TIMEOUT: WORKBOOK_OPEN_START | 63643 | 11131 | 6678 | 1891 | incomplete | not reached | not reached | not reached | 32720 |
| 3 | Gozareshkar.xlsm | PASS | 21682 | 1611 | 5924 | 2048 | 2965 | 770 | 275 | 4589 | 12024 |
| 4 | Mirax.xlsm | PASS | 39919 | 5029 | 2376 | 468 | 23779 | 433 | 1336 | 4101 | 9212 |

Times are milliseconds. Total includes source/copy/host bootstrap/cleanup overhead. COM-stage timings are the worker's persisted measurements. Outer healthcheck stage timestamps for returned worker stages are replay timestamps, not individual COM durations. An incomplete stage has no fabricated completion duration; not reached is distinct from timeout. Worker budget remains 60 seconds, separate cleanup budget 15 seconds; existing five-second quit watchdog unchanged. Before/after are four observations each, not a performance benchmark.

First failures: before G1 EXCEL_QUIT (recovered only after verified owned cleanup), before M2 WORKBOOK_CLOSE (open/read succeeded), after M1 WORKBOOK_OPEN_START (no open success). Each timeout has no returned COM HRESULT: null, not an inferred HRESULT. Before G1 recorded safe TimeoutError; baseline M2 lacked safe error/duration metadata, an observed diagnostic gap now corrected. After M1 reports safe TimeoutError, probe duration 60094 ms, separate cleanup 2656 ms. Before M2 cannot distinguish target close from release or seed close/release because those calls shared one stage. New persisted substages provide that distinction if it recurs; no close failure occurred in the retest. No current COM_CREATE or read failure reproduced.

## Ownership, cleanup and source integrity

All eight runs had HWND -> PID -> exact process start ticks proof, recorded with session 1 in MATRIX-BEFORE.json / MATRIX-AFTER.json. Eight distinct PIDs: 9356, 20272, 28516, 10608; then 23012, 32720, 12024, 9212. All owned-process cleanup verified; remainingOwnedProcessIds and unverifiedNewExcelProcessIds empty in every run. All copies created and deleted; copy basename identities are recorded in JSON. No name-based or pre-existing Excel termination.

Pre-existing Excel PID 9396, session 1, HWND 985748, start UTC 2026-10-01T11:24:43.8635012Z remained the sole Excel process after both matrices. Final identity matches baseline; no probe orphan remains. Historical unverified PID 31620 was absent in this session, and was not terminated.

All eight before/after source SHA256 pairs match, also independently rechecked after the final matrix:

- Mirax: 72302572d4965a5d0ba2571d1fdcadfe6b197873cbc9adeeb0985237398b4d77
- Gozareshkar: 2fd360e08b92c37c42a25c185b340e72cfe2dc43d6724d47144d83a6853cbe1d

These also match prior handoff hashes. Copy VBA payload checks remained true. No authoritative VBA/formula/style/name/validation/connection change, no conversion, no global Office/Trust Center/COM registration change.

## Host versus workbook conclusion

Interactive Windows session 1, PowerShell 7.6.5; Windows PowerShell bootstrap/Add-Type passed. Every current run completed COM creation/configuration. Exact workbook lock files were absent before testing. One existing interactive Excel process was preserved; its mere presence does not establish contention. Unique copy/state directories avoid shared filename collision; host load/hidden dialogs/add-ins were not measured and are hypotheses only.

Current classification: MULTIPLE_HOST_FAILURE_MODES, WORKBOOK_OPEN_INSTABILITY, QUIT_CLEANUP_INSTABILITY, plus observed WORKBOOK_CLOSE instability. Underlying cause UNKNOWN. HOST_COM_INSTABILITY / READ_PROBE_INSTABILITY exist in historical evidence but were not reproduced here. No demonstrated HOST_SESSION_CONTEXT or PROCESS_OWNERSHIP_GAP in these runs.

Mirax took longer to open successfully and failed once on opening and once after opening; Gozareshkar had a quit recovery. Workbook-specific content causation is unconfirmed. Mirax copy suppression cleared two refresh/query settings, Gozareshkar zero; Mirax still both failed and passed under identical policy. Automatic refresh/query behavior is not a confirmed root cause. No additional feature suppression is justified. Failure remained intermittent and moved stages; successful retest M2 does not establish stability.

## Narrow diagnostic correction

- Preserve OPENABLE/workbookOpened once WORKBOOK_OPEN_SUCCESS is recorded, even if read/close/quit later fails; overall ok remains false for unrecovered lifecycle failures.
- Ensure healthcheck rejects explicit probe ok=false even when openability=OPENABLE, preventing false health success after the classification correction.
- Persist metadata/close/quit substages before potentially blocking calls, and return safe timeout class, nullable HRESULT, probe duration and separate cleanup duration.
- Two new regressions cover close-timeout classification/substage/duration and healthcheck late-failure rejection plus source/copy safety. Existing tests cover COM failure, read/open/quit timeout and ownership cleanup.

No lifecycle sequencing, feature policy, timeout expansion or ownership/termination policy change. This fixes diagnostic truthfulness, not the underlying host hang.

## Validation and remaining work

Focused Excel/Runner tests: 16 passed, 0 failed/skipped, 51.4859 seconds. Full npm test: 215 passed, 0 failed/cancelled/skipped, 32.3907851 seconds. npm run check PASS; git diff --check PASS. Test fixtures are isolated and do not depend on owner workbook contents. Full suite includes typed allowlist, project binding, claim/lease/attempt fencing, stale-result rejection and terminal idempotence regressions. windows-runner/daily-system boundary code unchanged; no arbitrary execution surface introduced. No live production job/deployment/secret change.

Remaining blocker: opening/closing/quitting Excel can still block under identical safe copy policy. Smallest next experiment: one bounded Mirax copy open in the same interactive session, using the existing safety settings and budget, with read-only observation of dialog state for the proven owned HWND when opening blocks; compare against one Gozareshkar control. This would distinguish a modal prompt from a non-dialog COM hang before considering an Open argument change or add-in isolation. Do not change global settings, kill existing Excel, or repeat a long stress test. Exact target/seed close substages are now available on any subsequent close failure.

Canonical plan/current state/master/roadmap updated: Excel Host Closure OPEN; PJ-012 COMPLETE. NEXT_PJ_MILESTONE remains Excel Host Closure until resolved. The latest roadmap does not define a formal successor after it; no PJ-013 identifier/scope is created. Older duplicate PJ-010 entries remain historical independent policy/review work, not an invented automatic next milestone.

Finalizer DryRun PASS (exit 0): packages only MATRIX-AFTER.json, MATRIX-BEFORE.json and REVIEW.md; explicitly reports no external writes. Normal commit/push hash and final synchronization are recorded in the final response after execution. No external Plans/Review writes; no workbooks, secrets, env files, databases, raw logs or business contents staged. Commit hash is reported outside this commit because a commit cannot contain its own hash.

## Requested report mapping

1–4: starting HEAD/clean baseline, reviewed tooling and approved paths above. 5–8: before/after G1/M1/G2/M2 tables. 9–10: exact failures and nullable HRESULT/error class above. 11–13: eight proven owned instances, no orphans, identical hashes. 14–17: session evidence; no confirmed workbook-specific or root cause; multiple lifecycle modes. 18–20: diagnostic fix implemented, underlying stability fix not claimed, same matrix remained intermittent. 21–24: 16 focused / 215 full, syntax and diff PASS. 25: three code/test files, two safe matrix JSON files, this review, task changelog and four canonical documents. 26: docs/handoffs/PJ-EXCEL-HOST-CLOSURE/REVIEW.md. 27: four canonical updates. 28–31: finalization recorded below and final response. 32: closure complete NO / OPEN. 33: intermittent opening/close/quit blocker. 34: current next milestone still Excel Host Closure; successor undefined in latest roadmap.
