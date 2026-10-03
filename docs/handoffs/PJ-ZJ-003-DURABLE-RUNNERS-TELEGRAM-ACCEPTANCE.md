# PJ-ZJ-003 durable Runners and Telegram acceptance

Date: 2026-10-03. This entry supersedes PJ-ZJ-002's process-only operational state. It records live evidence and open acceptance gates without secret values.

## Baseline and storage

PJ started clean and synced at `969bbe88e190719aa95dd48677def506a01f3203`. ZJ started clean and synced at `f7d8bd43f9a1431a04d4e57bd3566dd18086a331` on `feature/P0-011-weekly-report`. DailySystem main/origin stayed `077a2ebd63de008a055c9e3dd0e134b9d398db80`, with its documented Mirax work untouched.

The existing Env Vault uses a separately supplied `ENV_VAULT_MASTER_KEY` and a legacy config DB or process-memory fallback; it does not provide a proven local restart path for these Runners. `EXISTING_ENV_VAULT_SUITABLE_FOR_RUNNER_SECRETS=NO`. The selected storage is `WINDOWS_DPAPI_CURRENT_USER`: separate encrypted records for `windows-runner`/`daily-system` and `zj-runner`/`zj` under `%LOCALAPPDATA%\ProjectManager\runner-secrets\`, outside Git. Directory and record ACLs grant the owner account; credentials are decrypted only inside the launcher process, passed to the child Runner through its environment, and cleared from the launcher environment after exit. Tokens are absent from process command lines and startup entries.

A disposable record was encrypted, opened by a fresh process, checked for plaintext absence and wrong decryption-context rejection, and deleted before production rotation. Two fresh independent 32-byte values were then generated and stored in separate records. The complete explicit-scope `PG_RUNNER_TOKENS_JSON` map was uploaded through Wrangler standard input. An earlier upload in the same task was superseded by a second coordinated rotation when the storage implementation was changed to direct DPAPI bytes. No other Cloudflare secret was changed. Neither credential value was printed or committed.

Task Scheduler was attempted first with owner interactive identity. It returned result 1 even for a harmless diagnostic action, so both nonfunctional tasks were removed. Two per-user Windows logon startup entries now invoke the repository launcher with project names only. Manual Start uses the same launcher and encrypted records. The entries are persistent, but an actual logoff/logon was not performed in this task; crash restart is manual. See [Windows runner](../WINDOWS-RUNNER.md) for the commands and recovery limits.

## Live restart and scope proof

DailySystem was restored first. Its durable launcher authenticated, heartbeated, completed HEALTHCHECK job `7be46ff3-5397-4df5-8991-0a35780c638c`, and received HTTP 403 `project_scope_denied` for a ZJ job request. The owned process was stopped, then a new process loaded the same encrypted record without token regeneration or re-entry and completed HEALTHCHECK job `08f683f2-4e63-4c6e-bec9-a4ce5a584cee`. `DAILYSYSTEM_RUNNER_RESTART_PROOF=PASS`.

ZJ was started from its separate record and received HTTP 403 `project_scope_denied` for a DailySystem job request. Its owned process was stopped; a new process loaded the same encrypted record and completed ZJ_REPO_STATUS job `fa3c3b28-f1ae-42a6-bc1c-f8e74280cb3b`. ZJ_RELEASE_EVIDENCE job `e08f01ec-ea3b-4cd2-9b1a-d35bc253ab91` also succeeded. ZJ_LOCAL_VALIDATION job `c246d55f-4e3e-4dac-a0ca-4f13063d5c87` SUCCEEDED on attempt 1; D1 stored result `PASS`, 61 files, 1,160 tests passed, 0 failed, result runner `zj-runner`. Both Runners were simultaneously ONLINE in remote D1 with current scoped heartbeats; PJ `/health` returned HTTP 200 with D1 available. The deployed PJ Worker remains version `45f537fb-f9dc-4d47-bb59-0b2fd7296d63` with `PJ_ZJ_ENABLED=true`. PJ local regression passed 234/234; `npm run check` and `git diff --check` passed.

Security scan after rotation found no plaintext tokens in repository files, Runner command lines, local encrypted records or status files, or logon startup entries. Persistent Runner logs are not configured. The temporary diagnostic Task Scheduler task was removed.

## Open gates

ProjectManager Telegram admin `/projects` → ZJ → Repository and Local Validation have not yet been observed in the owner's private chat. A Telegram-origin D1 job must be matched to the owner's job ID; an API-created job is not a substitute. DailySystem Telegram visibility and one-at-a-time Runner outage behavior also remain to be checked. Both Runners are being kept online while awaiting the owner's live Telegram check. Until these gates are recorded, `PJ_TELEGRAM_ZJ_ACCEPTANCE=PENDING`, `PJ_ZJ_MULTI_PROJECT_CONTROL_PLANE_COMPLETE=NO`, and `READY_FOR_ANTIGRAVITY_PJ_ZJ_ACCEPTANCE=NO`.

No ZJ production Worker, D1, webhook, Cron, AI configuration, or ZJ production Telegram bot was mutated or called. DailySystem authoritative Excel resources were not modified. Code and runbook can be recovered from origin/main after push; Runner secret recovery requires the owner-bound local encrypted store and Windows profile, and cannot be reconstructed from Git alone.
