# PJ-ZJ-001 multi-project control plane handoff

Date: 2026-10-03 Asia/Tehran. Starting PJ `main` and `origin/main`: `4aba1b11a62fd2f1bc308eb90a721fe31ef70ed7`, clean. Canonical PJ path: `C:\Users\Amir\Documents\GitHub\ProjectManager`.

## Stop checkpoint

The continuation found ZJ unexpectedly dirty: `tests/worker.test.ts`, 24 insertions / 8 deletions, HEAD unchanged at `4c9fa08bec566b9bba4d0f884ffbf8335ffc76de`. The real local typed queue returned `REPO_DIRTY` for `ZJ_LOCAL_VALIDATION` before any validation execution. This triggers the owner's explicit stop condition. Preserve the external ZJ work. No automatic Git recovery occurred. PJ changes remain uncommitted/unpushed/undeployed and the checked-in PJ-side integration flag is false. The detailed acceptance report is `PJ-ZJ-001-ACCEPTANCE-REPORT.md`.

## Scope implemented locally

PJ registers `daily-system` and `zj` in a static project registry. The registry holds capabilities and a per-project job allowlist; unknown IDs cannot select a repository path or command. Existing DailySystem job types remain. ZJ accepts repository status, release evidence, local validation, staging health and technical readiness only. ZJ has no Excel or production mutation job. `PJ_ZJ_ENABLED` controls PJ's ZJ adapter independently of ZJ's own `PJ_ENABLED`; ZJ remains operational when PJ is unavailable.

The ZJ Windows Runner reads the canonical ZJ repository and external Plans path from runner-local configuration, rejects any job payload parameters, executes Git and npm through fixed argument arrays, and bounds time/output. Validation uses a disposable local clone of the same clean commit so build output does not alter the canonical ZJ checkout. The Worker and Telegram never read local paths. Staging health accepts only an operator-configured HTTPS `zj-staging.*.workers.dev` origin and returns pending if absent. Readiness preserves unknown/pending gates and aggregates previously recorded typed evidence. The existing `cp_projects` and `cp_jobs` D1 schema is reused; no new migration is needed.

Telegram admin retains its existing menu and gains Projects → DailySystem/ZJ. The ZJ menu exposes status, repository, release evidence/readiness, local validation, staging health and last jobs. Long validation returns a queued job ID; `/job ID` retrieves a bounded result. Admin private-chat identity checks remain in force.

## Verified so far

- PJ full local suite: 223 passed, 0 failed, 0 skipped at the first complete green run after UX tests were updated. Rerun after final code changes.
- Real ZJ `ZJ_REPO_STATUS`: `CLEAN_SYNCED`, `feature/P0-011-weekly-report`, HEAD/upstream `4c9fa08bec566b9bba4d0f884ffbf8335ffc76de`, diff check pass.
- Real `ZJ_RELEASE_EVIDENCE`: `ZJ-RC-001`, 61 files / 1,159 tests baseline; staging D1 and CI missing per latest Plans; release not ready. Staging Worker existence is unverified and must remain UNKNOWN.
- Real staging health: not attempted because no approved staging URL is configured; expected `PENDING / STAGING_NOT_CONFIGURED`.
- ZJ product repository remains clean and at the same HEAD as last inspected. No ZJ production action has been executed.

## Open acceptance gates

The real disposable-clone ZJ validation job, final PJ regression, check/build, security audit, and docs review need final results. PJ Cloudflare account/D1/deployment pre-gates, live Worker deploy, PJ Telegram admin E2E and a project-scoped ZJ runner are not yet verified. The existing DailySystem Runner credential is not valid for ZJ. Do not claim completion, deploy, commit or push solely from this partial checkpoint. If a separately scoped ZJ runner credential is unavailable, request owner-controlled provisioning through the established PJ process; never reuse or expose the DailySystem token.

ZJ production Worker `zj`, D1 `zj-db`, webhook, Cron, AI and Telegram bot are outside this change. Preferred staging identities are `zj-staging` and `zj-staging-db`; no staging resource was created or mutated. ZJ release dependency on PJ remains **NO**.
