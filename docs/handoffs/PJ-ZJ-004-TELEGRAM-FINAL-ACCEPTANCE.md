# PJ-ZJ-004 Telegram emoji UX and live acceptance

Date: 2026-10-03. PJ started clean and synced at `0b9325c2db49e9479e1323598a9d13f48f521b69`. ZJ stayed clean and synced at `f7d8bd43f9a1431a04d4e57bd3566dd18086a331`; DailySystem's documented Mirax work was untouched. Emoji UX commit `21519a1a14e16e732fb9abd6a8fa94474dc9eee6` was pushed to PJ `origin/main` and deployed as Worker version `4d8b95c9-67d1-4a89-b471-348f2a376098`. PJ `/health` returned HTTP 200 and D1 available; `PJ_ZJ_ENABLED=true` remained bound.

## Visible menu and callback contract

`src/telegramApplication.js` is the Telegram menu builder and handler. `test/zjControlPlane.test.js` covers the navigation and callback contract. The table lists stable callback data; only display text and Back/Home navigation were added.

| Visible label | Callback data | Handler | Project |
|---|---|---|---|
| 📁 Projects | `projects` | Project list | PJ |
| 📘 DailySystem | `project daily-system` | Project menu | DailySystem |
| 🎓 ZJ | `project zj` | Project menu | ZJ |
| 📊 Status | `zj_status` | Status summary | ZJ |
| 🗂 Repository | `zj_repo` | `ZJ_REPO_STATUS` job | ZJ |
| 🧾 Release Evidence | `zj_release` | `ZJ_RELEASE_EVIDENCE` job | ZJ |
| ✅ Release Readiness | `zj_readiness` | `ZJ_RELEASE_READINESS` job | ZJ |
| 🧪 Local Validation | `zj_validate` | `ZJ_LOCAL_VALIDATION` job | ZJ |
| 🩺 Staging Health | `zj_staging` | `ZJ_STAGING_HEALTHCHECK` job | ZJ |
| 🕘 Last Jobs | `zj_jobs` | ZJ job list | ZJ |
| 📊 Status | `project_status daily-system` | Existing project-status job | DailySystem |
| 🩺 Health | `health daily-system` | Existing health job | DailySystem |
| 🧾 Jobs | `jobs` | Existing job list | PJ/DailySystem menu |
| ⬅️ Back | `projects` or `project zj` / `project daily-system` | Parent menu | Contextual |
| 🏠 Home | `start` | Admin home | PJ |
| 🔄 Refresh | `zj_status` or `zj_jobs` | Re-renders current view | ZJ |

Existing top-level callbacks `status`, `runners`, `incidents`, `jobs`, and `projects` remain stable. Callback labels are never used as job identifiers. Unknown callbacks show the existing help response; unauthorized users remain rejected. Disabled ZJ remains listed as disabled but does not appear as a selectable project. The local focused suite passed 18/18, full PJ suite 234/234, `npm run check`, and `git diff --check`.

## Durable Runner and post-deploy proof

Separate `windows-runner`/`daily-system` and `zj-runner`/`zj` credentials remain in the owner-bound Windows DPAPI CurrentUser store outside Git. Each owned Runner was stopped and restarted from that store with no token re-entry. Both cross-project requests returned HTTP 403 `project_scope_denied`. After restart, DailySystem HEALTHCHECK job `421167ca-951e-4c95-9419-f0d6bd5c7d89` and ZJ_REPO_STATUS job `af5430e9-5dfd-470b-ac2e-81da340f250b` succeeded. After deploy, DailySystem HEALTHCHECK job `8685e5a9-a40a-4aa5-862d-b789cebb86ec` and ZJ_REPO_STATUS job `1c57b03f-5a33-4fd4-80c6-29fd644b9987` succeeded. Both scoped Runners reported ONLINE with recent heartbeats. Secret audit found no plaintext tokens in repository files, startup entries, local files, or process command lines. No ZJ production Worker, D1, webhook, Cron, AI setting, or Telegram bot was changed or called.

## Human acceptance gate

The owner was asked to inspect the private ProjectManager admin bot: `/projects`, the ZJ menu, Repository acknowledgement/job/result, and Local Validation acknowledgement/job/result. Telegram-origin D1 records and Telegram-visible results remain unverified until the owner reports the actual job IDs and observations. API-created canaries are not substitutes. Both Runners are left online for the human check. Therefore `PJ_TELEGRAM_ZJ_ACCEPTANCE=PENDING`, `PJ_ZJ_MULTI_PROJECT_CONTROL_PLANE_COMPLETE=NO`, and `READY_FOR_ANTIGRAVITY_PJ_ZJ_ACCEPTANCE=NO`. When owner evidence arrives, match each job ID to D1 `requested_by=telegram-admin`, project, type, terminal status, result Runner and structured result; then update this handoff and continuity docs.
