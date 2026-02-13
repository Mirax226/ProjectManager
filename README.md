# Patch Runner Bot

Telegram bot that applies git patches to GitHub repositories and opens pull requests automatically.

## Requirements
- Node.js 18+
- Environment variables (can be stored in a `.env` file):
  - `BOT_TOKEN` – Telegram bot token
  - `ADMIN_TELEGRAM_ID` – only this user can interact with the bot
  - `GITHUB_TOKEN` – GitHub personal access token with `repo` scope
  - `WORKDIR` – directory where repositories are cloned (default `/tmp/patch-runner-bot`)
  - `DEFAULT_BASE_BRANCH` – default branch name (default `main`)
  - `PORT` – optional, for webhook usage

## Install
```
npm install
```

## Run
```
node src/bot.js
```

## Render deploy notes
- Build command: `npm install` (no extra installs needed).
- Start command: `node src/bot.js`.

## Usage
1. Start the bot with `/start` from the admin Telegram account.
2. Use **Projects** → **Add project** to register repositories (owner/repo/base branch).
3. Manage settings per project from **Projects** → **List projects** → **Settings**:
   - Rename project or change base branch.
   - Add/edit/clear start, test, and diagnostic commands.
   - Add/edit/clear Render service and deploy hook URLs.
   - Add/edit/clear Supabase binding.
   - Mark a project as the default project.
4. Use **Projects** → **Global settings** to override default base branch or clear defaults.
5. Choose **Projects** → **List projects** and tap **Apply patch** for a project, then send a git patch as text or a `.patch`/`.diff` file.
6. The bot updates the repo, creates a branch, applies the patch, pushes it, and opens a PR. A link to the PR is sent back.
7. Use **Ping test** to check GitHub API and git fetch latency.

## Project log forwarding
External services can forward logs to Project Manager for per-project alerting:

```
POST https://<path-applier-host>/project-log/<projectId>
Content-Type: application/json

{
  "level": "error",
  "message": "Unhandled exception in /webhook",
  "stack": "Error: ...",
  "context": {
    "requestId": "abc123",
    "userId": 42
  },
  "timestamp": "2026-01-06T10:00:00.000Z",
  "source": "daily-system-bot"
}
```

If JSON is not convenient, send a plain text body; it will be treated as an error-level message. No authentication is required for now.

## PM database separation + Render Postgres migration

### Namespace strategy (going forward)
- Use a dedicated schema (`pm`) for new PM tables (preferred).
- Current PM tables (from migrations):
  - `env_var_sets`
  - `project_env_vars`
  - `cron_job_links`
  - `project_telegram_bots`
  - `project_log_settings`
  - `project_recent_logs`

### Export (schema + data)
```
./pm db:export --source "$PM_DB_SOURCE_URL" --out-dir ./pm-db-export
```
- Dry-run (schema only):
```
./pm db:export --source "$PM_DB_SOURCE_URL" --out-dir ./pm-db-export --schema-only
```

### Import (schema + data + indexes/constraints)
```
./pm db:import --target "$PM_DB_TARGET_URL" --out-dir ./pm-db-export
```
- Includes row-count validation when `PM_DB_SOURCE_URL` is set.

### Safety checks
- Preflight runs `SELECT 1` on source/target before export/import.
- Import order:
  1) `schema.sql` (tables/sequences)
  2) `data.sql` (COPY data)
  3) `post-data.sql` (indexes/constraints)

### Cutover (Render deploy)
1) Provision a new Render Postgres for PM.
2) Export from the current shared DB.
3) Import into the new PM DB.
4) Set `DATABASE_URL_PM` (preferred) or `PATH_APPLIER_CONFIG_DSN` on the PM service.
5) Deploy PM with the new `DATABASE_URL_PM`.
6) Verify counts and run a smoke check (mini-site + /healthz).

## Ops & Reliability
- **Config DB status UX:** main menu no longer shows `Config DB: Ready`; only degraded/down/misconfigured states are shown.
- **Internal ops event log:** PM stores bounded internal reliability events (`ops_event_log`) with secret masking and uses those events for alert routing.
- **Alert routing controls:** per-user alert preferences (`ops_user_alert_prefs`) support severity threshold, muted categories, and destination toggles (`admin_inbox`, `admin_room`, `admin_rob`) with rate limits/debounce.
- **Muted categories by default:** `INVALID_URL`, `ENV_MISCONFIG`, and `MISSING_DSN` are internal-only and are never routed to Telegram alerts.
- **DB health snapshot:** PM tracks `HEALTHY | DEGRADED | DOWN | MISCONFIG` and outage/recovery IDs (`ops_db_health_snapshot`).
- **Recovery notice debounce:** on DB recovery, PM sends exactly one `✅ Config DB is back online.` per outage with a `✅ Readed` action for acknowledgment.
- **Ping test upgrade:** Ping includes Config DB health, latency, and last error category hint.


### Audit report (stability + DB + UX + safety)
- **Found:** potential endless Config DB retry loop during long outages, missing explicit secondary DB ping output, and no progress indicator during longer ping checks.
- **Fixed:** added capped Config DB retries per boot (`CONFIG_DB_MAX_RETRIES_PER_BOOT`, default 20) with degraded-mode halt + internal event; Ping test now reports primary DB health/latency and optional secondary DB health when `DATABASE_URL_PM_SECONDARY` or `PATH_APPLIER_CONFIG_DSN_SECONDARY` is set; added progress reporter that updates one message during Ping test.
- **Safety checks:** secret redaction tests expanded for DSN/password/token masking; recovery debounce explicitly unit-tested so outage recovery alert is sent once per outage cycle.
- **How to verify:** run `npm test`, run Ping Test from Settings (observe progress + DB lines), and boot without `DATABASE_URL_PM` to confirm degraded mode without crash loops.



## Routine Fixes (Codex Tasks)
- Open **Settings → Diagnostics → 🧰 Routine Fixes (Codex Tasks)**.
- Entry points:
  - `🔎 Analyze last error` (uses internal `ops_event_log` context)
  - `⚡ Quick detect (keyword / error)`
  - `🧾 Paste error/log text`
  - `🆔 Enter RefId`
  - `📎 Forward message`
  - `📚 Rule catalog`
- Output is deterministic and always follows:
  - `[Diagnosis]`
  - `[What you do now]`
  - `[Codex Task (copy)]` code block
- Routine outputs include inline actions:
  - `📋 Copy task`
  - `🗑 Delete`
  - `◀️ Back`

### Add new routine rule
- Add one file per rule under `src/routineFixes/rules/`.
- Export object with: `id`, `title`, `triggers`, `match(ctx)`, `render(fields)`.
- Register the rule in `src/routineFixes/index.js`.

### Security notes
- No LLM and no external API calls are used for routine matching.
- Callback payloads contain no secrets.
- Secret-like values in diagnostics remain masked; avoid adding raw tokens/DSNs to user-visible messages.

## Telegram UX updates
- Main menu panels are tracked per `chatId:userId` with a persistent registry (`menuMessageRegistry`) when Config DB is available; stale old menus are deleted or keyboard-disabled.
- Secondary transient messages include owner-scoped `🗑 Delete` and auto-delete after 10 seconds.
- Main menu/submenu panels do not append delete actions.
- Project creation confirmation format now uses:
  - `✅ Project created.`
  - `🏷️ Name: ...`
  - `🆔 ID: ...`

## Repo picker (token-first)
- Create Project flow now asks for GitHub PAT first.
- After validation, repos are listed as inline buttons with pagination (`Prev/Next`).
- Repo is selected by button click; manual `owner/repo` typing is no longer required in that step.
- Every wizard step includes `◀️ Back` + `❌ Cancel`.

## Multi-DB sync/migration engine primitives
- Added core sync/migration utilities in `src/dbSyncEngine.js`:
  - Last-write-wins conflict resolver (`updated_at`)
  - Batch partitioning for large-copy operations
  - Pair sync diff helper
- These functions are covered by unit tests and are ready to be wired into project DB UI actions (`Sync now`, `Migrate DB`).

## Ops Suite v1

PM now includes an operations-first suite with:
- Normalized main menu: Projects, Database, Cron Jobs, Deployments, Logs, Settings, Help.
- Ops Timeline with bounded retention and filters (severity/type/scope).
- Automatic Safe Mode for restart loops, DB outages, error spikes, and memory pressure.
- Actionable alerts (Routine Fix, Copy debug, Timeline, Delete), with cron API failures routed as `CRON_ERROR` ops events.
- Drift detector snapshots and compare reports.
- Rule-generated runbooks and quick routine detection.
- Shadow-run plans for dangerous operations before execute.
- Built-in project templates (Node Telegram bot, worker, API, cron-only).
- Guest read-only role controls for safe observability.
- Expanded Help pages for each major Ops feature.

## Navigation stack & menu regrouping
- Added per-user navigation stack behavior (chatId + userId) with bounded history and centralized back/home handling.
- Regrouped global hubs and project menu buckets, and moved Codex Tasks access to project scope.
- Updated logs hub/settings placement and transient notice behavior alignment.
