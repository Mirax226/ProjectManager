# PJ-012 Cloudflare activation runbook

Cloudflare is the designated production runtime. Activation is pending bootstrap administrator IDs and secret provisioning. Wrangler OAuth succeeded; the owner explicitly confirmed Amirhoseinsalmani19472@gmail.com's Account (9f12f5d584ab6b4bd94c66d4bf7f53dc). That account is pinned in wrangler.jsonc. D1 projectmanager-control-plane (1871da4e-4078-48d7-ac02-62ecb0662004), binding CONTROL_PLANE_DB, was created and all three migrations applied remotely on 2026-10-01. No existing unrelated database was changed. Render and old Postgres are DEPRECATED / UNUSED / NON-AUTHORITATIVE by owner decision; NO MIGRATION REQUIRED. No old data/resource was read, reconciled, recovered or deleted for this migration. Stop legacy polling separately when activating the webhook; an old deployment must not call deleteWebhook again.

## Architecture and routes

Telegram -> HTTPS Worker webhook -> shared Telegram application/Ops Center models -> existing Control Plane/D1 -> typed jobs -> local Windows Runner -> local execution. One Worker: projectmanager-control-plane. GET /health and /healthz expose only status/runtime/service/version/D1 availability. POST /telegram/webhook handles messages and callbacks. /api/v1/... stays compatible with Runner; /api/control-plane/... aliases it. GET/POST /api/v1/config requires admin authentication and uses existing managed-config validation/audit. No Postgres/Node bot bootstrap in the Worker bundle.

Bootstrap TELEGRAM_ADMIN_USER_IDS is a comma-separated numeric user-ID var, deliberately empty by default. D1 config may add existing admin IDs. Only listed administrators in their own private chat can use the Worker UI. Telegram commands: /start /admin /status /runners /incidents /jobs /job ID /health PROJECT /project_status PROJECT /excel_healthcheck PROJECT ASSET. Buttons use the same dispatcher. Existing Ops Center models and operational-job builder are reused. The large legacy Node handler collection remains compatibility code; arbitrary shell, deploy/delete, CODEX and authoritative sync actions are not exposed in this V1 Telegram surface. EXCEL_SYNC remains disabled at the local execution boundary.

## Required secrets (names only)

TELEGRAM_BOT_TOKEN; TELEGRAM_WEBHOOK_SECRET; PG_CONTROL_PLANE_ADMIN_TOKEN; PG_RUNNER_TOKENS_JSON. PG_PROJECT_TOKENS_JSON is optional for project API access. Do not place these values in wrangler.jsonc, command arguments, documentation, logs or Git. Neither DATABASE_URL_PM nor PATH_APPLIER_CONFIG_DSN is required.

## Owner activation commands

Run from the canonical repository. Authenticate before resource inspection; select only the intended Cloudflare account. Confirm free-tier availability and reuse an existing D1 database where appropriate.

```powershell
npx wrangler whoami --json
npx wrangler d1 list --json
# Account must match wrangler.jsonc account_id before any mutation.
# Database already provisioned: do not create it again.
```

The verified non-secret account ID and database UUID are already pinned in wrangler.jsonc. Set TELEGRAM_ADMIN_USER_IDS to the owner's numeric Telegram ID(s). Leave secrets out of that file. D1 migration filenames are preserved under migrations/d1; Postgres historical migrations remain outside this directory. Wrangler's migration ledger applies each D1 migration once. For an existing deployment, verify its prior migration ledger first; the PJ-012 ALTER adds a result-owner column once.

```powershell
npx wrangler secret put TELEGRAM_BOT_TOKEN
npx wrangler secret put TELEGRAM_WEBHOOK_SECRET
npx wrangler secret put PG_CONTROL_PLANE_ADMIN_TOKEN
npx wrangler secret put PG_RUNNER_TOKENS_JSON
# Optional:
npx wrangler secret put PG_PROJECT_TOKENS_JSON
npx wrangler secret list
npx wrangler d1 migrations apply projectmanager-control-plane --remote
npm run worker:deploy
```

Enter secret values only into Wrangler's interactive prompt. Runner-token JSON uses the existing shape: each runner ID maps to an object with token and project; never publish that object. Telegram webhook secret must use A-Z/a-z/0-9/underscore/hyphen (1..256 characters). For local development create an ignored .dev.vars manually; no real values are supplied by this repository.

After deploy, visit the exact returned public Worker origin /health; expect runtime cloudflare, d1 available. Set PJ_WORKER_URL to that explicit HTTPS origin in the local setup process. Supply TELEGRAM_BOT_TOKEN and TELEGRAM_WEBHOOK_SECRET there using secure owner input; they are never CLI arguments. Then:

```powershell
npm run telegram:webhook -- --set
npm run telegram:webhook -- --info
```

The tool calls only Telegram's fixed Bot API host. It sets secret_token, allowed_updates=message/callback_query, max_connections=1, drop_pending_updates=false. Verification prints only match/pending-count/error-presence/certificate booleans; no token, Telegram error description or raw API response. Webhook activation changes the Telegram update delivery path and must follow successful deployment.

## Safe production smoke checks

With the owner's bot chat: /start, /status, /health daily-system, then /job RETURNED_ID. Configure local Runner PG_CONTROL_PLANE_URL to the Worker origin; PG_RUNNER_TOKEN, PG_RUNNER_ID and PG_PROJECT_ID must match the scoped Worker secret. Set the local project-bound repository profile, run npm run runner, and confirm heartbeat, claim/lease, typed health result and SUCCEEDED state in D1/Telegram. This health job checks local repository availability and does not run Excel, shell, deploy, SQL mutation or sync. Do not use destructive operations for smoke checks.

## Delivery and persistence limits

Update IDs/status/lease owners are persisted, not raw Telegram updates. Concurrent deliveries use an atomic D1 update lease. Completed duplicates are acknowledged; an in-flight duplicate receives a retryable response. Failed handling releases the lease. Diagnostic job idempotency uses the update ID, preventing repeated job creation after delivery failures. Telegram sendMessage is not transactional with D1: a crash after Telegram accepts a message but before DONE can cause a repeated reply; exactly-once external message delivery is not claimed. The application data effect remains idempotent for allowed diagnostic jobs.

D1 stores are fresh per request; atomic SQL claims/result compare-and-swap protect cross-isolate jobs. Runner submits attemptCount, preventing prior attempts being accepted after reclaim. Terminal duplicates require the recorded result runner. Existing project/lease/stale/idempotency/retry rules remain. Initial CLI refresh failed, but the required CLI retry succeeded. The owner confirmed the account before D1 provisioning. Secret values were never read or requested. Worker metadata showed this service did not exist before provisioning, so deployed secret names were unavailable. Deployment remains pending secrets/admin IDs; no live Worker URL, webhook state or production smoke result is claimed. Wrangler is the primary path; browser fallback is reserved for a required operation unavailable safely through CLI. Recheck whoami and pinned account before every production mutation. If account identity becomes ambiguous, stop with CLOUDFLARE_ACCOUNT_AMBIGUOUS.

Owner follow-up: Telegram bootstrap administrator 843686302 configured in wrangler.jsonc. Remaining activation gate is interactive secret provisioning and live CLI verification/deployment.

## Activation resume — 2026-10-01 (authoritative current checkpoint)

DEPLOYED; HEALTH_VERIFIED; READY_FOR_WEBHOOK_ACTIVATION. Actual repository Worker deployed from clean 8d15a46e46e3f4b1b10706c36ea2fee117a163b3, preserving all previous PJ-012 changes. Confirmed account 9f12f5d584ab6b4bd94c66d4bf7f53dc; all four required secret names present before deployment. D1 CONTROL_PLANE_DB -> projectmanager-control-plane / 1871da4e-4078-48d7-ac02-62ecb0662004 exists; remote migration listing has no pending migrations. No data dump or secret value read.

Public origin: https://projectmanager-control-plane.amirhoseinsalmani194728095.workers.dev. Version 6dbe1049-ace7-4b84-87e2-9777e41cb715; deployment 0460e9ac-cc1f-40f7-8416-cc03036d74b9 at 100 percent. Both /health and /healthz returned HTTP 200, ok=true, runtime=cloudflare, service=projectmanager, d1=available, version=PJ-012. Safe health service label differs from deployment name; no code change needed. Worker contains no legacy Node bot/Config DB bootstrap, no Postgres DSN requirement and no Render dependency. External legacy resources untouched.

Release: 196 passed, 0 failed, 0 skipped, 31.8742396 seconds; npm run check and git diff --check passed. Initial sandbox run failed because Wrangler/workerd were restricted; authorized host-context rerun passed. Wrangler identity verification succeeded with existing OAuth; no login or scope refresh requested. No browser or Excel probe.

RUNNER_SECRET_SCOPE_MISMATCH: canonical client defaults src/runner/client.js are PG_RUNNER_ID=windows-runner and PG_PROJECT_ID=daily-system. Test fixtures explicitly override windows-01; they do not establish deployment configuration. No current PG_RUNNER_ID/PG_PROJECT_ID override or PG_RUNNER_TOKEN exists. Owner-provisioned secret is stated to bind windows-01/daily-system; values were not inspected. No Runner E2E attempted. Either explicitly configure windows-01 locally to match the existing owner-selected secret, or owner aligns secret to windows-runner; do not change or rotate automatically. Token continuity remains unavailable; secure owner-coordinated reprovisioning is needed before E2E unless the existing token is retained outside this process. Rotation is not needed for Worker/webhook readiness.

Telegram token and webhook secret names are deployed; corresponding process environment names absent. WEBHOOK_ACTIVE is NOT claimed: no setWebhook/getWebhookInfo or real chat smoke performed. Owner-side steps below use the SAME existing Cloudflare webhook secret; no extraction/rotation. PJ-012 COMPLETE: NO; Worker deployment portion complete, webhook/live Telegram and optional Runner smoke outstanding.

## Owner-only webhook activation (PowerShell)

Run in the canonical repository after confirming /health. Enter the bot token and the SAME webhook secret already provisioned in Cloudflare; do not share values in chat. The input is masked, values are passed only through the child process environment, and cleared afterward. Process environment is transient plaintext required by the existing tool; no file/CLI argument is used.

```powershell
Set-Location 'C:\Users\Amir\Documents\GitHub\cloned\ProjectManager'
$env:PJ_WORKER_URL = 'https://projectmanager-control-plane.amirhoseinsalmani194728095.workers.dev'
function Set-PjMaskedEnvironment([string]$name) {
  $pjInput = Read-Host "Enter $name (existing value)" -AsSecureString
  $pjPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($pjInput)
  try {
    [Environment]::SetEnvironmentVariable($name, [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pjPointer), 'Process')
  } finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pjPointer)
    $pjInput.Dispose()
  }
}
try {
  Set-PjMaskedEnvironment 'TELEGRAM_BOT_TOKEN'
  Set-PjMaskedEnvironment 'TELEGRAM_WEBHOOK_SECRET'
  npm run telegram:webhook -- --set
  if ($LASTEXITCODE -ne 0) { throw 'Webhook activation failed; credentials not printed.' }
  npm run telegram:webhook -- --info
  if ($LASTEXITCODE -ne 0) { throw 'Webhook verification failed; credentials not printed.' }
} finally {
  Remove-Item Env:TELEGRAM_BOT_TOKEN, Env:TELEGRAM_WEBHOOK_SECRET -ErrorAction SilentlyContinue
  Remove-Item Function:Set-PjMaskedEnvironment -ErrorAction SilentlyContinue
}
```

Expect webhookMatchesExpected=true. Existing historical lastErrorPresent may remain after a resolved error; confirm pending updates drain and a fresh /start or /status response arrives for administrator 843686302. These are read-only commands. Do not queue health jobs until Runner scope/token/profile is resolved. Never let legacy polling call deleteWebhook after activation.

Runner next procedure (owner only): choose the intended ID explicitly, keep project daily-system, generate and retain a fresh cryptographically random token securely if old token is lost; enter scoped JSON via `npx wrangler secret put PG_RUNNER_TOKENS_JSON` interactively, then configure that same token locally with masked input and explicit PG_RUNNER_ID/PG_PROJECT_ID/PG_CONTROL_PLANE_URL. Do not print the token or JSON. Verify local project repository profile, heartbeat, then one HEALTHCHECK/PROJECT_STATUS job. Do not rotate automatically or alter other runner entries.
