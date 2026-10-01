# OWNER ONLY: do not run automatically in Codex. No credential values in this file.
# Run in a separate PowerShell window from the canonical repository.
# -Webhook rotates only if the existing matching webhook secret was lost.
# -Runner rotates the existing single-runner map; keep this shell open afterward.
param([switch]$Webhook, [switch]$Runner)
$ErrorActionPreference = 'Stop'
if ($Webhook -eq $Runner) { throw 'Choose exactly one owner action: -Webhook or -Runner' }
Set-Location $PSScriptRoot
Set-Location '../../..'
$pjIdentity = npx wrangler whoami --json | ConvertFrom-Json
if (-not $pjIdentity.loggedIn -or @($pjIdentity.accounts.id) -notcontains '9f12f5d584ab6b4bd94c66d4bf7f53dc') { throw 'CLOUDFLARE_ACCOUNT_AMBIGUOUS' }
$pjOrigin = 'https://projectmanager-control-plane.amirhoseinsalmani194728095.workers.dev'
$pjHealth = Invoke-RestMethod "$pjOrigin/health"
if (-not $pjHealth.ok -or $pjHealth.d1 -ne 'available') { throw 'Worker health failed' }
$pjBytes = New-Object byte[] 32
$pjRandom = [Security.Cryptography.RandomNumberGenerator]::Create()
try { $pjRandom.GetBytes($pjBytes) } finally { $pjRandom.Dispose() }
$pjNewSecret = [BitConverter]::ToString($pjBytes).Replace('-', '').ToLowerInvariant()
[Array]::Clear($pjBytes, 0, $pjBytes.Length)
if ($Runner) {
  # Replaces the owner-confirmed single-runner map. Preserve additional runners separately.
  $pjMap = @{ 'windows-runner' = @{ token = $pjNewSecret; project = 'daily-system' } }
  $pjJson = $pjMap | ConvertTo-Json -Depth 4 -Compress
  $pjJson | npx wrangler secret put PG_RUNNER_TOKENS_JSON
  if ($LASTEXITCODE -ne 0) { throw 'Runner secret upload failed; do not authenticate' }
  $env:PG_RUNNER_TOKEN = $pjNewSecret
  $env:PG_RUNNER_ID = 'windows-runner'
  $env:PG_PROJECT_ID = 'daily-system'
  $env:PG_CONTROL_PLANE_URL = $pjOrigin
  $pjMap = $null; $pjJson = $null; $pjNewSecret = $null
  Write-Output 'RUNNER_SCOPE_CORRECTED: windows-runner / daily-system. Keep this shell open; E2E not yet run.'
} else {
  $pjInput = Read-Host 'Existing Telegram bot token (masked)' -AsSecureString
  $pjPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($pjInput)
  try { $env:TELEGRAM_BOT_TOKEN = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pjPointer) }
  finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pjPointer); $pjInput.Dispose() }
  $env:TELEGRAM_WEBHOOK_SECRET = $pjNewSecret
  $env:PJ_WORKER_URL = $pjOrigin
  $pjNewSecret = $null
  $env:TELEGRAM_WEBHOOK_SECRET | npx wrangler secret put TELEGRAM_WEBHOOK_SECRET
  if ($LASTEXITCODE -ne 0) { throw 'Webhook secret upload failed' }
  npm run telegram:webhook -- --set
  if ($LASTEXITCODE -ne 0) { throw 'Keep shell open; retry --set with SAME generated secret, not this rotation script' }
  npm run telegram:webhook -- --info
  if ($LASTEXITCODE -ne 0) { throw 'Keep shell open; retry --info before clearing values' }
  node -e 'const {telegramCall}=require("./src/controlPlane/telegramWebhook");telegramCall(process.env.TELEGRAM_BOT_TOKEN,"getWebhookInfo",{}).then(x=>console.log(JSON.stringify({allowedUpdates:(x.allowed_updates||[]).filter(v=>["message","callback_query"].includes(v)),pendingUpdateCount:x.pending_update_count||0,lastErrorDate:x.last_error_date||null}))).catch(()=>{console.error("Safe verification failed");process.exitCode=1})'
  if ($LASTEXITCODE -ne 0) { throw 'Safe getWebhookInfo failed' }
  Remove-Item Env:TELEGRAM_BOT_TOKEN, Env:TELEGRAM_WEBHOOK_SECRET -ErrorAction SilentlyContinue
  Write-Output 'Now verify /start and /status in administrator private chat. No chat smoke result is claimed here.'
}
