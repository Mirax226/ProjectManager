[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)][ValidateSet('PJ')][string]$Project,
  [Parameter(Mandatory = $true)][ValidatePattern('^[A-Za-z0-9][A-Za-z0-9._-]*$')][string]$Task,
  [switch]$DryRun,
  [string]$PlansRoot = (Join-Path $HOME 'Documents\GitHub\Plans'),
  [string]$ReviewRoot = (Join-Path $HOME 'Desktop\Review'),
  [string]$StagingRoot = (Join-Path ([System.IO.Path]::GetTempPath()) 'ProjectFinalizer')
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$planRoot = Join-Path $repoRoot 'docs\project-plan'
$taskRoot = Join-Path $planRoot "versions\$Task"
$handoffRoot = Join-Path $repoRoot "docs\handoffs\$Task"
$currentPlan = Join-Path $planRoot 'PJ_CURRENT_PLAN.md'
$changelog = Join-Path $taskRoot 'CHANGELOG.md'
$review = Join-Path $handoffRoot 'REVIEW.md'

function Assert-File([string]$Path, [string]$Label) {
  if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "$Label is missing: $Path" }
  if ((Get-Item -LiteralPath $Path).Length -le 0) { throw "$Label is empty: $Path" }
}
function Get-ReviewName([string]$Root) {
  $stamp = Get-Date -Format 'dd_HHmm'
  $base = "${Project}_${stamp}(review)"
  $zip = Join-Path $Root "$base.zip"
  if (-not (Test-Path -LiteralPath $zip)) { return "$base.zip" }
  for ($i = 1; $i -lt 1000; $i++) {
    $candidate = Join-Path $Root ("{0}_{1}(review).zip" -f $base, $i)
    if (-not (Test-Path -LiteralPath $candidate)) { return [System.IO.Path]::GetFileName($candidate) }
  }
  throw 'Unable to allocate a collision-free Review filename.'
}
function Assert-SafeReviewFiles([string]$Root) {
  $forbidden = @('\.env$', '\.env\.', '\.(key|pem|pfx|sqlite|db)$', '\.xlsm$', '\.xlsx$', '(^|[\\/])node_modules([\\/]|$)', '(^|[\\/])\.git([\\/]|$)')
  $files = @(Get-ChildItem -LiteralPath $Root -File -Recurse)
  foreach ($file in $files) {
    $relative = $file.FullName.Substring($Root.Length).TrimStart('\','/')
    if ($forbidden | Where-Object { $relative -match $_ }) { throw "Forbidden review artifact: $relative" }
    if ($file.Length -le 0) { throw "Empty review artifact: $relative" }
  }
  return $files
}

try {
  Assert-File $currentPlan 'Current plan'
  Assert-File $changelog 'Task changelog'
  if (-not (Test-Path -LiteralPath $handoffRoot -PathType Container)) { throw "Task handoff directory is missing: $handoffRoot" }
  Assert-File $review 'REVIEW.md'
  $reviewFiles = @(Assert-SafeReviewFiles $handoffRoot)
  $reviewName = Get-ReviewName $ReviewRoot
  $planTarget = Join-Path $PlansRoot "$Project\Current\PJ_CURRENT_PLAN.md"
  $changelogTarget = Join-Path $PlansRoot "$Project\Versions\$Task\CHANGELOG.md"
  $filesToPackage = @($reviewFiles | ForEach-Object { $_.FullName.Substring($handoffRoot.Length).TrimStart('\','/') })
  Write-Output "Repository: $repoRoot"
  Write-Output "Source plan: $currentPlan"
  Write-Output "Source changelog: $changelog"
  Write-Output "Plan target: $planTarget"
  Write-Output "Changelog target: $changelogTarget"
  Write-Output "Review target: $(Join-Path $ReviewRoot $reviewName)"
  Write-Output ('Files to package: ' + ($filesToPackage -join ', '))
  if ($DryRun) { Write-Output 'DRY RUN: no external writes performed.'; exit 0 }

  $runStage = Join-Path $StagingRoot "$Project\$Task\$([guid]::NewGuid().ToString('N'))"
  New-Item -ItemType Directory -Path $runStage -Force | Out-Null
  $success = $false
  try {
    $reviewStage = Join-Path $runStage 'review'
    New-Item -ItemType Directory -Path $reviewStage -Force | Out-Null
    foreach ($file in $reviewFiles) {
      $relative = $file.FullName.Substring($handoffRoot.Length).TrimStart('\','/')
      $destination = Join-Path $reviewStage $relative
      New-Item -ItemType Directory -Path (Split-Path $destination) -Force | Out-Null
      Copy-Item -LiteralPath $file.FullName -Destination $destination
    }
    $zipStage = Join-Path $runStage $reviewName
    Compress-Archive -Path (Join-Path $reviewStage '*') -DestinationPath $zipStage -CompressionLevel Optimal
    if (-not (Test-Path -LiteralPath $zipStage) -or (Get-Item -LiteralPath $zipStage).Length -le 0) { throw 'Review ZIP was not created or is empty.' }
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($zipStage)
    try {
      $names = @($archive.Entries | ForEach-Object FullName)
      foreach ($expected in $filesToPackage) { if ($names -notcontains ($expected -replace '\\','/')) { throw "Review ZIP is missing: $expected" } }
    } finally { $archive.Dispose() }
    New-Item -ItemType Directory -Path (Split-Path $planTarget) -Force | Out-Null
    New-Item -ItemType Directory -Path (Split-Path $changelogTarget) -Force | Out-Null
    New-Item -ItemType Directory -Path $ReviewRoot -Force | Out-Null
    Copy-Item -LiteralPath $currentPlan -Destination "$planTarget.tmp" -Force; Move-Item -LiteralPath "$planTarget.tmp" -Destination $planTarget -Force
    Copy-Item -LiteralPath $changelog -Destination "$changelogTarget.tmp" -Force; Move-Item -LiteralPath "$changelogTarget.tmp" -Destination $changelogTarget -Force
    Copy-Item -LiteralPath $zipStage -Destination (Join-Path $ReviewRoot $reviewName)
    $success = $true
    Write-Output "Finalized: $(Join-Path $ReviewRoot $reviewName)"
  } finally {
    if ($success) { Remove-Item -LiteralPath $runStage -Recurse -Force }
    else { Write-Error "Finalizer failed; staging retained at $runStage" }
  }
} catch {
  Write-Error $_.Exception.Message
  exit 1
}
