param(
  [ValidateSet('TestStore','TestRead','Rotate','Run','Start','InstallStartup','Status','Probe','ProbeScope','Audit')]
  [string]$Mode,
  [ValidateSet('daily-system','zj')]
  [string]$Project = 'daily-system',
  [ValidateSet('HEALTHCHECK','ZJ_REPO_STATUS','ZJ_RELEASE_EVIDENCE','ZJ_LOCAL_VALIDATION')]
  [string]$JobType = 'HEALTHCHECK'
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$localAppData = [Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)
if (-not $localAppData) { $localAppData = Join-Path $env:USERPROFILE 'AppData\Local' }
$store = Join-Path $localAppData 'ProjectManager\runner-secrets'
$ids = @{ 'daily-system' = 'windows-runner'; 'zj' = 'zj-runner' }
$endpoint = 'https://projectmanager-control-plane.amirhoseinsalmani194728095.workers.dev'

function Assert-WindowsOwner {
  if (-not $IsWindows) { throw 'Windows CurrentUser encryption is required.' }
  if (-not $localAppData) { throw 'Local application data path is missing.' }
}

function Protect-Directory {
  Assert-WindowsOwner
  [System.IO.Directory]::CreateDirectory($store) | Out-Null
  $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
  & icacls.exe $store '/grant:r' "*$($sid):(OI)(CI)F" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'Could not grant owner access to Runner secret directory.' }
  & icacls.exe $store '/inheritance:r' | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'Could not restrict Runner secret directory inheritance.' }
}

function Secret-Path([string]$name) {
  if ($name -notin @('daily-system','zj','disposable-test')) { throw 'Unknown secret record.' }
  return Join-Path $store "$name.dpapi"
}

function Save-Secret([string]$name, [string]$value) {
  Protect-Directory
  Add-Type -AssemblyName System.Security
  $clear = [System.Text.Encoding]::UTF8.GetBytes($value)
  try { $ciphertext = [System.Security.Cryptography.ProtectedData]::Protect($clear, $null, [System.Security.Cryptography.DataProtectionScope]::CurrentUser) }
  finally { [Array]::Clear($clear, 0, $clear.Length) }
  $path = Secret-Path $name
  [System.IO.File]::WriteAllBytes($path, $ciphertext)
  $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
  & icacls.exe $path '/grant:r' "*$($sid):F" | Out-Null
  & icacls.exe $path '/inheritance:r' | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'Could not restrict Runner secret file ACL.' }
}

function Read-Secret([string]$name) {
  $path = Secret-Path $name
  Add-Type -AssemblyName System.Security
  $ciphertext = [System.IO.File]::ReadAllBytes($path)
  $clear = [System.Security.Cryptography.ProtectedData]::Unprotect($ciphertext, $null, [System.Security.Cryptography.DataProtectionScope]::CurrentUser)
  try { return [System.Text.Encoding]::UTF8.GetString($clear) }
  finally { [Array]::Clear($clear, 0, $clear.Length) }
}

function New-Token {
  $bytes = [byte[]]::new(32)
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  try { return [Convert]::ToHexString($bytes).ToLowerInvariant() }
  finally { [Array]::Clear($bytes, 0, $bytes.Length) }
}

function Invoke-WranglerSecret([string]$json) {
  $bin = (Get-Command node.exe -ErrorAction Stop).Source
  $wrangler = Join-Path $repo 'node_modules\wrangler\bin\wrangler.js'
  if (-not (Test-Path -LiteralPath $wrangler)) { throw 'Local Wrangler installation missing.' }
  $start = [System.Diagnostics.ProcessStartInfo]::new($bin)
  $start.WorkingDirectory = $repo
  $start.ArgumentList.Add($wrangler)
  $start.ArgumentList.Add('secret')
  $start.ArgumentList.Add('put')
  $start.ArgumentList.Add('PG_RUNNER_TOKENS_JSON')
  $start.RedirectStandardInput = $true
  $start.RedirectStandardOutput = $true
  $start.RedirectStandardError = $true
  $start.UseShellExecute = $false
  $start.CreateNoWindow = $true
  $proc = [System.Diagnostics.Process]::Start($start)
  try {
    $stdout = $proc.StandardOutput.ReadToEndAsync()
    $stderr = $proc.StandardError.ReadToEndAsync()
    $proc.StandardInput.Write($json)
    $proc.StandardInput.Close()
    $proc.WaitForExit()
    [void]$stdout.Result; [void]$stderr.Result
    if ($proc.ExitCode -ne 0) { throw 'Runner secret upload failed. No Runner was started.' }
  } finally { $proc.Dispose() }
}

switch ($Mode) {
  'TestStore' {
    $token = New-Token
    try {
      Save-Secret 'disposable-test' $token
      $blob = [System.IO.File]::ReadAllBytes((Secret-Path 'disposable-test'))
      $ciphertext = [System.Text.Encoding]::UTF8.GetString($blob)
      if ($ciphertext.Contains($token)) { throw 'Disposable secret is visible in the encrypted record.' }
      $wrongContextFailed = $false
      try { [void][System.Security.Cryptography.ProtectedData]::Unprotect($blob, [byte[]](1,2,3), [System.Security.Cryptography.DataProtectionScope]::CurrentUser) }
      catch { $wrongContextFailed = $true }
      if (-not $wrongContextFailed) { throw 'DPAPI accepted the wrong decryption context.' }
      $start = [System.Diagnostics.ProcessStartInfo]::new((Get-Command pwsh.exe).Source)
      foreach ($arg in @('-NoProfile','-File',$PSCommandPath,'-Mode','TestRead')) { $start.ArgumentList.Add($arg) }
      $start.Environment['PJ_DISPOSABLE_EXPECTED_HASH'] = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($token)))
      $start.RedirectStandardOutput = $true
      $start.RedirectStandardError = $true
      $start.UseShellExecute = $false
      $start.CreateNoWindow = $true
      $proc = [System.Diagnostics.Process]::Start($start)
      try {
        $out = $proc.StandardOutput.ReadToEnd()
        $err = $proc.StandardError.ReadToEnd()
        $proc.WaitForExit()
        if ($proc.ExitCode -ne 0 -or $out.Trim() -ne 'DISPOSABLE_DPAPI_REOPEN_PASS') { throw 'Fresh-process DPAPI test failed.' }
      } finally { $proc.Dispose() }
      Write-Output 'DISPOSABLE_DPAPI_REOPEN_PASS'
    } finally {
      Remove-Item -LiteralPath (Secret-Path 'disposable-test') -ErrorAction SilentlyContinue
    }
  }
  'TestRead' {
    $value = Read-Secret 'disposable-test'
    $hash = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($value)))
    if ($hash -ne $env:PJ_DISPOSABLE_EXPECTED_HASH) { throw 'Disposable secret mismatch.' }
    Write-Output 'DISPOSABLE_DPAPI_REOPEN_PASS'
  }
  'Rotate' {
    $daily = New-Token
    $zj = New-Token
    if ($daily -eq $zj) { throw 'Runner token collision.' }
    Save-Secret 'daily-system' $daily
    Save-Secret 'zj' $zj
    if ((Read-Secret 'daily-system') -ne $daily -or (Read-Secret 'zj') -ne $zj) { throw 'Durable secret verification failed.' }
    $map = @{ 'windows-runner' = @{ token = $daily; project = 'daily-system' }; 'zj-runner' = @{ token = $zj; project = 'zj' } }
    Invoke-WranglerSecret (ConvertTo-Json -InputObject $map -Depth 4 -Compress)
    Write-Output 'COORDINATED_ROTATION_PASS'
  }
  'Run' {
    $statusPath = Join-Path $store "$Project.startup-status"
    [System.IO.File]::WriteAllText($statusPath, 'BOOTSTRAP_ENTERED')
    $env:PG_CONTROL_PLANE_URL = $endpoint
    $env:PG_RUNNER_ID = $ids[$Project]
    $env:PG_PROJECT_ID = $Project
    try { $env:PG_RUNNER_TOKEN = Read-Secret $Project }
    catch {
      [System.IO.File]::WriteAllText($statusPath, "CREDENTIAL_LOAD_FAILED $($_.Exception.GetType().Name) line=$($_.InvocationInfo.ScriptLineNumber)")
      throw 'Runner credential could not be loaded.'
    }
    [System.IO.File]::WriteAllText($statusPath, 'CREDENTIAL_LOADED')
    if ($Project -eq 'zj') {
      $env:ZJ_REPO_PATH = 'C:\Users\Amir\Documents\GitHub\ZJ'
      $env:ZJ_PLANS_PATH = 'C:\Users\Amir\Documents\GitHub\Plans\ZJ'
    } else { $env:DAILYSYSTEM_REPO_PATH = 'C:\Users\Amir\Documents\GitHub\DailySystem' }
    try {
      Push-Location $repo
      [System.IO.File]::WriteAllText($statusPath, 'RUNNER_STARTED')
      & (Get-Command node.exe).Source (Join-Path $repo 'src\runner\index.js')
      if ($LASTEXITCODE -ne 0) { throw 'Runner exited with an error.' }
    } finally {
      [System.IO.File]::WriteAllText($statusPath, 'RUNNER_EXITED')
      Pop-Location
      Remove-Item Env:PG_RUNNER_TOKEN -ErrorAction SilentlyContinue
    }
  }
  'Start' {
    $existing = @(Get-CimInstance Win32_Process | Where-Object {
      $_.Name -eq 'powershell.exe' -and $_.CommandLine -like '*runner-durable.ps1*' -and
      $_.CommandLine -like '*-Mode Run*' -and $_.CommandLine -like "*-Project $Project*"
    })
    if ($existing.Count -gt 0) { throw "Runner $Project is already started." }
    $exe = Join-Path $env:WINDIR 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $args = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Mode Run -Project $Project"
    $proc = Start-Process -FilePath $exe -ArgumentList $args -WorkingDirectory $repo -WindowStyle Hidden -PassThru
    Write-Output "RUNNER_BOOTSTRAP_STARTED project=$Project pid=$($proc.Id)"
  }
  'InstallStartup' {
    Assert-WindowsOwner
    $exe = Join-Path $env:WINDIR 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
    foreach ($scope in @('daily-system','zj')) {
      $name = "ProjectManagerRunner-$scope"
      $command = "`"$exe`" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$PSCommandPath`" -Mode Run -Project $scope"
      New-ItemProperty -Path $key -Name $name -Value $command -PropertyType String -Force | Out-Null
      Write-Output "LOGON_STARTUP_REGISTERED $name"
    }
  }
  'Status' {
    foreach ($scope in @('daily-system','zj')) {
      $key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
      $name = "ProjectManagerRunner-$scope"
      $entry = (Get-ItemProperty -Path $key -Name $name -ErrorAction SilentlyContinue).$name
      Write-Output "$scope logonStartup=$([bool]$entry) secretRecord=$([bool](Test-Path -LiteralPath (Secret-Path $scope)))"
    }
  }
  'Probe' {
    $token = Read-Secret $Project
    $headers = @{ authorization = "Bearer $token" }
    $body = @{ projectId = $Project; type = $JobType; payload = @{} } | ConvertTo-Json -Depth 4 -Compress
    $created = Invoke-RestMethod -Uri "$endpoint/api/v1/jobs" -Method Post -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec 70
    if (-not $created.ok -or -not $created.job.id) { throw 'Job creation failed.' }
    $jobId = $created.job.id
    Write-Output "JOB_CREATED project=$Project type=$JobType id=$jobId"
    $deadline = [DateTime]::UtcNow.AddMinutes(12)
    do {
      Start-Sleep -Seconds 5
      $response = Invoke-RestMethod -Uri "$endpoint/api/v1/jobs/$jobId" -Method Get -Headers $headers -TimeoutSec 70
      $job = $response.job
    } while ($job.status -notin @('SUCCEEDED','FAILED','CANCELLED') -and [DateTime]::UtcNow -lt $deadline)
    Write-Output "JOB_RESULT project=$Project type=$JobType id=$jobId status=$($job.status) runner=$($job.leaseOwner) attempts=$($job.attemptCount)"
    if ($job.status -ne 'SUCCEEDED') { throw 'Safe Runner probe did not succeed.' }
  }
  'ProbeScope' {
    $token = Read-Secret $Project
    $target = if ($Project -eq 'zj') { 'daily-system' } else { 'zj' }
    $job = if ($target -eq 'zj') { 'ZJ_REPO_STATUS' } else { 'HEALTHCHECK' }
    $body = @{ projectId = $target; type = $job; payload = @{} } | ConvertTo-Json -Depth 4 -Compress
    $reply = Invoke-WebRequest -Uri "$endpoint/api/v1/jobs" -Method Post -Headers @{ authorization = "Bearer $token" } -ContentType 'application/json' -Body $body -SkipHttpErrorCheck -TimeoutSec 70
    $errorCode = ($reply.Content | ConvertFrom-Json).error
    if ($reply.StatusCode -ne 403 -or $errorCode -ne 'project_scope_denied') { throw 'Cross-project request was not rejected as expected.' }
    Write-Output "SCOPE_DENIED source=$Project target=$target status=403"
  }
  'Audit' {
    $tokens = @((Read-Secret 'daily-system'), (Read-Secret 'zj'))
    $repoFound = $false
    $processFound = $false
    $localFound = $false
    Push-Location $repo
    try {
      $paths = @(& git ls-files) + @(& git ls-files --others --exclude-standard)
      foreach ($relative in $paths) {
        $full = Join-Path $repo $relative
        if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { continue }
        try {
          $content = [System.IO.File]::ReadAllText($full)
          foreach ($token in $tokens) { if ($content.Contains($token)) { $repoFound = $true } }
        } catch { }
      }
    } finally { Pop-Location }
    foreach ($proc in @(Get-CimInstance Win32_Process)) {
      foreach ($token in $tokens) { if ($proc.CommandLine -and $proc.CommandLine.Contains($token)) { $processFound = $true } }
    }
    $key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
    foreach ($scope in @('daily-system','zj')) {
      $name = "ProjectManagerRunner-$scope"
      $entry = (Get-ItemProperty -Path $key -Name $name -ErrorAction SilentlyContinue).$name
      foreach ($token in $tokens) { if ($entry -and $entry.Contains($token)) { $localFound = $true } }
    }
    foreach ($file in @(Get-ChildItem -LiteralPath $store -File)) {
      $content = [System.Text.Encoding]::UTF8.GetString([System.IO.File]::ReadAllBytes($file.FullName))
      foreach ($token in $tokens) { if ($content.Contains($token)) { $localFound = $true } }
    }
    Write-Output "SECRETS_IN_REPO=$(if ($repoFound) {'YES'} else {'NO'})"
    Write-Output "SECRETS_IN_PROCESS_COMMANDLINE=$(if ($processFound) {'YES'} else {'NO'})"
    Write-Output "PLAINTEXT_SECRET_FILES_OR_STARTUP=$(if ($localFound) {'YES'} else {'NO'})"
    if ($repoFound -or $processFound -or $localFound) { throw 'Runner credential exposure detected; rotate both credentials.' }
  }
}
