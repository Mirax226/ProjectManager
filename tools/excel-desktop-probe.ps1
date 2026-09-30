param([switch]$CleanupOnly, [switch]$PrepareOnly)
$ErrorActionPreference = 'Stop'
$statePath = $env:PJ_EXCEL_PROBE_STATE

function Cleanup-Owned($state) {
  $remaining = @()
  foreach ($owned in @($state.owned)) {
    $process = Get-Process -Id $owned.id -ErrorAction SilentlyContinue
    if ($process -and $process.ProcessName -eq 'EXCEL' -and
        $process.StartTime.ToUniversalTime().Ticks.ToString() -eq [string]$owned.startTicks -and
        @($state.before) -notcontains $process.Id) {
      # ID + creation time + HWND identity establish ownership; never kill a
      # process merely because it appeared after the baseline snapshot.
      try { Stop-Process -Id $process.Id -Force -ErrorAction Stop; $process.WaitForExit(5000) | Out-Null } catch {}
      if (Get-Process -Id $process.Id -ErrorAction SilentlyContinue) { $remaining += $process.Id }
    }
  }
  $newIds = @(Get-Process -Name EXCEL -ErrorAction SilentlyContinue | Where-Object { @($state.before) -notcontains $_.Id } | Select-Object -ExpandProperty Id)
  $noNewObserved = @($state.owned).Count -eq 0 -and $newIds.Count -eq 0
  $unverified = @()
  if (@($state.owned).Count -eq 0) { $unverified=@($newIds) }
  [pscustomobject]@{ cleanupVerified=($remaining.Count -eq 0 -and (@($state.owned).Count -gt 0 -or $noNewObserved)); noNewExcelProcessesObserved=$noNewObserved; remainingOwnedProcessIds=@($remaining); unverifiedNewExcelProcessIds=@($unverified) }
}

if ($CleanupOnly) {
  if (Test-Path -LiteralPath $statePath) {
    $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    Cleanup-Owned $state | ConvertTo-Json -Compress
  } else { [pscustomobject]@{ cleanupVerified=$false; remainingOwnedProcessIds=@() } | ConvertTo-Json -Compress }
  exit
}

Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class PjExcelWindow { [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hwnd, out uint processId); }'
$before = @(Get-Process -Name EXCEL -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
$excel = $null; $book = $null; $sheets = $null; $seedBook = $null
$sharedInstance = $false
$quitRequested = $false
$state = @{ stage='COM_CREATE'; stages=@(); before=$before; owned=@(); ownerProcessId=$PID }
$started = [Diagnostics.Stopwatch]::StartNew()
function Set-Stage([string]$stage) {
  $state.stage = $stage; $state.stages += $stage
  $state | ConvertTo-Json -Depth 4 -Compress | Set-Content -LiteralPath "$statePath.tmp" -Encoding UTF8
  Move-Item -LiteralPath "$statePath.tmp" -Destination $statePath -Force
}
$result = $null
try {
  $refreshFlagsCleared = 0; $queryTablesDisabled = 0; $vbaUnchanged = $null
  if ($env:PJ_EXCEL_SUPPRESS_REFRESH -eq 'true') {
    Set-Stage 'COPY_CONFIGURE'
    $copyPath = [IO.Path]::GetFullPath($env:PJ_EXCEL_PROBE_PATH)
    if ((Split-Path (Split-Path $copyPath -Parent) -Leaf) -notlike 'pj-excel-probe-*') { throw 'Refresh suppression requires a disposable probe copy' }
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [IO.Compression.ZipFile]::Open($copyPath, [IO.Compression.ZipArchiveMode]::Update)
    function Vba-Hash($zip) {
      $entry = $zip.GetEntry('xl/vbaProject.bin')
      if (-not $entry) { return $null }
      $stream=$entry.Open(); $hash=[Security.Cryptography.SHA256]::Create()
      try { return [BitConverter]::ToString($hash.ComputeHash($stream)) } finally { $stream.Dispose(); $hash.Dispose() }
    }
    try {
      $vbaBefore = Vba-Hash $archive
      foreach ($entry in @($archive.Entries | Where-Object { $_.FullName -eq 'xl/connections.xml' -or $_.FullName -like 'xl/queryTables/*.xml' })) {
        $reader=[IO.StreamReader]::new($entry.Open())
        try { $xml=[xml]$reader.ReadToEnd() } finally { $reader.Dispose() }
        $nodes=@($xml.SelectNodes('//*[@refreshOnLoad="1" or @refreshOnLoad="true"]'))
        $queryTable = $entry.FullName -like 'xl/queryTables/*.xml'
        if ($queryTable) { $xml.DocumentElement.SetAttribute('disableRefresh', '1'); $queryTablesDisabled++ }
        if ($nodes.Count -or $queryTable) {
          foreach ($element in $nodes) { $element.SetAttribute('refreshOnLoad', '0') }
          $entryName=$entry.FullName; $entry.Delete()
          $replacement=$archive.CreateEntry($entryName)
          $stream=$replacement.Open()
          try { $xml.Save($stream) } finally { $stream.Dispose() }
          $refreshFlagsCleared += $nodes.Count
        }
      }
      $vbaUnchanged = (Vba-Hash $archive) -eq $vbaBefore
      if (-not $vbaUnchanged) { throw 'Disposable VBA integrity check failed' }
    } finally { $archive.Dispose() }
  }
  $state.externalRefreshSuppressed=($env:PJ_EXCEL_SUPPRESS_REFRESH -eq 'true')
  $state.refreshFlagsCleared=$refreshFlagsCleared; $state.queryTablesDisabled=$queryTablesDisabled; $state.vbaProjectUnchanged=$vbaUnchanged
  if ($PrepareOnly) { [pscustomobject]@{ refreshFlagsCleared=$refreshFlagsCleared; vbaProjectUnchanged=$vbaUnchanged } | ConvertTo-Json -Compress; return }
  Set-Stage 'COM_CREATE'
  $excel = New-Object -ComObject Excel.Application
  [uint32]$excelPid = 0
  [PjExcelWindow]::GetWindowThreadProcessId([IntPtr]$excel.Hwnd, [ref]$excelPid) | Out-Null
  $process = Get-Process -Id $excelPid
  if ($before -contains $process.Id) { $sharedInstance=$true; throw 'COM activation reused a pre-existing Excel process' }
  if ($before -notcontains $process.Id) {
    $state.owned = @(@{ id=$process.Id; startTicks=$process.StartTime.ToUniversalTime().Ticks.ToString() })
  }
  Set-Stage 'EXCEL_CONFIGURE'
  $excel.Visible=$false; $excel.DisplayAlerts=$false; $excel.AskToUpdateLinks=$false
  $excel.EnableEvents=$false; $excel.AutomationSecurity=3
  # Calculation mode is inherited from the first workbook in an Excel instance.
  # Keep an unsaved blank workbook open so cached-read diagnostics cannot trigger
  # automatic calculation in the business workbook as it opens.
  $seedBooks=$excel.Workbooks
  try { $seedBook=$seedBooks.Add() } finally { [Runtime.InteropServices.Marshal]::FinalReleaseComObject($seedBooks) | Out-Null }
  $excel.Calculation=-4135; $excel.CalculateBeforeSave=$false
  $state.calculationDisabled=$true
  Set-Stage 'WORKBOOK_OPEN_START'
  $books = $excel.Workbooks
  try { $book = $books.Open($env:PJ_EXCEL_PROBE_PATH, 0, $true) }
  finally { [Runtime.InteropServices.Marshal]::FinalReleaseComObject($books) | Out-Null }
  Set-Stage 'WORKBOOK_OPEN_SUCCESS'
  Set-Stage 'WORKBOOK_READ_PROBE'
  $sheets = $book.Worksheets
  if ($sheets.Count -lt 1) { throw 'No worksheet available' }
  [Runtime.InteropServices.Marshal]::FinalReleaseComObject($sheets) | Out-Null; $sheets=$null
  $version = [string]$excel.Version
  $state.excelVersion=$version
  Set-Stage 'WORKBOOK_CLOSE'
  $book.Close($false)
  [Runtime.InteropServices.Marshal]::FinalReleaseComObject($book) | Out-Null; $book=$null
  $seedBook.Close($false)
  [Runtime.InteropServices.Marshal]::FinalReleaseComObject($seedBook) | Out-Null; $seedBook=$null
  Set-Stage 'EXCEL_QUIT'
  $quitRequested=$true
  $excel.Quit()
  $result = @{ ok=$true; version=$version }
} catch {
  $codes = @{ COPY_CONFIGURE='COPY_CONFIGURE_FAILED'; COM_CREATE='COM_CREATE_FAILED'; EXCEL_CONFIGURE='EXCEL_CONFIGURATION_FAILED'; WORKBOOK_OPEN_START='WORKBOOK_OPEN_FAILED'; WORKBOOK_READ_PROBE='WORKBOOK_READ_FAILED'; WORKBOOK_CLOSE='WORKBOOK_CLOSE_FAILED'; EXCEL_QUIT='EXCEL_QUIT_FAILED' }
  $result = @{ ok=$false; diagnosticCode=$codes[$state.stage]; failedStage=$state.stage; safeError=@{ exceptionClass=$_.Exception.GetType().Name; hresult=('0x' + $_.Exception.HResult.ToString('X8')); message=('Excel diagnostic failed at ' + $state.stage); scriptLine=$_.InvocationInfo.ScriptLineNumber } }
} finally {
  if ($sheets) { try { [Runtime.InteropServices.Marshal]::FinalReleaseComObject($sheets) | Out-Null } catch {} }
  if ($book) { try { $book.Close($false) } catch {}; try { [Runtime.InteropServices.Marshal]::FinalReleaseComObject($book) | Out-Null } catch {} }
  if ($seedBook) { try { $seedBook.Close($false) } catch {}; try { [Runtime.InteropServices.Marshal]::FinalReleaseComObject($seedBook) | Out-Null } catch {} }
  if ($excel) { if (-not $sharedInstance -and -not $quitRequested) { try { $excel.Quit() } catch {} }; try { [Runtime.InteropServices.Marshal]::FinalReleaseComObject($excel) | Out-Null } catch {} }
  [GC]::Collect(); [GC]::WaitForPendingFinalizers()
  $started.Stop()
}
$result.stages=@($state.stages); $result.stage=$state.stage
$result.ownedProcessIds=@($state.owned | ForEach-Object { $_.id })
$result.durationMs=$started.ElapsedMilliseconds
$result.externalRefreshSuppressed=($env:PJ_EXCEL_SUPPRESS_REFRESH -eq 'true')
$result.refreshFlagsCleared=$refreshFlagsCleared
$result.queryTablesDisabled=$queryTablesDisabled
$result.vbaProjectUnchanged=$vbaUnchanged
$result.calculationDisabled=$state.calculationDisabled
$result | ConvertTo-Json -Depth 4 -Compress
