<#
  OpenSAI fast regression suite (entry point).
  Ensures Redis is up, runs the QUIC transport unit test, then the integration
  orchestrator (scripts/test_suite.py, which starts/stops the stack itself).

  Usage:
    powershell -ExecutionPolicy Bypass -File scripts\test_suite.ps1
  Exit 0 only if everything passes.  ~25-30s.
#>
$root = Split-Path $PSScriptRoot -Parent
$vpy  = Join-Path $root '.venv\Scripts\python.exe'
$cli  = Join-Path $env:LOCALAPPDATA 'opensai-redis\redis-cli.exe'
$redisExe = Join-Path $env:LOCALAPPDATA 'opensai-redis\redis-server.exe'
$fail = 0

# ensure redis
& $cli ping 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Start-Process -FilePath $redisExe -ArgumentList '--port 6379 --save "" --appendonly no' -WindowStyle Hidden
    Start-Sleep 2
}

# 1) QUIC transport unit test (standalone)
Write-Host "[1] QUIC transport unit test..." -ForegroundColor Cyan
& $vpy (Join-Path $PSScriptRoot 'test_dataplane.py') *> "$root\.run\suite_unit.log"
if ($LASTEXITCODE -eq 0) { Write-Host "    unit PASS" -ForegroundColor Green }
else { Write-Host "    unit FAIL (see .run\suite_unit.log)" -ForegroundColor Red; $fail = 1 }

# 2) integration scenarios (self-contained python orchestrator: start stack -> run -> teardown)
Write-Host "[2] integration scenarios..." -ForegroundColor Cyan
& $vpy (Join-Path $PSScriptRoot 'test_suite.py')
if ($LASTEXITCODE -ne 0) { $fail = 1 }

Write-Host ""
if ($fail -eq 0) { Write-Host "==== ALL TESTS PASSED ====" -ForegroundColor Green; exit 0 }
else { Write-Host "==== SOME TESTS FAILED ====" -ForegroundColor Red; exit 1 }
