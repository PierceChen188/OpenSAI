<#
  Run the end-to-end smoke test against a running stack (scripts\run.ps1 first).
  Exit code 0 = PASS, non-zero = FAIL.

  Usage:
    powershell -ExecutionPolicy Bypass -File scripts\smoke_test.ps1
#>
$root = Split-Path $PSScriptRoot -Parent
$vpy  = Join-Path $root '.venv\Scripts\python.exe'

if (-not (Test-Path $vpy)) { Write-Host "venv missing — run scripts\setup.ps1 first." -ForegroundColor Red; exit 1 }

& $vpy (Join-Path $PSScriptRoot 'inject_dag.py')
$code = $LASTEXITCODE
if ($code -eq 0) { Write-Host "`nSMOKE TEST: PASS" -ForegroundColor Green }
else             { Write-Host "`nSMOKE TEST: FAIL (exit $code)" -ForegroundColor Red }
exit $code
