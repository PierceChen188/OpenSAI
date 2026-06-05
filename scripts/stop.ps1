<#
  Stop the OpenSAI stack.

  Usage:
    powershell -ExecutionPolicy Bypass -File scripts\stop.ps1        # stop server/device/user (leave Redis up)
    powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All   # also stop Redis
#>
param([switch]$All)

$redisDir = Join-Path $env:LOCALAPPDATA 'opensai-redis'

function Stop-OpenSaiPython([string]$pattern) {
    $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like "*$pattern*" }
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "stopped $pattern (PID $($p.ProcessId))" -ForegroundColor Yellow
    }
}

Stop-OpenSaiPython 'dag_offload\server.py'
Stop-OpenSaiPython 'device_client.py'
Stop-OpenSaiPython 'user_client.py'
Stop-OpenSaiPython 'inject_dag.py'

if ($All) {
    Get-Process redis-server -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "stopped redis-server" -ForegroundColor Yellow
}

Write-Host "Done." -ForegroundColor Green
