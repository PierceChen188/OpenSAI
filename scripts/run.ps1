<#
  Start the OpenSAI stack: Redis + server.py + device_client.py (+ optional web UI).
  Idempotent: stops any previously-started server/device first to avoid the
  Windows SO_REUSEADDR double-bind on port 8888.

  Usage:
    powershell -ExecutionPolicy Bypass -File scripts\run.ps1            # server + 1 device
    powershell -ExecutionPolicy Bypass -File scripts\run.ps1 -WithWeb  # also start user_client.py (Flask, :5000)
#>
param([switch]$WithWeb)

$ErrorActionPreference = 'Stop'
$root  = Split-Path $PSScriptRoot -Parent
$vpy   = Join-Path $root '.venv\Scripts\python.exe'
$logs  = Join-Path $root '.run'
$redisDir = Join-Path $env:LOCALAPPDATA 'opensai-redis'
$redisExe = Join-Path $redisDir 'redis-server.exe'
$redisCli = Join-Path $redisDir 'redis-cli.exe'

if (-not (Test-Path $vpy))      { Write-Host "venv missing — run scripts\setup.ps1 first." -ForegroundColor Red; exit 1 }
if (-not (Test-Path $redisExe)) { Write-Host "Redis missing — run scripts\setup.ps1 first." -ForegroundColor Red; exit 1 }
if (-not (Test-Path $logs))     { New-Item -ItemType Directory -Path $logs | Out-Null }

function Stop-OpenSaiPython([string]$pattern) {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like "*$pattern*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

function Start-OpenSaiPython([string]$script, [string]$tag) {
    $p = Start-Process -FilePath $vpy -ArgumentList "Offloading-System\dag_offload\$script" `
        -WorkingDirectory $root `
        -RedirectStandardOutput (Join-Path $logs "$tag.out.log") `
        -RedirectStandardError  (Join-Path $logs "$tag.err.log") `
        -WindowStyle Hidden -PassThru
    return $p.Id
}

# 1) Redis ---------------------------------------------------------------
& $redisCli ping 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Starting Redis..." -ForegroundColor Yellow
    Start-Process -FilePath $redisExe -ArgumentList '--port 6379 --save "" --appendonly no' -WindowStyle Hidden
    Start-Sleep -Seconds 2
}
if ((& $redisCli ping 2>$null) -ne 'PONG') { Write-Host "Redis not responding on 6379." -ForegroundColor Red; exit 1 }
Write-Host "Redis: PONG" -ForegroundColor Green

# 2) (re)start server + device ------------------------------------------
Write-Host "Stopping any existing server/device..." -ForegroundColor Yellow
Stop-OpenSaiPython 'server.py'
Stop-OpenSaiPython 'device_client.py'
if ($WithWeb) { Stop-OpenSaiPython 'user_client.py' }
Start-Sleep -Seconds 2

$srvPid = Start-OpenSaiPython 'server.py' 'server'
Write-Host "server.py     -> PID $srvPid" -ForegroundColor Green
Start-Sleep -Seconds 3
$devPid = Start-OpenSaiPython 'device_client.py' 'device'
Write-Host "device_client -> PID $devPid" -ForegroundColor Green
Start-Sleep -Seconds 5

if ($WithWeb) {
    $webPid = Start-OpenSaiPython 'user_client.py' 'user'
    Write-Host "user_client   -> PID $webPid  (web UI at http://127.0.0.1:5000)" -ForegroundColor Green
}

# 3) status --------------------------------------------------------------
$devCount = & $redisCli scard DEVICE_ONLINE_SET
Write-Host ""
Write-Host "Devices registered in Redis: $devCount" -ForegroundColor Cyan
Write-Host "Logs: $logs\{server,device$(if($WithWeb){',user'})}.{out,err}.log"
Write-Host "Verify end-to-end:  powershell -ExecutionPolicy Bypass -File scripts\smoke_test.ps1"
Write-Host "Stop everything:    powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All"
