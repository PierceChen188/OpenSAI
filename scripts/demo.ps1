<#
  One-click demo: start the whole stack (Redis + server + 4 devices + web bridge)
  with relay-multipath enabled, then open the browser to the auto-demo URL. The
  frontend auto-submits a complex DAG once the cluster is online, so you can watch
  smart placement / resource allocation / relay multipath in the 3D window.

  Usage:  double-click start-demo.bat   OR
          powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 [-NoBrowser]
#>
param([switch]$NoBrowser)

$ErrorActionPreference = 'Stop'
$root  = Split-Path $PSScriptRoot -Parent
$vpy   = Join-Path $root '.venv\Scripts\python.exe'
$logs  = Join-Path $root '.run'
$redisDir = Join-Path $env:LOCALAPPDATA 'opensai-redis'
$redisExe = Join-Path $redisDir 'redis-server.exe'
$redisCli = Join-Path $redisDir 'redis-cli.exe'

if (-not (Test-Path $vpy))      { Write-Host "venv missing - run scripts\setup.ps1 first" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $redisExe)) { Write-Host "Redis missing - run scripts\setup.ps1 first" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $logs))     { New-Item -ItemType Directory -Path $logs | Out-Null }

function Stop-Py([string]$pattern) {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like "*$pattern*" } |
        ForEach-Object { cmd /c "taskkill /F /T /PID $($_.ProcessId) >nul 2>nul" }
}
function Start-Py([string]$script, [string]$tag) {
    (Start-Process -FilePath $vpy -ArgumentList "Offloading-System\dag_offload\$script" `
        -WorkingDirectory $root `
        -RedirectStandardOutput (Join-Path $logs "$tag.out.log") `
        -RedirectStandardError  (Join-Path $logs "$tag.err.log") `
        -WindowStyle Hidden -PassThru).Id
}

# 1) Redis ---------------------------------------------------------------
& $redisCli ping 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Starting Redis..." -ForegroundColor Yellow
    Start-Process -FilePath $redisExe -ArgumentList '--port 6379 --save "" --appendonly no' -WindowStyle Hidden
    Start-Sleep -Seconds 2
}
if ((& $redisCli ping 2>$null) -ne 'PONG') { Write-Host "Redis not responding on 6379" -ForegroundColor Red; exit 1 }
& $redisCli flushall | Out-Null
Write-Host "Redis: PONG" -ForegroundColor Green

# 2) stop stale processes ------------------------------------------------
Write-Host "Stopping stale processes..." -ForegroundColor Yellow
Stop-Py 'server.py'; Stop-Py 'run_device.py'; Stop-Py 'device_client.py'; Stop-Py 'user_client.py'
Start-Sleep -Seconds 2
for ($i = 0; $i -lt 10; $i++) {
    $owner = (Get-NetTCPConnection -LocalPort 8888 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1).OwningProcess
    if (-not $owner) { break }
    Stop-Process -Id $owner -Force -ErrorAction SilentlyContinue; Start-Sleep -Seconds 1
}

# 3) server (relay multipath + auto resource tiers + per-path rate cap) ---
$env:OPENSAI_PLACEMENT        = 'plan'
$env:OPENSAI_MULTIPATH        = '1'
$env:OPENSAI_MULTIPATH_GAIN   = '0.5'
$env:OPENSAI_MULTIPATH_MIN_MB = '1'
$env:OPENSAI_MULTINIC         = '0'
$env:OPENSAI_TARGET_XFER_S    = '6'
$env:OPENSAI_AUTO_MEM         = '1'
$env:OPENSAI_AUTO_STORAGE     = '1'
$srvPid = Start-Py 'server.py' 'server'
Write-Host "server.py     -> PID $srvPid (placement=plan, relay-multipath=on)" -ForegroundColor Green
Start-Sleep -Seconds 3

# 4) 3 heterogeneous devices (strong/medium/weak) — 3 is the reliable spawn zone
#    on Windows and is enough for relay multipath (source + consumer + relay).
$specs = @(@{port=9971;workers=3}, @{port=9972;workers=2}, @{port=9973;workers=1})
$env:OPENSAI_PROBE_INTERVAL = '600'
$env:OPENSAI_POOL_WARMUP_S  = '45'
foreach ($s in $specs) {
    $env:OPENSAI_DATA_PORT    = "$($s.port)"
    $env:OPENSAI_TASK_WORKERS = "$($s.workers)"
    $dpid = Start-Py 'run_device.py' "dev$($s.port)"
    Write-Host "device :$($s.port) (workers=$($s.workers)) -> PID $dpid" -ForegroundColor Green
    Start-Sleep -Seconds 3
}

# 5) web bridge ----------------------------------------------------------
$webPid = Start-Py 'user_client.py' 'user'
Write-Host "user_client   -> PID $webPid (web UI :5000)" -ForegroundColor Green

# 6) wait for devices online + bridge ready ------------------------------
Write-Host "Waiting for cluster to come online..." -ForegroundColor Yellow
$n = 0; $web = 0
for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 2
    $n = [int](& $redisCli scard DEVICE_ONLINE_SET)
    $web = (Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue | Measure-Object).Count
    if (($n -ge 3) -and ($web -ge 1)) { break }
}
$webState = 'not-ready'; if ($web -ge 1) { $webState = 'ready' }
Write-Host "Online devices: $n / 3 ; bridge :5000 = $webState" -ForegroundColor Cyan

# 7) open the auto-demo URL (frontend auto-submits the complex DAG) -------
$demoUrl = 'http://127.0.0.1:5000/?demo=1'
if (-not $NoBrowser) { Start-Process $demoUrl }
Write-Host ""
Write-Host "Demo ready -> $demoUrl" -ForegroundColor Green
Write-Host "  The frontend auto-submits the demo task; watch the 3D stage on the Overview page" -ForegroundColor Gray
Write-Host "  and the step-by-step Placement/Resource/Transfer decision pages. Re-run via the top-right button." -ForegroundColor Gray
Write-Host "Stop all:  powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All" -ForegroundColor Gray
