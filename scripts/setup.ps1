<#
  OpenSAI one-time setup (Windows / PowerShell).
  Creates the venv, installs core deps, and fetches a portable Redis server.

  Usage:
    powershell -ExecutionPolicy Bypass -File scripts\setup.ps1          # core only
    powershell -ExecutionPolicy Bypass -File scripts\setup.ps1 -Ml      # also install torch/torchvision (large)
#>
param([switch]$Ml)

$ErrorActionPreference = 'Stop'
$root  = Split-Path $PSScriptRoot -Parent
$venv  = Join-Path $root '.venv'
$vpy   = Join-Path $venv 'Scripts\python.exe'
$redisDir = Join-Path $env:LOCALAPPDATA 'opensai-redis'

Write-Host "OpenSAI setup — repo root: $root" -ForegroundColor Cyan

# 1) venv ---------------------------------------------------------------
if (-not (Test-Path $vpy)) {
    Write-Host "[1/3] Creating virtual environment (.venv)..." -ForegroundColor Yellow
    python -m venv $venv
} else {
    Write-Host "[1/3] venv already exists." -ForegroundColor Green
}

# 2) deps ---------------------------------------------------------------
Write-Host "[2/3] Installing core dependencies..." -ForegroundColor Yellow
& $vpy -m pip install -r (Join-Path $root 'Offloading-System\requirements.txt')
if ($Ml) {
    Write-Host "      Installing optional ML dependencies (torch, torchvision)..." -ForegroundColor Yellow
    & $vpy -m pip install -r (Join-Path $root 'Offloading-System\requirements-ml.txt')
}

# 3) portable Redis -----------------------------------------------------
$redisExe = Join-Path $redisDir 'redis-server.exe'
if (-not (Test-Path $redisExe)) {
    Write-Host "[3/3] Fetching portable Redis for Windows..." -ForegroundColor Yellow
    if (-not (Test-Path $redisDir)) { New-Item -ItemType Directory -Path $redisDir | Out-Null }
    $zip = Join-Path $redisDir 'redis.zip'
    $url = 'https://github.com/tporadowski/redis/releases/download/v5.0.14.1/Redis-x64-5.0.14.1.zip'
    Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing -TimeoutSec 180
    Expand-Archive -Path $zip -DestinationPath $redisDir -Force
    Remove-Item $zip -Force
    Write-Host "      Redis installed to $redisDir" -ForegroundColor Green
} else {
    Write-Host "[3/3] Portable Redis already present at $redisDir" -ForegroundColor Green
}

Write-Host ""
Write-Host "Setup complete. Next:" -ForegroundColor Cyan
Write-Host "  powershell -ExecutionPolicy Bypass -File scripts\run.ps1          # start redis + server + device"
Write-Host "  powershell -ExecutionPolicy Bypass -File scripts\smoke_test.ps1   # verify end-to-end"
