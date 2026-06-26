# ============================================================================
# OpenSAI Console — one-click launcher (Windows / PowerShell)
#
#   • (optionally) builds the frontend bundle into dag_offload/static
#   • starts the Flask bridge (user_client.py) on http://127.0.0.1:5000
#   • opens your browser at the console
#
# Usage:
#   scripts\start_console.ps1            # serve the existing build (build if missing)
#   scripts\start_console.ps1 -Build     # force a fresh frontend build first
#   scripts\start_console.ps1 -NoBrowser # don't auto-open the browser
#
# The bridge serves the SPA even if the TCP hub / Redis aren't running — in that
# case the console shows "Disconnected"; toggle DEMO in the top bar to explore.
# ============================================================================
param(
    [switch]$Build,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"

# --- resolve paths (repo root = parent of this script's folder) ---
$RepoRoot   = Split-Path -Parent $PSScriptRoot
$Frontend   = Join-Path $RepoRoot "frontend"
$Bridge     = Join-Path $RepoRoot "Offloading-System\dag_offload\user_client.py"
$Static     = Join-Path $RepoRoot "Offloading-System\dag_offload\static\index.html"
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Python     = if (Test-Path $VenvPython) { $VenvPython } else { "python" }
$Url        = "http://127.0.0.1:5000"

Write-Host "OpenSAI Console launcher" -ForegroundColor Cyan
Write-Host "  repo:   $RepoRoot"
Write-Host "  python: $Python"

# --- build the frontend if asked, or if no bundle exists yet ---
$needBuild = $Build -or -not (Test-Path $Static)
if ($needBuild) {
    if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
        Write-Host "npm not found. Install Node.js, or build on another machine and copy dag_offload/static." -ForegroundColor Red
        if (-not (Test-Path $Static)) { exit 1 }
    } else {
        Push-Location $Frontend
        try {
            if (-not (Test-Path (Join-Path $Frontend "node_modules"))) {
                Write-Host "Installing frontend dependencies (first run)..." -ForegroundColor Yellow
                npm install --no-audit --no-fund
            }
            Write-Host "Building frontend bundle..." -ForegroundColor Yellow
            npm run build
        } finally {
            Pop-Location
        }
    }
} else {
    Write-Host "  bundle: found (use -Build to rebuild)" -ForegroundColor DarkGray
}

# --- open the browser shortly after the server comes up ---
if (-not $NoBrowser) {
    Start-Job -ScriptBlock {
        param($u)
        for ($i = 0; $i -lt 30; $i++) {
            try { Invoke-WebRequest -Uri $u -UseBasicParsing -TimeoutSec 2 | Out-Null; break } catch { Start-Sleep -Milliseconds 500 }
        }
        Start-Process $u
    } -ArgumentList $Url | Out-Null
}

# --- run the bridge in the foreground (Ctrl+C to stop) ---
Write-Host "`nStarting bridge at $Url  (Ctrl+C to stop)`n" -ForegroundColor Green
& $Python $Bridge
