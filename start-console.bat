@echo off
REM OpenSAI Console — double-click launcher.
REM Builds the frontend if needed, starts the Flask bridge, opens the browser.
REM Pass /build to force a fresh frontend rebuild.
setlocal
set ARGS=
if /I "%~1"=="/build" set ARGS=-Build
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\start_console.ps1" %ARGS%
pause
