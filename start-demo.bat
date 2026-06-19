@echo off
REM One-click demo: starts the full OpenSAI stack and opens the browser to the
REM auto-demo (smart placement / resource allocation / relay multipath). Double-click me.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\demo.ps1" %*
echo.
echo Demo stack is running in the background. Closing this window is fine.
echo To stop: powershell -ExecutionPolicy Bypass -File scripts\stop.ps1 -All
pause
