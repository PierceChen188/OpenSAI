<#
  跨设备数据面测试：单机起 server + 2 个 device(不同 OPENSAI_DATA_PORT)，
  提交 Producer->Consumer DAG（强制分到不同 device），验证经 QUIC 跨设备传输正确。
  纯本机 loopback 即可验证全部逻辑。
#>
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$vpy = Join-Path $root '.venv\Scripts\python.exe'
$logs = Join-Path $root '.run'
$cli = Join-Path $env:LOCALAPPDATA 'opensai-redis\redis-cli.exe'
if (-not (Test-Path $logs)) { New-Item -ItemType Directory -Path $logs | Out-Null }

function Stop-All {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like '*dag_offload*' -or $_.CommandLine -like '*inject_*' } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

# 确保 redis 在跑
if ((& $cli ping 2>$null) -ne 'PONG') {
    Start-Process (Join-Path $env:LOCALAPPDATA 'opensai-redis\redis-server.exe') -ArgumentList '--port 6379 --save "" --appendonly no' -WindowStyle Hidden
    Start-Sleep 2
}

Stop-All
Start-Sleep 2
# 确保 8888 空闲：杀掉任何残留的旧 server，避免 Windows SO_REUSEADDR 让注入器连到没有设备的旧实例
for ($i = 0; $i -lt 10; $i++) {
    $owner = (Get-NetTCPConnection -LocalPort 8888 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1).OwningProcess
    if (-not $owner) { break }
    Stop-Process -Id $owner -Force -ErrorAction SilentlyContinue
    Start-Sleep 1
}
& $cli flushall | Out-Null

# server
Start-Process $vpy -ArgumentList 'Offloading-System\dag_offload\server.py' -WorkingDirectory $root `
    -RedirectStandardOutput "$logs\server.out.log" -RedirectStandardError "$logs\server.err.log" -WindowStyle Hidden
Start-Sleep 3

# device1 (data port 9999) + device2 (data port 9998)
$env:OPENSAI_DATA_PORT = '9999'
Start-Process $vpy -ArgumentList 'Offloading-System\dag_offload\device_client.py' -WorkingDirectory $root `
    -RedirectStandardOutput "$logs\device1.out.log" -RedirectStandardError "$logs\device1.err.log" -WindowStyle Hidden
$env:OPENSAI_DATA_PORT = '9998'
Start-Process $vpy -ArgumentList 'Offloading-System\dag_offload\device_client.py' -WorkingDirectory $root `
    -RedirectStandardOutput "$logs\device2.out.log" -RedirectStandardError "$logs\device2.err.log" -WindowStyle Hidden
Remove-Item Env:\OPENSAI_DATA_PORT -ErrorAction SilentlyContinue

# 等两台 device 注册
$n = 0
for ($i = 0; $i -lt 25; $i++) {
    Start-Sleep 1
    $n = [int](& $cli scard DEVICE_ONLINE_SET)
    if ($n -ge 2) { break }
}
Write-Host "devices online: $n" -ForegroundColor Cyan

# 跑跨设备注入测试
& $vpy (Join-Path $PSScriptRoot 'inject_crossdevice.py')
$code = $LASTEXITCODE
if ($code -eq 0) { Write-Host "`nCROSS-DEVICE TEST: PASS" -ForegroundColor Green }
else { Write-Host "`nCROSS-DEVICE TEST: FAIL ($code)" -ForegroundColor Red }
exit $code
