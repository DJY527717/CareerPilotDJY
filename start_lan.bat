@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "APP_HOST=0.0.0.0"
set "APP_PORT=8503"

for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$ip = Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254*' -and $_.PrefixOrigin -ne 'WellKnown' } | Sort-Object InterfaceMetric | Select-Object -First 1 -ExpandProperty IPAddress; if ($ip) { $ip }"`) do set "APP_OPEN_HOST=%%I"

if not defined APP_OPEN_HOST set "APP_OPEN_HOST=127.0.0.1"
set "APP_SHARE_URL=http://%APP_OPEN_HOST%:%APP_PORT%"

echo.
echo ============================================
echo  CareerPilot LAN launcher
echo ============================================
echo.
echo Keep this window running.
echo This computer opens: %APP_SHARE_URL%
echo Other computers on the same Wi-Fi/LAN open:
echo   %APP_SHARE_URL%
echo.
echo If other computers cannot open it, allow Python/Streamlit through Windows Firewall for port %APP_PORT%.
echo.

call "%~dp0start_app.bat"
