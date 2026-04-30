@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "ENV_DIR=.venv_cp313"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
if not defined APP_HOST set "APP_HOST=127.0.0.1"
if not defined APP_PORT set "APP_PORT=8503"
if not defined APP_OPEN_HOST set "APP_OPEN_HOST=%APP_HOST%"
if "%APP_OPEN_HOST%"=="0.0.0.0" set "APP_OPEN_HOST=127.0.0.1"
set "APP_URL=http://%APP_OPEN_HOST%:%APP_PORT%"

echo.
echo ============================================
echo  CareerPilot Job Analyzer - launcher
echo ============================================
echo.

if not exist "%ENV_DIR%\Scripts\python.exe" (
    echo Local environment %ENV_DIR% not found. Running first setup...
    call "%~dp0install_and_run.bat"
    exit /b
)

"%ENV_DIR%\Scripts\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
if errorlevel 1 (
    echo Local environment is broken or too old. Running setup repair...
    call "%~dp0install_and_run.bat"
    exit /b
)

echo Checking installed dependencies...
"%ENV_DIR%\Scripts\python.exe" -c "import streamlit, pandas, numpy, plotly, requests, openpyxl, pypdf, docx, bs4, PIL, pytesseract, reportlab, rapidfuzz, jieba, sklearn, pdfplumber, lxml, playwright" >nul 2>nul
if errorlevel 1 (
    echo Some dependencies are missing or damaged. Running setup repair...
    call "%~dp0install_and_run.bat"
    exit /b
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing '%APP_URL%' -TimeoutSec 2; if ($r.StatusCode -ge 200) { exit 0 } } catch { exit 1 }" >nul 2>nul
if not errorlevel 1 (
    echo CareerPilot is already running.
    echo Opening %APP_URL% ...
    start "" "%APP_URL%"
    exit /b 0
)

echo Starting CareerPilot at %APP_URL%
if defined APP_SHARE_URL echo Other computers can open: %APP_SHARE_URL%
start "" /min powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 5; Start-Process '%APP_URL%'"
"%ENV_DIR%\Scripts\python.exe" serve.py

echo.
echo CareerPilot has stopped. You can close this window or run the shortcut again.
pause
