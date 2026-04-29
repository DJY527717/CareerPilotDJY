@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "ENV_DIR=.venv_cp313"
set "VENV_PY=%ENV_DIR%\Scripts\python.exe"
set "PY_CMD="
set "VENV_OK=0"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
if not defined APP_HOST set "APP_HOST=127.0.0.1"
if not defined APP_PORT set "APP_PORT=8503"
if not defined APP_OPEN_HOST set "APP_OPEN_HOST=%APP_HOST%"
if "%APP_OPEN_HOST%"=="0.0.0.0" set "APP_OPEN_HOST=127.0.0.1"
set "APP_URL=http://%APP_OPEN_HOST%:%APP_PORT%"

echo.
echo ============================================
echo  CareerPilot Job Analyzer - setup/repair
echo ============================================
echo.

if exist "%VENV_PY%" (
    "%VENV_PY%" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set "VENV_OK=1"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "C:\Python313\python.exe" (
    "C:\Python313\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="C:\Python313\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "C:\Python312\python.exe" (
    "C:\Python312\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="C:\Python312\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "C:\Python311\python.exe" (
    "C:\Python311\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="C:\Python311\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "%ProgramFiles%\Python313\python.exe" (
    "%ProgramFiles%\Python313\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="%ProgramFiles%\Python313\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "%ProgramFiles%\Python312\python.exe" (
    "%ProgramFiles%\Python312\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="%ProgramFiles%\Python312\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "%ProgramFiles%\Python311\python.exe" (
    "%ProgramFiles%\Python311\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="%ProgramFiles%\Python311\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD (
    py -3.13 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set "PY_CMD=py -3.13"
)

if "%VENV_OK%"=="0" if not defined PY_CMD (
    py -3.12 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set "PY_CMD=py -3.12"
)

if "%VENV_OK%"=="0" if not defined PY_CMD (
    py -3.11 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set "PY_CMD=py -3.11"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" (
    "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" (
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD if exist "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" (
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set PY_CMD="%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
)

if "%VENV_OK%"=="0" if not defined PY_CMD (
    python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set "PY_CMD=python"
)

if "%VENV_OK%"=="0" if not defined PY_CMD (
    python3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
    if not errorlevel 1 set "PY_CMD=python3"
)

if "%VENV_OK%"=="0" if not defined PY_CMD (
    echo Could not find a runnable Python 3.11+.
    echo Please install Python 3.13 to C:\Python313 and run this file again.
    pause
    exit /b 1
)

if "%VENV_OK%"=="0" if exist "%VENV_PY%" (
    echo Existing local environment is broken or too old. Recreating...
    rmdir /s /q "%ENV_DIR%"
)

if "%VENV_OK%"=="0" if not exist "%VENV_PY%" (
    echo Creating local virtual environment...
    %PY_CMD% -m venv "%ENV_DIR%"
    if errorlevel 1 (
        echo Failed to create local environment.
        pause
        exit /b 1
    )
)

if not exist "%VENV_PY%" (
    echo Local environment was not created.
    pause
    exit /b 1
)

"%VENV_PY%" -c "import sys" >nul 2>nul
if errorlevel 1 (
    echo Local environment still cannot start.
    echo If Python is installed under a Chinese user path, reinstall Python 3.13 to C:\Python313 and run this file again.
    pause
    exit /b 1
)

echo Checking installed dependencies...
"%VENV_PY%" -c "import streamlit, pandas, numpy, plotly, requests, openpyxl, pypdf, docx, bs4, PIL, pytesseract, reportlab, rapidfuzz, jieba, sklearn, pdfplumber, lxml, playwright" >nul 2>nul
if not errorlevel 1 (
    echo Dependencies are already available. Skipping network install.
    goto START_APP
)

echo Dependencies are missing or damaged. Repairing with pip...
echo.
echo Upgrading pip via mirror. This step can be skipped safely if the network is unstable.
"%VENV_PY%" -m pip install --disable-pip-version-check --retries 5 --timeout 60 --prefer-binary --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
if errorlevel 1 (
    echo Pip upgrade failed, continuing with the existing pip.
)

echo.
echo Installing dependencies from Tsinghua mirror...
"%VENV_PY%" -m pip install --disable-pip-version-check --retries 5 --timeout 60 --prefer-binary -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
if errorlevel 1 (
    echo Tsinghua mirror failed. Trying Aliyun mirror...
    "%VENV_PY%" -m pip install --disable-pip-version-check --retries 5 --timeout 60 --prefer-binary -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple --trusted-host mirrors.aliyun.com
)
if errorlevel 1 (
    echo Aliyun mirror failed. Trying official PyPI...
    "%VENV_PY%" -m pip install --disable-pip-version-check --retries 5 --timeout 60 --prefer-binary -r requirements.txt -i https://pypi.org/simple --trusted-host pypi.org --trusted-host files.pythonhosted.org
)
if errorlevel 1 (
    echo.
    echo Dependency installation failed.
    echo The SSL EOF warning usually means the package download connection was interrupted.
    echo Please check the network or proxy, then run this file again.
    pause
    exit /b 1
)

echo.
echo Verifying dependencies...
"%VENV_PY%" -c "import streamlit, pandas, numpy, plotly, requests, openpyxl, pypdf, docx, bs4, PIL, pytesseract, reportlab, rapidfuzz, jieba, sklearn, pdfplumber, lxml, playwright" >nul 2>nul
if errorlevel 1 (
    echo Dependency verification failed after installation.
    echo Please run this file again, or delete %ENV_DIR% and retry setup.
    pause
    exit /b 1
)

:START_APP
echo.
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
"%VENV_PY%" -m streamlit run app.py --server.address %APP_HOST% --server.port %APP_PORT%

echo.
echo CareerPilot has stopped. You can close this window or run the shortcut again.
pause
