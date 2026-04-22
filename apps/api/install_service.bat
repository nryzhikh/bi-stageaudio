@echo off
REM ============================================
REM HireTrack Sync API - Windows Service Installer
REM Run as Administrator
REM ============================================

REM === CONFIGURATION - EDIT THESE PATHS ===
SET PYTHON_PATH=C:\Users\Admin\AppData\Local\Programs\Python\Python313-32\python.exe
SET APP_DIR=E:\hiretrack-flask-api\server
SET NSSM_PATH=E:\nssm\nssm.exe
SET SERVICE_NAME=HireTrackFlaskApi

echo ==========================================
echo HireTrack Sync API - Service Installer
echo ==========================================
echo.
echo Configuration:
echo   Python: %PYTHON_PATH%
echo   App:    %APP_DIR%
echo   NSSM:   %NSSM_PATH%
echo   Service: %SERVICE_NAME%
echo.

REM Check if NSSM exists
if not exist "%NSSM_PATH%" (
    echo [ERROR] NSSM not found at %NSSM_PATH%
    echo Please download NSSM from https://nssm.cc/ and update NSSM_PATH
    pause
    exit /b 1
)

REM Check if Python exists
if not exist "%PYTHON_PATH%" (
    echo [ERROR] Python not found at %PYTHON_PATH%
    echo Please update PYTHON_PATH in this script
    pause
    exit /b 1
)

REM Check if app.py exists
if not exist "%APP_DIR%\app.py" (
    echo [ERROR] app.py not found at %APP_DIR%\app.py
    echo Please update APP_DIR in this script
    pause
    exit /b 1
)

REM Install dependencies
echo.
echo [1/4] Installing Python dependencies...
"%PYTHON_PATH%" -m pip install -r "%APP_DIR%\requirements.txt"
if %errorlevel% neq 0 (
    echo [WARNING] Some dependencies may not have installed correctly
)

REM Check if service already exists
sc query %SERVICE_NAME% >nul 2>&1
if %errorlevel% == 0 (
    echo.
    echo [2/4] Removing existing service...
    "%NSSM_PATH%" stop %SERVICE_NAME% >nul 2>&1
    "%NSSM_PATH%" remove %SERVICE_NAME% confirm
)

REM Install service
echo.
echo [3/4] Installing service...
"%NSSM_PATH%" install %SERVICE_NAME% "%PYTHON_PATH%" "%APP_DIR%\app.py"
"%NSSM_PATH%" set %SERVICE_NAME% AppDirectory "%APP_DIR%"
"%NSSM_PATH%" set %SERVICE_NAME% DisplayName "HireTrack Database Sync API"
"%NSSM_PATH%" set %SERVICE_NAME% Description "REST API for HireTrack database access and BI tool integration"
"%NSSM_PATH%" set %SERVICE_NAME% Start SERVICE_AUTO_START

REM Set environment variables (optional)
REM "%NSSM_PATH%" set %SERVICE_NAME% AppEnvironmentExtra API_PORT=5003
REM "%NSSM_PATH%" set %SERVICE_NAME% AppEnvironmentExtra API_USERNAME=admin
REM "%NSSM_PATH%" set %SERVICE_NAME% AppEnvironmentExtra API_PASSWORD=secret

REM Configure logging
"%NSSM_PATH%" set %SERVICE_NAME% AppStdout "%APP_DIR%\service.log"
"%NSSM_PATH%" set %SERVICE_NAME% AppStderr "%APP_DIR%\service_error.log"
"%NSSM_PATH%" set %SERVICE_NAME% AppRotateFiles 1
"%NSSM_PATH%" set %SERVICE_NAME% AppRotateBytes 1048576

REM Start service
echo.
echo [4/4] Starting service...
"%NSSM_PATH%" start %SERVICE_NAME%

REM Check status
echo.
echo ==========================================
sc query %SERVICE_NAME% | findstr "STATE"
echo ==========================================
echo.
echo Service installed successfully!
echo.
echo API available at:
echo   Local:  http://localhost:5003
echo   Remote: http://YOUR_IP:5003
echo.
echo Test with:
echo   curl http://localhost:5003/health
echo   curl http://localhost:5003/api/tables
echo.
echo Logs:
echo   %APP_DIR%\service.log
echo   %APP_DIR%\service_error.log
echo.
pause
