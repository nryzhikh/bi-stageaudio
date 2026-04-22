@echo off
REM ============================================
REM HireTrack Sync API - Service Uninstaller
REM Run as Administrator
REM ============================================

SET NSSM_PATH=E:\nssm\nssm.exe
SET SERVICE_NAME=HireTrackFlaskApi

echo Stopping and removing %SERVICE_NAME%...

"%NSSM_PATH%" stop %SERVICE_NAME%
"%NSSM_PATH%" remove %SERVICE_NAME% confirm

echo.
echo Service removed.
pause
