@echo off
cls
echo.
echo ═══════════════════════════════════════════════════════════════
echo    🎨 Starting React Dashboard Frontend
echo ═══════════════════════════════════════════════════════════════
echo.

cd /d "%~dp0\GUI_Dashboard\frontend"

echo Checking node_modules...
if not exist "node_modules" (
    echo Installing dependencies (first time only, may take 3-5 minutes)...
    call npm install
    echo.
)

echo.
echo Starting React development server...
echo The browser will open automatically at http://localhost:3000
echo.
call npm start
