@echo off
chcp 65001 >nul
cls
echo.
echo ═══════════════════════════════════════════════════════════════
echo    🌱 Farm IoT Dashboard - System Launcher
echo ═══════════════════════════════════════════════════════════════
echo.

REM Check if Docker is running
echo Checking Docker status...
docker info >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ ERROR: Docker is not running!
    echo Please start Docker Desktop and try again.
    echo.
    pause
    exit /b 1
)

echo ✅ Docker is running
echo.
echo 🚀 Starting all services...
echo (This may take 2-3 minutes on first run)
echo.

REM Stop any existing containers
docker-compose down >nul 2>&1

REM Start all services
docker-compose up -d --build

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ ERROR: Failed to start containers
    echo.
    echo Please check the error messages above.
    pause
    exit /b 1
)

echo.
echo ✅ All containers started!
echo.
echo ⏳ Waiting for services to initialize...
echo    (60 seconds - please be patient)
echo.

REM Wait for services to be ready
timeout /t 60 /nobreak >nul

echo.
echo ═══════════════════════════════════════════════════════════════
echo    ✅ SYSTEM READY!
echo ═══════════════════════════════════════════════════════════════
echo.
echo    Access your dashboards:
echo.
echo    🎨 Main Dashboard:  http://localhost:3000
echo    🔌 Backend API:     http://localhost:8000
echo    📈 Grafana:         http://localhost:3001
echo    💾 InfluxDB:        http://localhost:8086
echo.
echo ═══════════════════════════════════════════════════════════════
echo.

REM Open the main dashboard
echo Opening dashboard in your browser...
start http://localhost:3000

echo.
echo ✅ System is running!
echo.
echo To stop the system, run: docker-compose down
echo.
pause
