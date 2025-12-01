@echo off
cls
echo.
echo ═══════════════════════════════════════════════════════════════
echo    🔌 Starting FastAPI Backend Server
echo ═══════════════════════════════════════════════════════════════
echo.

cd /d "%~dp0\GUI_Dashboard\backend"

echo Checking virtual environment...
if not exist "venv" (
    echo Creating virtual environment (first time only)...
    python -m venv venv
    echo.
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo Installing/Updating dependencies...
pip install -q -r requirements.txt

echo.
echo Starting Backend API Server...
echo API will be available at http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo.
echo Setting environment variable for Kafka...
set KAFKA_BOOTSTRAP_SERVERS=localhost:9092

uvicorn main:app --reload --port 8000
