@echo off
cls
echo.
echo ═══════════════════════════════════════════════════════════════
echo    📡 Starting IoT Sensor Producer
echo ═══════════════════════════════════════════════════════════════
echo.

cd /d "%~dp0\Producer"

echo Setting environment variables...
set KAFKA_BOOTSTRAP_SERVERS=localhost:9092
set PYTHONIOENCODING=utf-8

echo.
echo Starting IoT Sensor Simulator...
echo Data will be sent to Kafka topic: farmSensors
echo.
python IotSystem_Version1.1.py
