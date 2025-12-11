# 🌱 Farm IoT System - Linux Setup Guide

This guide explains how to run the Farm IoT System monitoring and simulation on a Linux system.

## Requirements

- **Linux** (Ubuntu 20.04 or newer)
- **Docker** >= 20.10
- **Docker Compose** >= 1.29
- **Python** 3.8+ (for local development)
- **Git**

## Installation

### 1. Verify Requirements

```bash
# Check Docker
docker --version

# Check Docker Compose
docker-compose --version

# Check Python
python3 --version
```

### 2. Clone the Project

```bash
git clone https://github.com/MostafaAbd-elzaher/Final_Project_Data_Hive.git
cd Final_Project_Data_Hive
```

### 3. Prepare Environment

```bash
# Create required directories
mkdir -p ~/spark_project_data/output
mkdir -p ~/spark_project_data/farm_iot_parquet
mkdir -p ~/spark_project_data/checkpoints/farm_iot_full_pipeline

# Make shell files executable
chmod +x *.sh

# Load environment variables
source .env
```

## Quick Start

### Method 1: Start the Entire System

```bash
# Start all services
./start_system.sh

# Stop all services
./stop_system.sh
```

### Method 2: Start Services Separately

#### Start Producer (Sensor Simulator)
```bash
./start_producer.sh
```

#### Start Consumer (Spark Streaming)
```bash
./start_consumer.sh
```

#### Start Backend API
```bash
./start_backend.sh
```

#### Start Frontend Dashboard
```bash
./start_frontend.sh
```

## Accessing Services

After successfully starting the system:

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka | localhost:9092 | - |
| PostgreSQL | localhost:5432 | spark_user / spark_password |
| InfluxDB | http://localhost:8086 | admin / admin |
| Grafana | http://localhost:3001 | admin / admin |
| Backend API | http://localhost:8000 | - |
| Frontend | http://localhost:3000 | - |

## Viewing Logs

```bash
# View logs for all services
docker-compose logs -f

# View logs for specific service
docker-compose logs -f producer
docker-compose logs -f consumer
docker-compose logs -f backend
```

## Important Linux Modifications

The project has been modified to work on Linux:

✅ **File Paths**
- Changed paths from Windows to Linux (use `/root/` instead of `C:\`)
- Using `os.path.expanduser()` for portable paths

✅ **Environment Variables**
- Updated `.env` to use Linux paths
- Support for environment variables from Docker

✅ **Shell Scripts**
- Created `.sh` files instead of `.bat`
- Bash support on Linux

✅ **Database**
- Updated PostgreSQL connection to work with Docker
- Support for environment variables in all settings

## Troubleshooting

### Problem: Services Won't Start

```bash
# Check service status
docker-compose ps

# Restart services
docker-compose restart

# Clean containers and restart
docker-compose down -v
./start_system.sh
```

### Problem: Kafka Connection Error

```bash
# Check Kafka logs
docker-compose logs kafka

# Wait a few seconds and retry
sleep 10
```

### Problem: PostgreSQL Error

```bash
# Check logs
docker-compose logs postgres

# Make sure data is preserved
docker volume ls
```

## Important Files

```
FinalProject/
├── .env                              # Environment variables (updated for Linux)
├── docker-compose.yml                # Docker settings
├── start_system.sh                   # Start the entire system
├── stop_system.sh                    # Stop the system
├── start_producer.sh                 # Start Producer
├── start_consumer.sh                 # Start Consumer
├── start_backend.sh                  # Start Backend
├── start_frontend.sh                 # Start Frontend
├── Producer/
│   └── IotSystem_Version1.1.py       # (updated for Linux)
├── Consumer/
│   └── Spark_Transformation_v1.1.py  # (updated for Linux)
└── GUI_Dashboard/backend/
    ├── main.py                       # (updated for Linux)
    ├── kafka_consumer.py             # (updated for Linux)
    └── database.py                   # (updated for Linux)
```

## Local Development

### Run Producer Locally (Without Docker)

```bash
# Install requirements
pip3 install -r requirements.txt

# Run the simulator
python3 Producer/IotSystem_Version1.1.py
```

### Run Backend Locally

```bash
cd GUI_Dashboard/backend

# Install requirements
pip3 install -r requirements.txt

# Start the server
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Performance and Improvements

- Docker for isolation and scalability
- Kafka for high-performance messaging
- Spark Streaming for real-time processing
- PostgreSQL for persistent data storage
- Grafana for visual presentations

## Support and Help

For more information or to report issues:
- See main `README.md`
- See `implementation_plan.md` for design details
- Check Docker logs for errors

---

**Updated:** December 2024
**Compatibility:** Linux (Ubuntu 20.04+), Docker 20.10+
