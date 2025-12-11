# 📋 Linux Modifications Summary

## Update Date
**December 2, 2024** - Complete project modification to work on Linux system

## 📝 Modified Files

### 1️⃣ Main Python Files

#### `Consumer/Spark_Transformation_v1.1.py` ✅
**Changes:**
- Added `import os` to support environment variables
- Updated `KAFKA_BOOTSTRAP` to use `os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')`
- Updated `HOME_PATH` to default to `/root`
- Updated `PARQUET_BASE_PATH` and `CHECKPOINT_BASE` to use environment variables
- Updated `DB_URL` and `DB_PROPERTIES` to use `os.getenv()`

#### `Producer/IotSystem_Version1.1.py` ✅
**Changes:**
- Added `os.path.expanduser()` for Linux path support
- Updated InfluxDB config to use environment variables:
  - `INFLUX_URL`
  - `INFLUX_TOKEN`
  - `INFLUX_ORG`
  - `INFLUX_BUCKET`

#### `GUI_Dashboard/backend/main.py` ✅
**Changes:**
- Added `import logging` for logging support on Linux
- Updated configuration to use logging instead of `print()`

#### `GUI_Dashboard/backend/database.py` ✅
**Changes:**
- Added `import logging` and `import os`
- Updated PostgreSQL connection to include `port` from environment variables
- Updated all logging messages to use `logger` instead of `print()`

#### `GUI_Dashboard/backend/kafka_consumer.py` ✅
**Changes:**
- Added `import logging`
- Updated Consumer to add `group_id` and `session_timeout_ms`
- Updated all logging messages to use `logger`

#### Legacy Versions
- `Consumer/Spark_Transformation_v0.0.py` ✅
- `Consumer/Spark_Transformation_v0.1.py` ✅
- `Consumer/Spark_Transformation_v1.0.py` ✅

**Changes Applied:**
- Updated `KAFKA_BOOTSTRAP` to use environment variables
- Updated `DB_URL` and `DB_PROPERTIES` to use environment variables
- Updated paths to work with Linux

### 2️⃣ Docker Files

#### `.env` ✅
**Changes:**
- Updated `KAFKA_BOOTSTRAP_SERVERS` to point to `kafka:29092` (container name)
- Updated `OUTPUT_DIR` to `/root/spark_project_data/output`
- Added new variables:
  - `SPARK_HOME=/opt/spark`
  - `HOME=/root`
  - `PARQUET_BASE_PATH`
  - `CHECKPOINT_BASE`
  - `INFLUX_*` variables
  - `GF_*` Grafana variables

#### `docker-entrypoint-producer.sh` ✅
**Changes:**
- Added `max_attempts` variable to avoid infinite loops
- Improved wait messages

#### `docker-entrypoint-consumer.sh` ✅
**Changes:**
- Added `max_attempts` variable
- Improved error handling

### 3️⃣ New Shell Scripts

✅ Created new shell scripts instead of .bat files:

1. **`start_system.sh`** - Start the entire system
   - Start all Docker services
   - Create required directories
   - Display connection information

2. **`stop_system.sh`** - Stop the system
   - Stop all Docker services
   - Preserve data (volumes)

3. **`start_producer.sh`** - Start Producer
   - Start Zookeeper, Kafka, and Producer

4. **`start_consumer.sh`** - Start Consumer
   - Start Kafka, PostgreSQL, and Consumer

5. **`start_backend.sh`** - Start Backend
   - Start PostgreSQL, Grafana, and Backend

6. **`start_frontend.sh`** - Start Frontend
   - Start Frontend Dashboard

### 4️⃣ New Helper Files

✅ **`README_LINUX.md`**
- Comprehensive guide for running on Linux
- System requirements
- Installation steps
- Quick start commands
- List of all services
- Common troubleshooting

✅ **`verify_linux_compatibility.sh`**
- Automatic verification program
- Path checking
- localhost references check
- Shell files check
- Environment variables check

## 🔧 Key Changes

### Paths
```
❌ Windows: C:\Users\mostafa\...
✅ Linux:   /root/... or ~/...
✅ Docker:  Environment variables
```

### Environment Variables
```
✅ KAFKA_BOOTSTRAP_SERVERS
✅ POSTGRES_HOST/USER/PASSWORD/DB
✅ PARQUET_BASE_PATH
✅ CHECKPOINT_BASE
✅ INFLUX_* variables
✅ GF_* Grafana variables
```

### Connections
```
❌ localhost:9092
✅ kafka:29092 (container name)

❌ localhost:5432
✅ postgres:5432 (container name)

❌ http://localhost:8086
✅ http://influxdb:8086 (container name)
```

### Logging
```
❌ print() - direct output
✅ logging module - supports centralized logging
```

## ✅ Compatibility Verification

Successfully ran `verify_linux_compatibility.sh`:
- ✅ No Windows paths found
- ✅ No incorrect localhost references
- ✅ All shell files are executable
- ✅ .env file contains all required variables
- ✅ All Python files import `os`

## 🚀 Quick Start

```bash
# Immediate start
./start_system.sh

# Stop
./stop_system.sh

# View logs
docker-compose logs -f
```

## 📊 Available Services After Startup

| Service | URL | Notes |
|---------|-----|-------|
| Kafka | kafka:29092 | Inside Docker |
| PostgreSQL | postgres:5432 | Username: spark_user |
| InfluxDB | http://influxdb:8086 | Time-series data |
| Grafana | http://localhost:3001 | Monitoring dashboard |
| Backend API | http://localhost:8000 | FastAPI |
| Frontend | http://localhost:3000 | React Dashboard |

## 📝 Important Notes

1. **Environment variables**: Default values set in code to ensure operation even without `.env`
2. **Docker Compose**: All services configured to work with Docker volumes
3. **Data**: Data preserved in Docker volumes even after stopping services
4. **Paths**: Used `$HOME` and `os.getenv()` for flexibility

## 🔍 How to Verify

```bash
# Verify compatibility
./verify_linux_compatibility.sh

# View service status
docker-compose ps

# View logs
docker-compose logs [service_name]
```

---

**Created by:** Automatic Update System
**Version:** v2.0 (Linux Compatible)
**Date:** 2024-12-02
