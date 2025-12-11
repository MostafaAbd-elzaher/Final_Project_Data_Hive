# ▶️ Greenhouse Monitoring System Startup Guide

**Step-by-step guide to run the complete system on Linux and Docker**

---

## 📋 Prerequisites

Before starting, make sure of:
- ✅ Docker and Docker Compose installed
- ✅ `requirements.txt` with all libraries
- ✅ Read `.env` file and verify variables
- ✅ `docker-compose.yml` file in the correct path

---

## 🚀 Method 1: Interactive Quick Start (Recommended)

This method guides you step by step with automatic checks:

```bash
# Make the script executable
chmod +x QUICK_START_LINUX.sh

# Run the quick start guide
./QUICK_START_LINUX.sh
```

The guide will:
1. Check Docker and Docker Compose availability
2. Load environment variables from `.env`
3. Create required directories
4. Verify Linux compatibility
5. Pull latest Docker images
6. Start all services
7. Display access information

---

## 🔧 Method 2: Manual Startup

### Step 1: Verify Requirements

```bash
# Check Docker
docker --version
# Expected result: Docker version 20.10+

# Check Docker Compose
docker-compose --version
# Expected result: Docker Compose version 1.29+
```

### Step 2: Verify Compatibility

```bash
# Run the verification script
./verify_linux_compatibility.sh

# You should see: ✅ All checks passed!
```

### Step 3: Load Environment Variables

```bash
# Load settings
export $(cat .env | grep -v '#' | xargs)

# Verify important variables
echo $KAFKA_BOOTSTRAP_SERVERS    # kafka:29092
echo $POSTGRES_HOST              # postgres
echo $PARQUET_BASE_PATH          # /root/spark_project_data/farm_iot_parquet
```

### Step 4: Create Required Directories

```bash
# Create data storage directories
mkdir -p ~/spark_project_data/output
mkdir -p ~/spark_project_data/farm_iot_parquet
mkdir -p ~/spark_project_data/checkpoints/farm_iot_full_pipeline

# Verify creation
ls -la ~/spark_project_data/
```

### Step 5: Start the System

#### Option A: Start all services together

```bash
# Start all services
./start_system.sh

# Or manually:
docker-compose up -d

# Wait 10 seconds for all services to start
sleep 10

# Check services status
docker-compose ps
```

#### Option B: Start services separately

If you want full control:

```bash
# In Terminal 1: Start Kafka, Zookeeper, and Producer
./start_producer.sh

# In Terminal 2: Start Consumer (Spark)
./start_consumer.sh

# In Terminal 3: Start Backend and Database
./start_backend.sh

# In Terminal 4: Start Frontend (optional)
./start_frontend.sh
```

---

## 🔍 Verify Correct Operation

### 1. Check services status

```bash
# Display all containers
docker-compose ps

# Expected result:
# NAME                 STATUS
# zookeeper           Up 1 minute
# kafka               Up 50 seconds
# postgres            Up 1 minute
# influxdb            Up 1 minute
# grafana             Up 50 seconds
# producer            Up 30 seconds
# consumer            Up 20 seconds
# backend             Up 10 seconds
```

### 2. Display logs

```bash
# Display all logs in real-time
docker-compose logs -f

# Display logs for a specific service only
docker-compose logs -f producer
docker-compose logs -f consumer
docker-compose logs -f backend
docker-compose logs -f kafka

# Display only last 100 lines
docker-compose logs --tail=100 consumer
```

### 3. Test Kafka connection

```bash
# Check for existing topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Expected result:
# farmSensors
# farmInsights
# farmTrends
# farmKpis
```

### 4. Verify the database

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U spark_user -d farm_dwh -c "SELECT COUNT(*) FROM fact_sensor_events;"

# You should see the number of records inserted
```

---

## 🌐 Access Services

After successful startup, you can access:

| Service | URL | Credentials | Notes |
|---------|-----|-------------|-------|
| **Grafana** | http://localhost:3001 | admin / admin | Dashboards |
| **Backend API** | http://localhost:8000 | - | FastAPI Docs: /docs |
| **Frontend Dashboard** | http://localhost:3000 | - | React App |
| **InfluxDB** | http://localhost:8086 | admin / admin | Time-series database |
| **Kafka UI** | http://localhost:8080 | - | If installed |
| **PostgreSQL** | localhost:5432 | spark_user / spark_password | From within Docker only |

---

## 🛑 Stop the System

### Safe shutdown

```bash
# Stop all services while keeping data
./stop_system.sh

# Or manually:
docker-compose down

# Verify shutdown
docker-compose ps
```

### Force shutdown (if needed)

```bash
# Stop and delete all data and Volumes
docker-compose down -v

# Complete cleanup of Containers and Images
docker system prune -a --volumes
```

---

## 🔄 Restart the System

```bash
# Quick restart (services keep running)
docker-compose restart

# Restart a specific service
docker-compose restart producer
docker-compose restart consumer

# Complete restart (stop then start)
./stop_system.sh
sleep 5
./start_system.sh
```

---

## 🧹 Clean Old Data

If you want to start fresh without old data:

```bash
# Delete old Checkpoints
rm -rf ~/spark_project_data/checkpoints/

# Delete Delta Lake
rm -rf ~/spark_project_data/farm_iot_parquet/

# Delete data from Docker Volumes
docker-compose down -v

# Restart the system
./start_system.sh
```

---

## ⚠️ Troubleshooting Common Issues

### Issue: Services won't start

```bash
# Display logs to find the error
docker-compose logs

# Check used ports
netstat -tlnp | grep -E '3001|8000|8086|5432|9092'

# If ports are busy, stop the process
sudo kill -9 <PID>
```

### Issue: Kafka not responding

```bash
# Check Kafka logs
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka zookeeper

# Wait 15 seconds
sleep 15

# Check topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

### Issue: PostgreSQL not accepting connections

```bash
# Check logs
docker-compose logs postgres

# Restart PostgreSQL
docker-compose restart postgres

# Check connection
docker-compose exec postgres pg_isready
```

### Issue: Spark Consumer stops

```bash
# Display Spark logs
docker-compose logs -f consumer

# Check for existing Checkpoints
ls -la ~/spark_project_data/checkpoints/

# Delete Checkpoints and try again
rm -rf ~/spark_project_data/checkpoints/farm_iot_full_pipeline
docker-compose restart consumer
```

---

## 📊 Performance Monitoring

### Display resource consumption

```bash
# Monitor resources in real-time
docker stats

# Display container information
docker-compose ps --quiet | xargs docker inspect
```

### Display processed events count

```bash
# Check number of records in database
docker-compose exec postgres psql -U spark_user -d farm_dwh -c \
  "SELECT COUNT(*) as total_events FROM fact_sensor_events;"

# Check number of Kafka messages
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 --list
```

---

## 🎯 Next Steps

After successful startup:

1. ✅ Open **Grafana**: http://localhost:3001
2. ✅ Login with: **admin / admin**
3. ✅ Add PostgreSQL data source
4. ✅ Create dashboard to display data
5. ✅ Monitor logs: `docker-compose logs -f`

---

## 📞 Support

If you encounter any issues:

1. Check logs: `docker-compose logs -f`
2. Review [README_LINUX.md](README_LINUX.md)
3. Review [SETUP.md](SETUP.md)
4. Check [COMPLETED_LINUX_MIGRATION.md](COMPLETED_LINUX_MIGRATION.md)

---

**Last Update:** December 2024 | **Version:** 2.0 (Docker-based)
