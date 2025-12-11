# Running Data Hive IoT Pipeline Project on Docker

## ✅ Completed Preparations

All required files for running the entire project on Docker have been completed:

### Files Created:
1. ✅ **Dockerfile.producer** - Docker image for Producer (IoT simulator)
2. ✅ **Dockerfile.consumer** - Docker image for Spark Consumer
3. ✅ **docker-compose.yml** - Complete configuration for all services
4. ✅ **docker-entrypoint-producer.sh** - Producer startup script
5. ✅ **docker-entrypoint-consumer.sh** - Spark Consumer startup script
6. ✅ **.env** - Environment variables
7. ✅ Updated Producer and Consumer code to work with Docker

### Configured Services:
- ✅ Zookeeper
- ✅ Kafka (with health checks)
- ✅ PostgreSQL (with health checks)
- ✅ InfluxDB (with health checks)
- ✅ Grafana (with health checks)  
- ✅ Producer (IoT Simulator)
- ✅ Spark Consumer (Data Pipeline)

---

## 📋 Runtime Requirements

### 1. Install Docker Desktop

**Must install Docker Desktop on Linux/WSL:**

1. Download Docker Desktop from: https://www.docker.com/products/docker-desktop/
2. Install the software
3. Start Docker Desktop
4. Verify Docker is running by opening terminal and running:
   ```bash
   docker --version
   docker compose version
   ```

### 2. Other Requirements
- Linux (Ubuntu 20.04+) or WSL 2
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

---

## 🚀 Running the Project

### Step 1: Verify Docker Desktop
```bash
# Check that Docker is running
docker ps
```

### Step 2: Navigate to Project Folder
```bash
cd /mnt/E/MyCareer/DepiData/DataHive/FinalProject
```

### Step 3: Build All Images
```bash
docker compose build
```
**This will take several minutes on first run**

### Step 4: Run All Services
```bash
docker compose up
```

**Or run in background:**
```bash
docker compose up -d
```

---

## 📊 Accessing Services

After running the services, you can access:

| Service | URL | Username | Password |
|---------|-----|----------|----------|
| **Grafana** | http://localhost:3000 | admin | admin |
| **InfluxDB** | http://localhost:8086 | my_user | my_password |
| **PostgreSQL** | localhost:5432 | spark_user | spark_password |
| **Kafka** | localhost:9092 | - | - |

---

## 🔍 Monitoring Services

### View Service Status
```bash
docker compose ps
```

### View logs for specific service
```bash
# Producer logs
docker compose logs -f producer

# Spark Consumer logs
docker compose logs -f spark-consumer

# Kafka logs
docker compose logs -f kafka

# PostgreSQL logs
docker compose logs -f postgres
```

### View logs for all services
```bash
docker compose logs -f
```

---

## ✅ Verify Operations

### 1. Verify Producer
```bash
docker compose logs producer
```
**You should see:**
- ✅ Successfully connected to Kafka
- 📬 Data sent to Kafka Topic

### 2. Verify Spark Consumer
```bash
docker compose logs spark-consumer
```
**You should see:**
- ✅ Spark session started successfully
- Writing to Kafka topics
- Writing to DWH tables

### 3. Verify Data in PostgreSQL
```bash
docker compose exec postgres psql -U spark_user -d farm_dwh -c "\dt"
```
**You should see tables:**
- dim_location
- dim_date
- fact_sensor_events
- daily_farm_kpis
- farm_dry_sessions

### 4. View Data
```bash
docker compose exec postgres psql -U spark_user -d farm_dwh -c "SELECT COUNT(*) FROM fact_sensor_events;"
```

---

## 🛑 Stopping Services

### Temporary Stop (preserve data)
```bash
docker compose stop
```

### Stop and Remove Containers (preserve data)
```bash
docker compose down
```

### Stop and Delete Everything (including data)
```bash
docker compose down -v
```

---

## 🔧 Troubleshooting

### Problem: Services Won't Start
```bash
# Check Docker status
docker info

# Check used ports
netstat -ano | grep 9092
netstat -ano | grep 5432
netstat -ano | grep 3000
```

### Problem: Out of Memory
- Open Docker Desktop
- Go to Settings → Resources
- Increase Memory allocation to at least 8GB

### Problem: Producer Can't Connect to Kafka
```bash
# Check logs
docker compose logs kafka
docker compose logs producer

# Restart services
docker compose restart
```

### Rebuild Specific Image
```bash
docker compose build --no-cache producer
docker compose build --no-cache spark-consumer
```

---

## 📝 Important Notes

1. **First Time:** Building images will take 10-15 minutes
2. **Waiting:** After `docker compose up`, wait 1-2 minutes for all services to stabilize
3. **Order:** Services will start in correct order automatically thanks to `depends_on` and `healthcheck`
4. **Data:** All data preserved in Docker volumes (postgres_data, influxdb_data, etc.)
5. **Logs:** Use `docker compose logs -f` to monitor what's happening

---

## 🎯 Next Steps

After successfully running the project:

1. Open Grafana at http://localhost:3000
2. Add PostgreSQL data source:
   - Host: `postgres:5432`
   - Database: `farm_dwh`
   - User: `spark_user`
   - Password: `spark_password`
3. Add InfluxDB data source:
   - URL: `http://influxdb:8086`
   - Token: `my_super_secret_token`
   - Organization: `my_org`
   - Bucket: `iot_bucket`
4. Create Dashboard to display live data!

---

## 📞 Support

If you encounter any issues:
1. Verify Docker Desktop is running
2. Check logs: `docker compose logs -f`
3. Restart services: `docker compose restart`
4. As a last resort, rebuild: `docker compose down -v && docker compose build --no-cache && docker compose up`
