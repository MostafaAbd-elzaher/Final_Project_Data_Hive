# 🔧 Complete Installation and Setup Guide

**Detailed guide for installing all system tools on Linux**

---

## 📋 Table of Contents

1. [System Requirements](#system-requirements)
2. [Docker Installation (Recommended Method)](#method-1-docker--recommended)
3. [Local Linux Installation](#method-2-local-installation)
4. [Database Configuration](#-database-configuration)
5. [Installation Verification](#-installation-verification)

---

## ✅ System Requirements

- **Operating System:** Linux (Ubuntu 20.04+) or WSL
- **Processor:** Minimum 4 cores
- **Memory:** Minimum 8GB RAM
- **Storage:** 20GB free space
- **Network:** Internet connection for downloads and installation

---

## 🐳 Method 1: Docker (Recommended)

### Step 1: Install Docker and Docker Compose

#### On Ubuntu/Debian:

```bash
# Update packages
sudo apt update && sudo apt upgrade -y

# Install basic requirements
sudo apt install -y curl wget gnupg lsb-release ca-certificates

# Add Docker key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add Docker repository
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add current user to Docker group (without sudo)
sudo usermod -aG docker $USER
newgrp docker

# Enable Docker on startup
sudo systemctl enable docker
sudo systemctl start docker
```

#### Verify Installation:

```bash
docker --version
# Expected output: Docker version 20.10+

docker-compose --version
# Expected output: Docker Compose version 1.29+
```

### Step 2: Install Additional Requirements

```bash
# Python 3.10+
sudo apt install -y python3.10 python3.10-venv python3-pip

# Useful tools
sudo apt install -y git wget curl make git-flow

# Upgrade pip
pip3 install --upgrade pip setuptools wheel
```

### Step 3: Clone the Project

```bash
# Clone the repository
git clone https://github.com/MostafaAbd-elzaher/Final_Project_Data_Hive.git

# Navigate to folder
cd Final_Project_Data_Hive/FinalProject

# Or from another path:
cd /path/to/FinalProject
```

### Step 4: Install Python Requirements

```bash
# Create virtual environment (optional)
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip3 install -r requirements.txt

# Verify installation
pip3 list | grep -E "kafka|pyspark|pandas|influx"
```

### Step 5: Verify docker-compose.yml File

```bash
# Check file existence
ls -la docker-compose.yml

# Verify file validity
docker-compose config

# If everything is correct, a YAML version of the configuration will appear
```

### Step 6: Start the System

```bash
# Pull latest images
docker-compose pull

# Start all services
docker-compose up -d

# Wait 15 seconds
sleep 15

# Check services status
docker-compose ps
```

---

## 💻 Method 2: Local Installation

**Note:** This method is more complex. We recommend using Docker.

### First: Python and Libraries

```bash
# Install Python
sudo apt install -y python3.10 python3.10-venv python3-pip

# Create virtual environment
python3.10 -m venv farm_iot_env
source farm_iot_env/bin/activate

# Install requirements
pip install -r requirements.txt
```

### Second: Java (Required for Spark and Kafka)

```bash
# Install OpenJDK 17
sudo apt install -y openjdk-17-jdk

# Verify
java -version
# Expected output: openjdk version "17..."
```

### Third: Apache Kafka

```bash
# Download Kafka
cd /tmp
wget https://archive.apache.org/dist/kafka/3.5.0/kafka_2.13-3.5.0.tgz

# Extract and install
tar -xzf kafka_2.13-3.5.0.tgz
sudo mv kafka_2.13-3.5.0 /opt/kafka

# Add to PATH
echo 'export KAFKA_HOME=/opt/kafka' >> ~/.bashrc
echo 'export PATH=$KAFKA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Verify
kafka-topics.sh --version
```

### Fourth: Apache Spark

```bash
# Download Spark
cd /tmp
wget https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz

# Extract and install
tar -xzf spark-4.0.0-bin-hadoop3.tgz
sudo mv spark-4.0.0-bin-hadoop3 /opt/spark

# Add to PATH
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Verify
spark-shell --version
```

### Fifth: PostgreSQL

```bash
# Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Start service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create user and database
sudo -u postgres psql << EOF
CREATE USER spark_user WITH PASSWORD 'spark_password';
CREATE DATABASE farm_dwh OWNER spark_user;
GRANT ALL PRIVILEGES ON DATABASE farm_dwh TO spark_user;
\q
EOF

# Verify
psql -U spark_user -d farm_dwh -c "SELECT 1;"
```

### Sixth: InfluxDB

```bash
# Install InfluxDB
sudo apt install -y influxdb

# Start service
sudo systemctl start influxdb
sudo systemctl enable influxdb

# Create Bucket
influx setup \
  --org "my_org" \
  --bucket "iot_bucket" \
  --username "admin" \
  --password "admin" \
  --retention 720h \
  --force
```

### Seventh: Grafana

```bash
# Install Grafana
sudo apt install -y software-properties-common
sudo add-apt-repository "deb https://packages.grafana.com/oss/deb stable main"
sudo apt update
sudo apt install -y grafana-server

# Start service
sudo systemctl start grafana-server
sudo systemctl enable grafana-server

# Access Grafana
# http://localhost:3000 (admin/admin)
```

### Create Data Folders

```bash
# Create folder structure
mkdir -p ~/spark_project_data/output
mkdir -p ~/spark_project_data/farm_iot_parquet
mkdir -p ~/spark_project_data/checkpoints/farm_iot_full_pipeline

# Set permissions
chmod -R 755 ~/spark_project_data
```

---

## 🗄️ Database Configuration

### Create Base Tables

```sql
-- Connect to database
psql -U spark_user -d farm_dwh

-- Create fact table
CREATE TABLE fact_sensor_events (
    id SERIAL PRIMARY KEY,
    event_ts TIMESTAMP,
    location_id INT,
    date_key INT,
    soil_temperature_c FLOAT,
    air_temperature_c FLOAT,
    soil_humidity_percent FLOAT,
    air_humidity_percent FLOAT,
    soil_ph FLOAT,
    soil_salinity_ds_m FLOAT,
    light_intensity_lux FLOAT,
    water_level_percent FLOAT,
    env_health_score FLOAT,
    is_outlier_temp_z INT,
    is_outlier_hum_z INT,
    ml_anomaly_score INT,
    is_sensor_error INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create dimension table (location)
CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    location_name VARCHAR(255),
    crop_type VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
);

-- Create daily KPI table
CREATE TABLE daily_farm_kpis (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    location VARCHAR(255),
    avg_env_health_score_1d FLOAT,
    pct_time_dry FLOAT,
    anomaly_count_day INT,
    error_count_day INT,
    records_day INT,
    farm_health_grade VARCHAR(1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes (for performance improvement)
CREATE INDEX idx_fact_sensor_events_event_ts ON fact_sensor_events(event_ts);
CREATE INDEX idx_fact_sensor_events_location_id ON fact_sensor_events(location_id);
CREATE INDEX idx_daily_farm_kpis_window_start ON daily_farm_kpis(window_start);
```

### Add Initial Data

```sql
-- Add locations
INSERT INTO dim_location (location_id, location_name, crop_type, latitude, longitude) VALUES
(1, 'Cairo, Egypt', 'Tomatoes', 30.0444, 31.2357),
(2, 'Alexandria, Egypt', 'Cucumbers', 31.2001, 29.9187);

-- Verify data
SELECT * FROM dim_location;
```

---

## ✅ Installation Verification

### 1. Check Services

```bash
# Docker
docker ps

# Python
python3 --version

# Java (if local installation)
java -version

# PostgreSQL (if local installation)
psql --version
```

### 2. Test Connections

```bash
# Test Kafka (in Docker)
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Test PostgreSQL (in Docker)
docker-compose exec postgres psql -U spark_user -d farm_dwh -c "SELECT 1;"

# Test InfluxDB (in Docker)
curl http://localhost:8086/api/v2/health
```

### 3. Run Automatic Verification Script

```bash
# Run verification script
./verify_linux_compatibility.sh

# You should see: ✅ All checks passed!
```

---

## 🔐 Security Settings (for Production)

**Note:** Current settings are for development only. For production:

```bash
# Change passwords in .env
POSTGRES_PASSWORD=strong_password_here
INFLUX_TOKEN=secure_token_here

# Update docker-compose.yml
# - Add volumes for data
# - Configure resource limits
# - Use HTTPS instead of HTTP
# - Use secrets instead of environment variables

# Restart services
docker-compose restart
```

---

## 📞 Troubleshooting

### Problem: Cannot open Docker port

```bash
# Check permissions
groups $USER

# If not in docker group
sudo usermod -aG docker $USER
newgrp docker
```

### Problem: PostgreSQL won't start

```bash
# View logs
docker-compose logs postgres

# Delete old data
docker-compose down -v
docker-compose up -d
```

### Problem: Kafka doesn't respond

```bash
# Restart Kafka
docker-compose restart kafka zookeeper

# Wait 15 seconds
sleep 15

# Test connection
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

---

## 📚 References

- [Docker Documentation](https://docs.docker.com/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

---

**Last Updated:** December 2024 | **Version:** 2.0
        `export SPARK_HOME=/opt/spark`
    * Refresh your terminal: `source ~/.bashrc`

---

## 5. ⏳ InfluxDB & Telegraf

### InfluxDB (Monitoring Database)

1.  Follow the [Official Install Guide](https://docs.influxdata.com/influxdb/v2/install/) for your OS.
2.  **Start and enable the service:**
    ```bash
    sudo systemctl start influxdb
    sudo systemctl enable influxdb
    ```
3.  **Initial Setup:**
    * Go to `http://localhost:8086`.
    * Create a user and password.
    * **Very Important:** Create an `Organization` (e.g., `my_org`) and a `Bucket` (e.g., `iot_bucket`).
    * Go to the `API Tokens` section and generate a new `All-Access Token`. **Save this token.**

### Telegraf (Data Collector)

1.  Follow the [Official Install Guide](https://docs.influxdata.com/telegraf/v1/install/) for your OS.
2.  **Create Configuration File:**
    * Create a file named `telegraf.conf`.
    * Copy the content below, **and update the InfluxDB values**:

    ```toml
    [agent]
      interval = "5s"
      flush_interval = "5s"

    # 1. Input: Read from Kafka
    [[inputs.kafka_consumer]]
      brokers = ["localhost:9092"]
      topics = ["farmSensors"]
      consumer_group = "telegraf_monitor_group"
      offset = "oldest"
      data_format = "json"

    # 2. Output: Write to InfluxDB
    [[outputs.influxdb_v2]]
      urls = ["http://localhost:8086"]
      token = "YOUR_INFLUXDB_TOKEN_HERE"    # <--- REPLACE
      organization = "YOUR_ORG_NAME_HERE"   # <--- REPLACE
      bucket = "YOUR_BUCKET_NAME_HERE"      # <--- REPLACE
    ```
3.  **Run Command (Reference):**
    * `telegraf --config /your/path/to/telegraf.conf`

---

## 6. 📊 Grafana (Dashboard)

1.  Follow the [Official Install Guide](https://grafana.com/docs/grafana/latest/installation/) for your OS.
2.  **Start and enable the service:**
    ```bash
    sudo systemctl start grafana-server
    sudo systemctl enable grafana-server
    ```
3.  **Login:**
    * Go to `http://localhost:3000`.
    * (Default user/pass: `admin`/`admin`).
4.  Add `PostgreSQL` and `InfluxDB` as Data Sources as shown in the main `README.md`.
