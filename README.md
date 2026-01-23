# 🌱 Smart Farm Greenhouse Monitoring and Analysis System
## Farm IoT Real-time Monitoring & Analytics System

**An integrated system for monitoring and analyzing sensor data in agricultural greenhouses in real-time**

---
## 🏆Award-Winning Project:
Successfully achieved 1st Place in the Data Engineering Track at the Digital Egypt Pioneers Initiative (DEPI) in collaboration with Eyouth.

## 🏗️Project Overview
Data Hive is a robust, end-to-end Data Pipeline designed to handle complex data workflows. This project demonstrates proficiency in modern data engineering practices, from ingestion to visualization.

## 🌟Key Achievements & Recognition
Ranked #1: Best project among all participants in the DEPI Data Engineering Track.
Technical Excellence: Recognized for optimized ETL process design and scalable Data Architecture.
Professional Implementation: Implemented using industry-standard tools ensuring data integrity and performance.
---

## 🎯 Project Objective

Build a comprehensive and integrated system for:
- ✅ **Simulating sensor data** in agricultural greenhouses (temperature, humidity, salinity, etc.)
- ✅ **Real-time data processing** using Apache Spark Streaming
- ✅ **Data cleaning and enhancement** (Cleaning, Enrichment, Anomaly Detection)
- ✅ **Storing processed data** in a Data Warehouse and Data Lake
- ✅ **Generating analytics and alerts** (KPIs, Trends, Alerts)
- ✅ **Visualizing data** through interactive dashboards (Grafana)

---

## 🏛️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Farm IoT Pipeline Architecture                │
└─────────────────────────────────────────────────────────────────┘

1️⃣ DATA INGESTION (Ingestion Layer)
   └─> Producer (IotSystem_Version1.1.py)
       • Simulating data from 8 sensor devices
       • Generating JSON messages
       • Sending to Kafka Topic: "farmSensors"
       • Saving locally in CSV and JSON

2️⃣ DATA STREAMING (Processing Layer)
   └─> Spark Structured Streaming (Spark_Transformation_v1.1.py)
       • Reading data from Kafka
       • Cleaning and enrichment
       • Anomaly Detection (Z-Score)
       • Computing environmental health scores
       • Data aggregation (Windowing)

3️⃣ DATA STORAGE (Storage Layer)
   ├─> Gold Layer (Final Data Warehouse)
   │   └─ PostgreSQL Database
   │       • Fact Tables: fact_sensor_events
   │       • Dimension Tables: dim_location, dim_date
   │       • Aggregates: daily_farm_kpis
   │
   ├─> Silver Layer (Data Lake)
   │   └─ Delta Lake
   │       • All processed events
   │       • Data backup
   │
   └─> Message Brokers (Kafka Topics)
       • farmInsights (Detailed events)
       • farmTrends (5-minute trends)
       • farmKpis (Daily KPIs)

4️⃣ DATA VISUALIZATION (Presentation Layer)
   ├─> Grafana Dashboards
   │   └─ Connected to PostgreSQL and InfluxDB
   │       • Real-time dashboards
   │       • Interactive charts
   │       • Automatic alerts
   │
   └─> REST API (FastAPI)
       └─ Backend Dashboard
           • Serving data via HTTP
           • WebSocket for instant updates
```

---

## 🛠️ Tools and Technologies Used

### Backend Technologies
| Technology | Version | Function |
|---------|--------|--------|
| **Apache Kafka** | 7.5.0 | Message Broker |
| **Apache Spark** | 4.0.0 | Distributed Stream Processing |
| **Python** | 3.10+ | Main Programming Language |
| **PostgreSQL** | 15 | Data Warehouse |
| **InfluxDB** | 2.7 | Time-Series Database |
| **Delta Lake** | 3.0.0 | Data Lake Storage |

### Frontend Technologies
| Technology | Version | Function |
|---------|--------|--------|
| **Grafana** | 10.0.0 | Data Visualization |
| **FastAPI** | Latest | REST API Backend |
| **React** | Latest | Frontend Dashboard |

### Infrastructure
| Tool | Version | Function |
|------|--------|--------|
| **Docker** | 20.10+ | Container Management |
| **Docker Compose** | 1.29+ | Service Orchestration |
| **Zookeeper** | 7.5.0 | Service Coordination |

---

## 📊 Data Processing

### Data Sources
- **8 simulated sensor devices** measuring:
  - 🌡️ Soil and air temperature
  - 💧 Soil and air humidity
  - ⚗️ Soil acidity (pH)
  - 🧂 Soil salinity
  - ☀️ Light intensity
  - 🚰 Water level

### Data Processing
- ✅ **Data Cleaning** (Filtering & Validation)
- ✅ **Anomaly Detection** (Z-Score Outlier Detection)
- ✅ **Windowed Aggregations**
- ✅ **Environmental Health Scores**
- ✅ **Trend Analysis** (5 min rolling windows)
- ✅ **Daily KPIs**

### Outputs
- 📊 **Fact Tables** in PostgreSQL
- 🗂️ **Data Lake** in Delta Lake (Parquet)
- 📨 **Kafka Topics** for real-time processing
- 📈 **Grafana Dashboards** for visualization

---

## 🚀 Quick Start

### Requirements
- Docker and Docker Compose installed
- Linux (Ubuntu 20.04+) or WSL
- 8GB RAM and 20GB storage space

### Steps
```bash
# 1. Clone the project
git clone https://github.com/MostafaAbd-elzaher/Final_Project_Data_Hive.git
cd FinalProject

# 2. Verify compatibility
./verify_linux_compatibility.sh

# 3. Interactive quick start
./QUICK_START_LINUX.sh

# Or manual start:
./start_system.sh

# 4. Access services
# Grafana: http://localhost:3001 (admin/admin)
# Backend: http://localhost:8000
# Frontend: http://localhost:3000
```

---

## 📚 Main Files

| File | Description |
|------|------|
| `Producer/IotSystem_Version1.1.py` | Sensor simulator |
| `Consumer/Spark_Transformation_v1.1.py` | Data processor (Spark Pipeline) |
| `GUI_Dashboard/backend/main.py` | REST API (FastAPI) |
| `GUI_Dashboard/backend/database.py` | Database connections |
| `GUI_Dashboard/backend/kafka_consumer.py` | Kafka reader |
| `README_LINUX.md` | Detailed Linux guide |
| `SETUP.md` | Detailed installation guide |
| `RUN.md` | Running guide |

---

## 🔗 Important Links

- 📖 [Installation Guide](SETUP.md)
- ▶️ [Running Guide](RUN.md)
- 🐧 [Linux Guide](README_LINUX.md)
- 📋 [Changes Summary](LINUX_MODIFICATIONS_SUMMARY.md)
- ✅ [Completion Status](COMPLETED_LINUX_MIGRATION.md)

---

## 📞 Support and Help

For more information, refer to the following files:
- `README_LINUX.md` - Comprehensive Linux running guide
- `SETUP.md` - Detailed explanation of each installation step
- `RUN.md` - Running and stopping commands

---

**Last Update:** December 2024 | **Version:** 2.0 (Linux Compatible) | **Status:** ✅ Production Ready
"/your/path/to/Spark_Transformation_v1.0.py"
```

### 4. Start Ingestion & Visualization

**Start Producer:** (Wait for the Spark Consumer to start successfully)
```
python3 /your/path/to/IotSystem_Version1.1.py
```
**Start Grafana:** (Depends on your OS, e.g., ```sudo systemctl start grafana-server```)

Open **http://localhost:3000** and start building your dashboards.

## DWH Schema 📊
The pipeline builds the following Star Schema in PostgreSQL:

**dim_location (Dimension Table):** 
Contains location_id, location_name, crop_type, latitude, longitude.

**fact_sensor_events (Fact Table):** 
Contains all metrics (e.g., soil_temperature_c) and ML results, linked via location_id.

**daily_farm_kpis (Aggregate Table):**
 A daily summary of health scores and grades.
farm_dry_sessions (Insight Table): Records the duration of "dry spell" sessions (Sessionization).