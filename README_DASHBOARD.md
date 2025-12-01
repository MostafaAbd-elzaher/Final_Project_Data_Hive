# 🌱 Farm IoT Real-Time Monitoring Dashboard

<div align="center">

![Dashboard Preview](https://via.placeholder.com/800x400/1a1a2e/16c784?text=Farm+IoT+Dashboard)

**Real-time IoT sensor monitoring system with beautiful web dashboard**

[Quick Start](#-quick-start) • [Features](#-features) • [Architecture](#-architecture) • [Documentation](#-documentation)

</div>

---

## 📖 Overview

This project combines a complete **IoT Data Pipeline** with a modern **Real-Time Web Dashboard** for monitoring farm sensors. It includes:

- 🔴 **Real-time sensor data streaming** (Kafka)
- 📊 **Interactive web dashboard** (React + FastAPI)
- 📈 **Advanced analytics** (Grafana + InfluxDB)
- 💾 **Data warehousing** (PostgreSQL + Spark)
- 🤖 **Automated IoT simulation** (Python sensors)

---

## ⚡ Quick Start

### Prerequisites
- ✅ Docker Desktop (must be running)
- ✅ Python 3.9+
- ✅ Node.js 16+ and npm

### Launch in 3 Steps:

```batch
# Step 1: Navigate to project folder
cd d:\DEPI\FINALLLLLLL\Final_Project_Data_Hive-main

# Step 2:  Make sure Docker is running
docker --version

# Step 3: Launch everything! 🚀
LAUNCH_COMPLETE_SYSTEM.bat
```

That's it! 🎉 Four windows will open:
1. 🐳 **Docker Services** - Kafka, PostgreSQL, InfluxDB, Grafana
2. 🔌 **Backend API** - FastAPI server at `http://localhost:8000`
3. 🎨 **Frontend Dashboard** - React app at `http://localhost:3000`
4. 📡 **IoT Producer** - Simulated sensor data streaming

Your browser will automatically open the dashboard.

---

## 🎯 Available Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| 🎨 **Main Dashboard** | http://localhost:3000 | - |
| 🔌 **API Server** | http://localhost:8000 | - |
| 📖 **API Docs** | http://localhost:8000/docs | - |
| 📈 **Grafana** | http://localhost:3001 | admin / admin |
| 💾 **InfluxDB** | http://localhost:8086 | - |

---

## ✨ Features

### 🎨 Web Dashboard Features:
- ✅ **Real-time KPIs** - Live sensor readings
- ✅ **Interactive Maps** - Leaflet-based location tracking
- ✅ **Live Charts** - Chart.js trend visualization
- ✅ **Anomaly Alerts** - Automatic outlier detection
- ✅ **Producer Control** - Start/stop data simulation
- ✅ **Warehouse Management** - Docker service control
- ✅ **WebSocket Updates** - Sub-second latency
- ✅ **Responsive Design** - Mobile-friendly UI

### 📊 Monitored Sensors:
- 🌡️ Air Temperature (°C)
- 🌡️ Soil Temperature (°C)
- 💧 Air Humidity (%)
- 💧 Soil Moisture (%)
- ⚗️ Soil pH Level
- 🧂 Soil Salinity (dS/m)
- ☀️ Light Intensity (lux)
- 🚰 Water Level (%)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         USER INTERFACE                          │
├─────────────────────────────────────────────────────────────────┤
│  🎨 React Dashboard (localhost:3000)                            │
│  📈 Grafana Analytics (localhost:3001)                          │
└────────────────────────────┬────────────────────────────────────┘
                             │ WebSocket + REST
┌────────────────────────────▼────────────────────────────────────┐
│               🔌 FastAPI Backend (localhost:8000)               │
│  • WebSocket Server  • REST API  • Kafka Consumer               │
└────────────┬───────────────────────────────┬────────────────────┘
             │                               │
     ┌───────▼────────┐              ┌──────▼───────┐
     │  📡 Kafka      │              │ 💾 Databases │
     │  (Port 9092)   │              │  PostgreSQL  │
     │                │              │  InfluxDB    │
     └───────▲────────┘              └──────────────┘
             │
     ┌───────┴────────┐
     │ 🤖 IoT Producer│
     │ Sensor Sim.    │
     └────────────────┘
```

### Data Flow:

```
IoT Sensors → Kafka (farmSensors) → Backend Consumer → Database
                ↓                           ↓
           Spark Consumer             WebSocket Push
                ↓                           ↓
         Data Warehouse               React Dashboard
```

---

## 📂 Project Structure

```
Final_Project_Data_Hive-main/
│
├── 🚀 LAUNCH_COMPLETE_SYSTEM.bat    ← **START HERE!**
├── 📘 QUICK_START_GUIDE.md          ← Detailed guide
│
├── Producer/                        # IoT data generator
│   └── IotSystem_Version1.1.py
│
├── Consumer/                        # Spark processing
│   └── Spark_Transformation_v1.1.py
│
├── GUI_Dashboard/           # 🎨 GUI Dashboard
│   ├── backend/             # FastAPI server
│   │   ├── main.py
│   │   ├── kafka_consumer.py
│   │   ├── database.py
│   │   └── simulation_engine.py
│   │
│   └── frontend/            # React dashboard
│       └── src/
│           ├── App.js
│           └── components/
│                   ├── Dashboard.js
│                   ├── LandingPage.js
│                   ├── LocationMap.js
│                   ├── TrendChart.js
│                   ├── AlertsLog.js
│                   └── ProducerControl.js
│
├── grafana/                         # Grafana configs
│   ├── dashboards/
│   └── provisioning/
│
├── docker-compose.yml               # Infrastructure
│
└── Helper Scripts:
    ├── start_backend.bat
    ├── start_frontend.bat
    └── start_producer.bat
```

---

## 🛠️ Manual Startup (Alternative)

If you prefer to start services individually:

### 1. Start Docker Services:
```bash
docker-compose up -d
```

### 2. Start Backend:
```bash
start_backend.bat
# OR manually:
cd "GUI_Dashboard\backend"
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### 3. Start Frontend:
```bash
start_frontend.bat
# OR manually:
cd "GUI_Dashboard\frontend"
npm install
npm start
```

### 4. Start Producer:
```bash
start_producer.bat
# OR manually:
cd Producer
set KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python IotSystem_Version1.1.py
```

---

## 🐛 Troubleshooting

### Problem: "Docker is not running"
**Solution:** Start Docker Desktop and wait for it to fully initialize

### Problem: "Port already in use"
**Solution:** 
```bash
# Kill process on port 8000
netstat -ano | findstr :8000
taskkill /PID <PID_NUMBER> /F
```

### Problem: "Kafka connection failed"
**Solution:**
```bash
# Check Kafka status
docker-compose ps kafka
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka
```

### Problem: "No data in dashboard"
**Solution:**
1. Check if Producer is running (should show sensor data in terminal)
2. Check Backend logs for Kafka connection
3. Open browser DevTools (F12) → Console tab
4. Verify WebSocket connection is established

### Problem: "Frontend won't start"
**Solution:**
```bash
cd "final project excution\final project excution\frontend"
# Delete node_modules and reinstall
rm -rf node_modules package-lock.json
npm install
npm start
```

---

## 📊 Dashboard Pages

### 1. Landing Page (`/`)
- Entry point with navigation
- System status overview
- Quick access to Farm and Warehouse interfaces

### 2. Farm Dashboard (`/farm`)
- **KPI Cards**: Total Yield, Avg Moisture, Active Sensors
- **Location Map**: Interactive Leaflet map with sensor pins
- **Trend Charts**: Historical data visualization
- **Alerts Log**: Real-time anomaly notifications
- **Producer Control**: Start/Stop data simulation
- **Live Sensor Feed**: WebSocket-powered updates

### 3. Warehouse Control (`/warehouse`)
- Docker Compose management
- Service status monitoring
- One-click start/stop

---

## 🔧 Configuration

### Environment Variables

Create `.env` file in project root:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=farm_dwh

# InfluxDB
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=my_super_secret_token
INFLUX_ORG=my_org
INFLUX_BUCKET=iot_bucket
```

---

## 📖 API Documentation

Once the backend is running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Main Endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/kpis/daily` | Get daily KPI metrics |
| GET | `/api/trends/historical` | Get historical trends |
| GET | `/api/locations` | Get sensor locations |
| POST | `/api/simulation/start` | Start simulation |
| POST | `/api/simulation/stop` | Stop simulation |
| POST | `/api/warehouse/start` | Start warehouse services |
| WS | `/ws/live` | WebSocket for live data |

---

## 🎓 Learning Resources

- **Kafka Basics**: Understanding message streaming
- **FastAPI**: Modern Python web framework
- **React**: Frontend framework fundamentals
- **Docker Compose**: Multi-container orchestration
- **WebSockets**: Real-time bidirectional communication

---

## 📝 Notes

- **First Run**: Docker images download may take 5-10 minutes
- **Ports Used**: 3000, 8000, 3001, 8086, 9092, 5432, 2181
- **Data Persistence**: Stored in Docker volumes
- **Memory**: Recommended 4GB+ RAM available for Docker

---

## ⚠️ Stopping the System

```bash
# Stop all Docker services
docker-compose down

# Stop all services including volumes (full cleanup)
docker-compose down -v

# Or just close all terminal windows and run:
docker-compose down
```

---

## 🤝 Contributing

This project was developed for the DEPI Final Project 2025.

---

## 📞 Support

If you encounter issues:
1. Check `QUICK_START_GUIDE.md` for detailed instructions
2. Review Docker logs: `docker-compose logs`
3. Check Backend terminal for errors
4. Open Browser Console (F12) for frontend errors

---

## 📜 License

Educational project - DEPI 2025

---

<div align="center">

**Built with ❤️ using Kafka, Spark, FastAPI, React, and Docker**

🌱 Happy Monitoring! 🌱

</div>
