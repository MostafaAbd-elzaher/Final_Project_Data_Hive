# 🌱 Farm IoT Dashboard Project - Quick Start Guide

## 📋 Overview
A comprehensive project for monitoring and analyzing smart farm data in real-time using:
- **IoT Sensors Simulator** - IoT sensor simulator
- **Apache Kafka** - Message queue system
- **Apache Spark** - Data processing engine
- **PostgreSQL** - Analytical database
- **InfluxDB** - Time-series database
- **Grafana** - Advanced dashboards
- **FastAPI** - Backend API interface
- **HTML/JS Dashboard** - Interactive user interface with Chatbot

## 🚀 Running the Project (One way only!)

### Requirements:
- ✅ Docker Desktop installed and running
- ✅ At least 8GB RAM
- ✅ 10GB free disk space

### Setup Steps:

#### 1️⃣ Open Command Prompt in project folder
```cmd
cd d:\DEPI\FINALLLLLLL\Final_Project_Data_Hive-main
```

#### 2️⃣ Run the main file
```cmd
START_PROJECT.bat
```

**That's it!** 🎉

The file will:
- ✅ Clean up any old containers
- ✅ Build all images
- ✅ Run all services
- ✅ Open the dashboard in the browser automatically

## 🌐 Access Services

After running, you can access:

| Service | URL | Description |
|---------|-----|-------------|
| **🎨 Main Dashboard** | http://localhost:3000 | Full interface with Chatbot |
| **🔌 Backend API** | http://localhost:8000 | FastAPI Backend |
| **📈 Grafana** | http://localhost:3001 | Advanced dashboards (admin/admin) |
| **💾 InfluxDB** | http://localhost:8086 | Time-series database |

## 🤖 Using Chatbot

The Chatbot is located in the bottom right corner of the main Dashboard.

**Example questions:**
- "What is the system status?"
- "How is the humidity?"
- "Do we need irrigation?"
- "What is the temperature?"

## 📊 Dashboard Components

### The main page contains:
1. **KPI Cards** - Key performance indicators
2. **Real-time Chart** - Live temperature chart
3. **Map** - Farm location map
4. **Chatbot** - Interactive smart assistant
5. **Grafana Integration** - Button to open Grafana

## 🛠️ Useful Commands

### View services status:
```cmd
docker-compose ps
```

### View Logs:
```cmd
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f producer
docker-compose logs -f spark-consumer
docker-compose logs -f backend-dashboard
docker-compose logs -f frontend-dashboard
```

### Stop the project:
```cmd
docker-compose down
```

### Stop and delete all data:
```cmd
docker-compose down -v
```

### Rebuild a specific service:
```cmd
docker-compose up -d --build [service-name]
```

## 🔧 Troubleshooting

### Problem: "Docker is not running"
**Solution:** Start Docker Desktop and wait until it's ready

### Problem: "Port already in use"
**Solution:** 
```cmd
# Stop old services
docker-compose down

# Or change port in docker-compose.yml
```

### Problem: Dashboard not showing data
**Solution:**
1. Verify Producer is running: `docker-compose logs -f producer`
2. Verify Spark Consumer is running: `docker-compose logs -f spark-consumer`
3. Wait 30-60 seconds for data flow to start

### Problem: Chatbot not responding
**Solution:**
1. Check Backend: http://localhost:8000
2. View logs: `docker-compose logs -f backend-dashboard`

## 📁 Project Structure

```
Final_Project_Data_Hive-main/
├── Producer/                    # IoT data generator
│   └── IotSystem_Version1.1.py
├── Consumer/                    # Spark processor
│   └── Spark_Transformation_v1.1.py
├── GUI_Dashboard/
│   ├── backend/                # FastAPI Backend
│   │   ├── main.py
│   │   ├── database.py
│   │   ├── kafka_consumer.py
│   │   └── Dockerfile
│   └── frontend/               # HTML Dashboard
│       ├── index.html
│       ├── nginx.conf
│       └── Dockerfile
├── grafana/                    # Grafana settings
├── docker-compose.yml          # Services definition
├── START_PROJECT.bat           # Main startup file ⭐
└── README_AR.md               # This file

```

## 🎯 Data Flow

```
IoT Producer → Kafka → Spark Consumer → PostgreSQL + InfluxDB
                                              ↓
                                         Grafana
                                              ↓
Backend API ← Kafka (Real-time) → WebSocket → Frontend Dashboard
```

## 📞 Support

If you encounter any issues:
1. Check logs: `docker-compose logs -f`
2. Verify Docker Desktop is running
3. Ensure required ports are available
4. Try restarting: `docker-compose down && START_PROJECT.bat`

## 📝 Important Notes

- ⚠️ **Do not use** `run_project.bat` or `LAUNCH_COMPLETE_SYSTEM.bat` - use `START_PROJECT.bat` only
- ⏱️ First run may take 5-10 minutes to load images
- 💾 Data is saved in Docker Volumes and persists after restart
- 🔄 To delete all data and start fresh: `docker-compose down -v`

## ✨ Features

- ✅ Single command startup
- ✅ Complete interface
- ✅ Interactive smart chatbot
- ✅ Real-time data
- ✅ Multiple dashboards (Dashboard + Grafana)
- ✅ Advanced data processing with Spark
- ✅ Multiple databases (PostgreSQL + InfluxDB)

---

**Developed by:** Data Hive Team  
**Date:** November 2025  
**Version:** 2.0
