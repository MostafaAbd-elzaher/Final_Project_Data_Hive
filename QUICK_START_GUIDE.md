# 🌱 Farm IoT Data Warehouse - تشغيل وإيقاف سريع

## نظرة عامة
هذا المشروع يجمع:
1. **خط أنابيب IoT** (Kafka + Spark + PostgreSQL + InfluxDB + Grafana)
2. **لوحة تحكم الويب المباشرة** (React Frontend + FastAPI Backend)

---

## 🚀 **تشغيل المشروع**

### الطريقة السريعة (موصى بها):

```bash
cd /mnt/E/MyCareer/DepiData/DataHive/FinalProject
./start_system.sh
```

**ماذا يحدث:**
- ✅ يحمل جميع متغيرات البيئة
- ✅ ينشئ جميع الحاويات (9 خدمات)
- ✅ يبدأ جميع الخدمات
- ✅ ينتظر حتى تكون جاهزة

**وقت التشغيل:**
- **المرة الأولى:** 3-5 دقائق
- **المرات التالية:** 30-60 ثانية

---

## ⏹️ **إيقاف المشروع**

```bash
cd /mnt/E/MyCareer/DepiData/DataHive/FinalProject
./stop_system.sh
```

**ملاحظة:** البيانات محفوظة! تشغيل `./start_system.sh` مجددًا سيحمل نفس البيانات.

---

## 🎯 Available Interfaces

After startup, you'll have these interfaces:

| Service | URL | Description |
|---------|-----|-------------|
| 🎨 **Main Dashboard** | http://localhost:3000 | Real-time monitoring dashboard |
| 🔌 **Backend API** | http://localhost:8000 | FastAPI REST + WebSocket |
| 📈 **Grafana** | http://localhost:3001 | Advanced analytics (Username: admin, Password: admin) |
| 💾 **InfluxDB UI** | http://localhost:8086 | Time-series database |

---

## 📊 Dashboard Components

### 1. **Landing Page** (`/`)
- Home page with options:
  - Farm Operations Dashboard
  - Warehouse System Control

### 2. **Farm Dashboard** (`/farm`)
- **Live KPIs**: Live sensor readings
- **Sensor Map**: Interactive location map
- **Trend Charts**: Trend visualizations
- **Alerts Log**: Alert history
- **Producer Control**: Data simulation control

### 3. **Warehouse Control** (`/warehouse`)
- Start/Stop Docker Compose
- Monitor service status

---

## 🔧 Manual Control

### Run Backend Only:
```bash
cd "GUI_Dashboard/backend"
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Run Frontend Only:
```bash
cd "GUI_Dashboard/frontend"
npm install
npm start
```

### Run Docker Services:
```bash
docker-compose up -d
```

### Run IoT Producer:
```bash
cd Producer
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python IotSystem_Version1.1.py
```

---

## 🔌 Data Flow

```
IoT Producer (IotSystem_Version1.1.py)
    ↓ (Kafka Topic: farmSensors)
Kafka Consumer (Backend)
    ↓ (Transform & Store)
PostgreSQL / InfluxDB
    ↓ (WebSocket)
React Dashboard (Real-time updates)
```

---

## 🛠️ Troubleshooting

### Problem: "Docker is not running"
**Solution:** Open Docker Desktop and verify it's running

### Problem: "Port 8000 already in use"
**Solution:** 
```bash
# Kill process using the port
lsof -ti:8000 | xargs kill -9
```

### Problem: "Kafka connection failed"
**Solution:** Make sure Docker Compose is running first:
```bash
docker-compose ps
docker-compose logs kafka
```

### Problem: "No data in Dashboard"
**Solution:** 
1. Verify Producer is running (`IotSystem_Version1.1.py`)
2. Check Backend logs
3. Open Developer Console in browser (F12)

---

## 📦 Project Structure

```
Final_Project_Data_Hive-main/
├── Producer/                     # IoT sensor simulator
│   └── IotSystem_Version1.1.py
├── Consumer/                     # Spark consumer
│   └── Spark_Transformation_v1.1.py
├── GUI_Dashboard/                # 🎨 GUI Dashboard
│   ├── backend/                  # FastAPI backend
│   │   ├── main.py
│   │   ├── kafka_consumer.py
│   │   └── simulation_engine.py
│   └── frontend/                 # React dashboard
│       └── src/
│           ├── App.js
│           └── components/
├── docker-compose.yml            # Infrastructure services
└── start_system.sh              # Quick start script
```

---

## 📝 Important Notes

1. **First Time Run:** Docker image download may take time
2. **Ports Used:** 3000, 8000, 3001, 8086, 9092, 5432
3. **Data:** Stored in Docker volumes

---

## ⚡ Stopping the System

```bash
# Stop Docker services
docker-compose down

# Close Backend and Frontend
# Press Ctrl+C in each window
```

---

## 🎨 Features

✅ Real-time data streaming  
✅ Interactive maps & charts  
✅ Anomaly detection  
✅ Historical analytics  
✅ Mobile-responsive design  
✅ WebSocket live updates  
✅ Multi-sensor support  

---

## 📞 Support

If you encounter issues:
1. Check Docker logs: `docker-compose logs`
2. Check Backend logs in Terminal window
3. Open Browser Console (F12) for errors

**Good Luck! 🚀**
