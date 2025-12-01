# 🌱 Farm IoT Dashboard - دليل التشغيل السريع

## نظرة عامة
هذا المشروع يجمع بين:
1. **IoT Data Pipeline** (Kafka + Spark + PostgreSQL + InfluxDB + Grafana)
2. **Real-time Web Dashboard** (React Frontend + FastAPI Backend)

---

## 📋 المتطلبات

### 1. البرامج المطلوبة:
- ✅ **Docker Desktop** (يجب أن يكون يعمل)
- ✅ **Python 3.9+**
- ✅ **Node.js 16+** و **npm**

### 2. تأكد من تشغيل Docker:
```bash
docker --version
docker ps
```

---

## 🚀 طريقة التشغيل السريعة (One-Click Start)

### الطريقة الأولى: تشغيل كامل النظام مع لوحة التحكم

1. **افتح موجه الأوامر** في مجلد المشروع:
   ```
   d:\DEPI\FINALLLLLLL\Final_Project_Data_Hive-main
   ```

2. **شغّل السكريبت الآلي:**
   ```batch
   START_GUI_DASHBOARD.bat
   ```

3. **انتظر حتى تفتح نوافذ:**
   - نافذة Backend API (Python)
   - نافذة Frontend Dashboard (React)
   - المتصفح سيفتح تلقائياً على `http://localhost:3000`

4. **شغّل IoT Producer** (في نافذة منفصلة):
   ```batch
   cd Producer
   python IotSystem_Version1.1.py
   ```

---

## 🎯 الواجهات المتاحة

بعد التشغيل، ستكون عندك الواجهات التالية:

| الخدمة | الرابط | الوصف |
|--------|--------|-------|
| 🎨 **Dashboard الرئيسي** | http://localhost:3000 | Real-time monitoring dashboard |
| 🔌 **Backend API** | http://localhost:8000 | FastAPI REST + WebSocket |
| 📈 **Grafana** | http://localhost:3001 | Advanced analytics (Username: admin, Password: admin) |
| 💾 **InfluxDB UI** | http://localhost:8086 | Time-series database |

---

## 📊 مكونات Dashboard

### 1. **Landing Page** (`/`)
- صفحة البداية مع خيارات:
  - Farm Operations Dashboard
  - Warehouse System Control

### 2. **Farm Dashboard** (`/farm`)
- **Live KPIs**: عرض مباشر للقراءات
- **Sensor Map**: خريطة تفاعلية للموقع
- **Trend Charts**: رسوم بيانية للتوجهات
- **Alerts Log**: سجل التنبيهات
- **Producer Control**: تحكم في تشغيل البيانات

### 3. **Warehouse Control** (`/warehouse`)
- تشغيل/إيقاف Docker Compose
- مراقبة حالة الخدمات

---

## 🔧 التحكم اليدوي

### تشغيل Backend فقط:
```bash
cd "GUI_Dashboard\backend"
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### تشغيل Frontend فقط:
```bash
cd "GUI_Dashboard\frontend"
npm install
npm start
```

### تشغيل Docker Services:
```bash
docker-compose up -d
```

### تشغيل IoT Producer:
```bash
cd Producer
set KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python IotSystem_Version1.1.py
```

---

## 🔌 تدفق البيانات

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

## 🛠️ استكشاف الأخطاء

### المشكلة: "Docker is not running"
**الحل:** افتح Docker Desktop وتأكد من تشغيله

### المشكلة: "Port 8000 already in use"
**الحل:** 
```bash
# إيقاف العملية التي تستخدم البورت
netstat -ano | findstr :8000
taskkill /PID <PID> /F
```

### المشكلة: "Kafka connection failed"
**الحل:** تأكد من تشغيل Docker Compose أولاً:
```bash
docker-compose ps
docker-compose logs kafka
```

### المشكلة: "No data في Dashboard"
**الحل:** 
1. تأكد من تشغيل Producer (`IotSystem_Version1.1.py`)
2. تحقق من Backend logs
3. افتح Developer Console في المتصفح (F12)

---

## 📦 هيكل المشروع

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
└── START_GUI_DASHBOARD.bat       # Quick start script
```

---

## 📝 ملاحظات مهمة

1. **أول مرة تشغيل:** قد يأخذ وقت لتحميل Docker images
2. **المنافذ المستخدمة:** 3000, 8000, 3001, 8086, 9092, 5432
3. **البيانات:** يتم حفظها في Docker volumes

---

## ⚡ إيقاف النظام

```bash
# إيقاف Docker services
docker-compose down

# إغلاق Backend و Frontend
# اضغط Ctrl+C في كل نافذة
```

---

## 🎨 المميزات

✅ Real-time data streaming  
✅ Interactive maps & charts  
✅ Anomaly detection  
✅ Historical analytics  
✅ Mobile-responsive design  
✅ WebSocket live updates  
✅ Multi-sensor support  

---

## 📞 الدعم

في حالة وجود مشاكل:
1. تحقق من Docker logs: `docker-compose logs`
2. تحقق من Backend logs في نافذة الـ Terminal
3. افتح Browser Console (F12) للأخطاء

**Good Luck! 🚀**
