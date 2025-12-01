# 🌱 مشروع Farm IoT Dashboard - دليل التشغيل السريع

## 📋 نظرة عامة
مشروع متكامل لمراقبة وتحليل بيانات المزارع الذكية في الوقت الفعلي باستخدام:
- **IoT Sensors Simulator** - محاكي أجهزة الاستشعار
- **Apache Kafka** - نظام الرسائل
- **Apache Spark** - معالجة البيانات
- **PostgreSQL** - قاعدة بيانات تحليلية
- **InfluxDB** - قاعدة بيانات السلاسل الزمنية
- **Grafana** - لوحات تحكم متقدمة
- **FastAPI** - واجهة برمجية خلفية
- **HTML/JS Dashboard** - واجهة مستخدم تفاعلية مع Chatbot

## 🚀 تشغيل المشروع (طريقة واحدة فقط!)

### المتطلبات:
- ✅ Docker Desktop مثبت ومشغل
- ✅ 8GB RAM على الأقل
- ✅ 10GB مساحة فارغة

### خطوات التشغيل:

#### 1️⃣ افتح Command Prompt في مجلد المشروع
```cmd
cd d:\DEPI\FINALLLLLLL\Final_Project_Data_Hive-main
```

#### 2️⃣ شغل الملف الرئيسي
```cmd
START_PROJECT.bat
```

**هذا كل شيء!** 🎉

الملف سيقوم بـ:
- ✅ تنظيف أي حاويات قديمة
- ✅ بناء جميع الصور (Images)
- ✅ تشغيل جميع الخدمات
- ✅ فتح لوحة التحكم في المتصفح تلقائياً

## 🌐 الوصول للخدمات

بعد التشغيل، يمكنك الوصول إلى:

| الخدمة | الرابط | الوصف |
|--------|--------|-------|
| **🎨 Dashboard الرئيسي** | http://localhost:3000 | الواجهة الكاملة مع Chatbot |
| **🔌 Backend API** | http://localhost:8000 | FastAPI Backend |
| **📈 Grafana** | http://localhost:3001 | لوحات تحكم متقدمة (admin/admin) |
| **💾 InfluxDB** | http://localhost:8086 | قاعدة البيانات الزمنية |

## 🤖 استخدام Chatbot

الـ Chatbot موجود في الزاوية السفلية اليمنى من Dashboard الرئيسي.

**أمثلة على الأسئلة:**
- "ما حالة النظام؟"
- "كيف الرطوبة؟"
- "هل نحتاج ري؟"
- "ما درجة الحرارة؟"

## 📊 مكونات Dashboard

### الصفحة الرئيسية تحتوي على:
1. **KPI Cards** - مؤشرات الأداء الرئيسية
2. **Real-time Chart** - رسم بياني مباشر لدرجة الحرارة
3. **Map** - خريطة مواقع المزارع
4. **Chatbot** - مساعد ذكي تفاعلي
5. **Grafana Integration** - زر لفتح Grafana

## 🛠️ أوامر مفيدة

### مشاهدة حالة الخدمات:
```cmd
docker-compose ps
```

### مشاهدة السجلات (Logs):
```cmd
# كل الخدمات
docker-compose logs -f

# خدمة معينة
docker-compose logs -f producer
docker-compose logs -f spark-consumer
docker-compose logs -f backend-dashboard
docker-compose logs -f frontend-dashboard
```

### إيقاف المشروع:
```cmd
docker-compose down
```

### إيقاف وحذف كل البيانات:
```cmd
docker-compose down -v
```

### إعادة بناء خدمة معينة:
```cmd
docker-compose up -d --build [service-name]
```

## 🔧 حل المشاكل

### المشكلة: "Docker is not running"
**الحل:** شغل Docker Desktop وانتظر حتى يكون جاهز

### المشكلة: "Port already in use"
**الحل:** 
```cmd
# أوقف الخدمات القديمة
docker-compose down

# أو غير البورت في docker-compose.yml
```

### المشكلة: Dashboard لا يظهر بيانات
**الحل:**
1. تأكد أن Producer يعمل: `docker-compose logs -f producer`
2. تأكد أن Spark Consumer يعمل: `docker-compose logs -f spark-consumer`
3. انتظر 30-60 ثانية لبدء تدفق البيانات

### المشكلة: Chatbot لا يستجيب
**الحل:**
1. تحقق من Backend: http://localhost:8000
2. شاهد logs: `docker-compose logs -f backend-dashboard`

## 📁 هيكل المشروع

```
Final_Project_Data_Hive-main/
├── Producer/                    # مولد بيانات IoT
│   └── IotSystem_Version1.1.py
├── Consumer/                    # معالج Spark
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
├── grafana/                    # إعدادات Grafana
├── docker-compose.yml          # تعريف جميع الخدمات
├── START_PROJECT.bat           # ملف التشغيل الرئيسي ⭐
└── README_AR.md               # هذا الملف

```

## 🎯 تدفق البيانات

```
IoT Producer → Kafka → Spark Consumer → PostgreSQL + InfluxDB
                                              ↓
                                         Grafana
                                              ↓
Backend API ← Kafka (Real-time) → WebSocket → Frontend Dashboard
```

## 📞 الدعم

إذا واجهت أي مشكلة:
1. تحقق من السجلات: `docker-compose logs -f`
2. تأكد من تشغيل Docker Desktop
3. تأكد من توفر المنافذ المطلوبة
4. جرب إعادة التشغيل: `docker-compose down && START_PROJECT.bat`

## 📝 ملاحظات مهمة

- ⚠️ **لا تستخدم** `run_project.bat` أو `LAUNCH_COMPLETE_SYSTEM.bat` - استخدم `START_PROJECT.bat` فقط
- ⏱️ أول تشغيل قد يستغرق 5-10 دقائق لتحميل الصور
- 💾 البيانات تُحفظ في Docker Volumes وتبقى بعد إعادة التشغيل
- 🔄 لحذف كل البيانات والبدء من جديد: `docker-compose down -v`

## ✨ المميزات

- ✅ تشغيل بأمر واحد فقط
- ✅ واجهة عربية كاملة
- ✅ Chatbot ذكي تفاعلي
- ✅ بيانات حقيقية في الوقت الفعلي
- ✅ لوحات تحكم متعددة (Dashboard + Grafana)
- ✅ معالجة بيانات متقدمة مع Spark
- ✅ قواعد بيانات متعددة (PostgreSQL + InfluxDB)

---

**تم التطوير بواسطة:** Data Hive Team  
**التاريخ:** نوفمبر 2025  
**الإصدار:** 2.0
