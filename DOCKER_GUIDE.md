# تشغيل مشروع Data Hive IoT Pipeline على Docker

## ✅ التحضيرات المكتملة

تم إنجاز جميع الملفات المطلوبة لتشغيل المشروع بالكامل على Docker:

### الملفات المُنشأة:
1. ✅ **Dockerfile.producer** - صورة Docker للـ Producer (محاكي IoT)
2. ✅ **Dockerfile.consumer** - صورة Docker للـ Spark Consumer
3. ✅ **docker-compose.yml** - تكوين كامل لجميع الخدمات
4. ✅ **docker-entrypoint-producer.sh** - سكريبت بدء تشغيل Producer
5. ✅ **docker-entrypoint-consumer.sh** - سكريبت بدء تشغيل Spark Consumer
6. ✅ **.env** - متغيرات البيئة
7. ✅ تحديث أكواد Producer و Consumer للعمل مع Docker

### الخدمات المُكونة:
- ✅ Zookeeper
- ✅ Kafka (مع health checks)
- ✅ PostgreSQL (مع health checks)
- ✅ InfluxDB (مع health checks)
- ✅ Grafana (مع health checks)  
- ✅ Producer (IoT Simulator)
- ✅ Spark Consumer (Data Pipeline)

---

## 📋 متطلبات التشغيل

### 1. تثبيت Docker Desktop

**يجب تثبيت Docker Desktop على Windows:**

1. قم بتحميل Docker Desktop من: https://www.docker.com/products/docker-desktop/
2. قم بتثبيت البرنامج
3. قم بتشغيل Docker Desktop
4. تأكد من أن Docker يعمل بفتح PowerShell وتشغيل:
   ```powershell
   docker --version
   docker compose version
   ```

### 2. المتطلبات الأخرى
- Windows 10/11 (64-bit)
- WSL 2 (سيتم تثبيته تلقائياً مع Docker Desktop)
- 8GB RAM على الأقل (16GB موصى به)
- 20GB مساحة فارغة على القرص

---

## 🚀 تشغيل المشروع

### الخطوة 1: التأكد من Docker Desktop
```powershell
# تحقق من أن Docker يعمل
docker ps
```

### الخطوة 2: الانتقال لمجلد المشروع
```powershell
cd "d:/DEPI/FINALLLLLLL/Final_Project_Data_Hive-main/Final_Project_Data_Hive-main"
```

### الخطوة 3: بناء جميع الصور
```powershell
docker compose build
```
**هذا سيأخذ عدة دقائق في المرة الأولى**

### الخطوة 4: تشغيل جميع الخدمات
```powershell
docker compose up
```

**أو للتشغيل في الخلفية:**
```powershell
docker compose up -d
```

---

## 📊 الوصول للخدمات

بعد تشغيل الخدمات، يمكنك الوصول إلى:

| الخدمة | الرابط | اسم المستخدم | كلمة المرور |
|--------|--------|--------------|-------------|
| **Grafana** | http://localhost:3000 | admin | admin |
| **InfluxDB** | http://localhost:8086 | my_user | my_password |
| **PostgreSQL** | localhost:5432 | spark_user | spark_password |
| **Kafka** | localhost:9092 | - | - |

---

## 🔍 مراقبة الخدمات

### عرض حالة الخدمات
```powershell
docker compose ps
```

### عرض logs لخدمة معينة
```powershell
# Producer logs
docker compose logs -f producer

# Spark Consumer logs
docker compose logs -f spark-consumer

# Kafka logs
docker compose logs -f kafka

# PostgreSQL logs
docker compose logs -f postgres
```

### عرض logs لجميع الخدمات
```powershell
docker compose logs -f
```

---

## ✅ التحقق من سير العمل

### 1. التحقق من Producer
```powershell
docker compose logs producer
```
**يجب أن ترى:**
- ✅ تم الاتصال بـ Kafka بنجاح
- 📬 تم إرسال البيانات إلى Kafka Topic

### 2. التحقق من Spark Consumer
```powershell
docker compose logs spark-consumer
```
**يجب أن ترى:**
- ✅ Spark session started successfully
- Writing to Kafka topics
- Writing to DWH tables

### 3. التحقق من البيانات في PostgreSQL
```powershell
docker compose exec postgres psql -U spark_user -d farm_dwh -c "\dt"
```
**يجب أن ترى الجداول:**
- dim_location
- dim_date
- fact_sensor_events
- daily_farm_kpis
- farm_dry_sessions

### 4. عرض البيانات
```powershell
docker compose exec postgres psql -U spark_user -d farm_dwh -c "SELECT COUNT(*) FROM fact_sensor_events;"
```

---

## 🛑 إيقاف الخدمات

### إيقاف مؤقت (الحفاظ على البيانات)
```powershell
docker compose stop
```

### إيقاف وحذف Containers (الحفاظ على البيانات)
```powershell
docker compose down
```

### إيقاف وحذف كل شيء (بما في ذلك البيانات)
```powershell
docker compose down -v
```

---

## 🔧 استكشاف الأخطاء

### مشكلة: الخدمات لا تبدأ
```powershell
# تحقق من حالة Docker
docker info

# تحقق من المنافذ المستخدمة
netstat -ano | findstr "9092"
netstat -ano | findstr "5432"
netstat -ano | findstr "3000"
```

### مشكلة: نفاد الذاكرة
- افتح Docker Desktop
- اذهب إلى Settings → Resources
- زد الـ Memory allocation إلى 8GB على الأقل

### مشكلة: Producer لا يتصل بـ Kafka
```powershell
# تحقق من logs
docker compose logs kafka
docker compose logs producer

# أعد تشغيل الخدمات
docker compose restart
```

### إعادة بناء صورة معينة
```powershell
docker compose build --no-cache producer
docker compose build --no-cache spark-consumer
```

---

## 📝 ملاحظات مهمة

1. **المرة الأولى:** بناء الصور سيأخذ 10-15 دقيقة
2. **الانتظار:** بعد `docker compose up`، انتظر 1-2 دقيقة حتى تستقر جميع الخدمات
3. **الترتيب:** الخدمات ستبدأ بالترتيب الصحيح تلقائياً بفضل `depends_on` و `healthcheck`
4. **البيانات:** جميع البيانات محفوظة في Docker volumes (postgres_data, influxdb_data, etc.)
5. **Logs:** استخدم `docker compose logs -f` لمتابعة ما يحدث

---

## 🎯 الخطوات التالية

بعد تشغيل المشروع بنجاح:

1. افتح Grafana على http://localhost:3000
2. أضف PostgreSQL data source:
   - Host: `postgres:5432`
   - Database: `farm_dwh`
   - User: `spark_user`
   - Password: `spark_password`
3. أضف InfluxDB data source:
   - URL: `http://influxdb:8086`
   - Token: `my_super_secret_token`
   - Organization: `my_org`
   - Bucket: `iot_bucket`
4. قم بإنشاء Dashboard لعرض البيانات الحية!

---

## 📞 الدعم

في حالة وجود أي مشاكل:
1. تحقق من Docker Desktop أنه يعمل
2. راجع الـ logs: `docker compose logs -f`
3. أعد تشغيل الخدمات: `docker compose restart`
4. كحل أخير، أعد البناء: `docker compose down -v && docker compose build --no-cache && docker compose up`
