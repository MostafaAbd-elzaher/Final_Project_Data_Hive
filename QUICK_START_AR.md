# 🌱 Farm IoT - تشغيل وإيقاف سريع (عربي)

## التشغيل

```bash
cd /mnt/E/MyCareer/DepiData/DataHive/FinalProject
./start_system.sh
```

**ينتظر 1-2 دقيقة حتى تبدأ جميع الخدمات** ✅

---

## الإيقاف

```bash
cd /mnt/E/MyCareer/DepiData/DataHive/FinalProject
./stop_system.sh
```

**البيانات محفوظة - لن تفقد شيئًا** 💾

---

## الخدمات المتاحة بعد التشغيل

| الخدمة | الرابط | البيانات |
|--------|--------|---------|
| **Grafana** 📊 | http://localhost:3001 | admin / admin |
| **Backend API** 🔌 | http://localhost:8000 | - |
| **Frontend** 🌐 | http://localhost:3000 | - |
| **Database** 🗄️ | localhost:5432 | spark_user / spark_password |

---

## مراقبة الخدمات

### شاشة الحالة:
```bash
docker compose ps
```

### المتابعة المباشرة (logs):
```bash
# جميع الخدمات:
docker compose logs -f

# خدمة واحدة فقط:
docker compose logs -f producer
docker compose logs -f spark-consumer
docker compose logs -f backend-dashboard

# توقف المتابعة: Ctrl + C
```

---

## الوصول للقاعدة في DBeaver

```
Host: localhost
Port: 5432
Database: farm_dwh
User: spark_user
Password: spark_password
```

---

## أوامر مفيدة

```bash
# إعادة تشغيل خدمة:
docker compose restart [service-name]

# حذف كل شيء (احذر!):
docker compose down -v

# إعادة بناء الصور:
docker compose build --no-cache && ./start_system.sh
```

---

## استكشاف الأخطاء

**المنافذ مشغولة؟**
```bash
lsof -i :3001  # Grafana
lsof -i :5432  # PostgreSQL
```

**خدمات لا تبدأ؟**
```bash
docker compose logs
./start_system.sh  # حاول من جديد
```

**فقدت البيانات؟**
```bash
docker compose restart iot-producer
```

---

## 📋 ملخص سريع

| الفعل | الأمر |
|------|-------|
| تشغيل | `./start_system.sh` |
| إيقاف | `./stop_system.sh` |
| الحالة | `docker compose ps` |
| السجلات | `docker compose logs -f` |
| إعادة | `docker compose restart [name]` |

---

**تم! 🚀 النظام جاهز للعمل**
