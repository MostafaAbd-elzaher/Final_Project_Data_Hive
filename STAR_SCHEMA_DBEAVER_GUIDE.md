# Star Schema Diagram - DBeaver Instructions

## كيفية عرض الـ Diagram في DBeaver

### الطريقة 1: Entity-Relationship Diagram (ERD)
1. افتح DBeaver وتأكد أنك متصل بـ PostgreSQL
2. اذهب إلى الـ Database Navigator على اليسار
3. اختر: `farm_dwh` → `Public` → `Tables`
4. اختر أي من الجداول التالية:
   - `fact_sensor_events`
   - `daily_farm_kpis`
   - `farm_dry_sessions`
   - `dim_location`
   - `dim_date`
5. اضغط **كليك يميني** على الجدول → اختر **"Edit Diagram"** أو **"ER Diagram"**
6. ستشوف الجدول مع كل الـ Foreign Keys محددة

### الطريقة 2: View All Tables Diagram
1. في Database Navigator، اختر مجلد **Public**
2. اضغط **كليك يميني** → **"ER Diagram"**
3. هذا سيفتح diagram يعرض **كل الجداول والعلاقات**

### الطريقة 3: SQL Script
1. في DBeaver، افتح **SQL Editor**
2. انسخ كود من `STAR_SCHEMA.sql`
3. اتنفذ الـ Queries لتتحقق من العلاقات

---

## Star Schema Structure

### Fact Table (الجدول المركزي)
**fact_sensor_events** - كل قراءة حساس
- `event_ts` - timestamp الحدث
- `location_id` → dim_location (FK)
- `date_key` → dim_date (FK)
- قيم القراءات: temperature, humidity, light, water level, etc.

### Dimension Tables (جداول الأبعاد)

**dim_location** - معلومات المواقع
- `location_id` (PK)
- `location_name` - اسم الموقع
- `crop_type` - نوع المحصول
- `latitude`, `longitude` - الإحداثيات

**dim_date** - معلومات التواريخ
- `date_key` (PK)
- `date_full` - التاريخ كنص
- `day_of_month`, `month`, `year` - تفاصيل التاريخ
- `season` - الفصل
- `day_period_name` - فترة اليوم

### Aggregate Tables (جداول مشتقة)

**daily_farm_kpis** - معدّلات يومية
- `location_id` → dim_location (FK)
- `window_end` - نهاية الفترة اليومية
- `avg_env_health_score_1d` - متوسط درجة الصحة
- `pct_time_dry` - نسبة فترات الجفاف
- `anomaly_count_day` - عدد الشذوذ
- `error_count_day` - عدد الأخطاء

**farm_dry_sessions** - جلسات الجفاف
- `location_id` → dim_location (FK)
- `session_start`, `session_end` - فترة الجلسة
- `avg_humidity_during_dry_session` - متوسط الرطوبة
- `event_count_in_session` - عدد الأحداث

---

## الفوائد من هذه البنية

✅ **Normalization** - تجنب تكرار البيانات
✅ **Performance** - Indexes على Foreign Keys
✅ **Referential Integrity** - ارتباطات صحيحة بين الجداول
✅ **Easy Querying** - Joins سهلة بين الجداول
✅ **Data Warehouse Pattern** - Star Schema (النمط الأمثل للتحليلات)

---

## SQL Queries للتحقق من الآلاقات

```sql
-- عرض جميع الآلاقات (Foreign Keys)
SELECT * FROM information_schema.table_constraints 
WHERE constraint_type = 'FOREIGN KEY' 
AND table_schema = 'public';

-- عينة من البيانات مع Join
SELECT 
  fse.event_ts,
  dl.location_name,
  fse.soil_temperature_c,
  fse.air_humidity_percent
FROM fact_sensor_events fse
JOIN dim_location dl ON fse.location_id = dl.location_id
LIMIT 20;

-- تحقق من عدد الصفوف في كل جدول
SELECT 
  'fact_sensor_events' as table_name,
  COUNT(*) as row_count
FROM fact_sensor_events
UNION ALL
SELECT 'daily_farm_kpis', COUNT(*) FROM daily_farm_kpis
UNION ALL
SELECT 'farm_dry_sessions', COUNT(*) FROM farm_dry_sessions
UNION ALL
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;
```

---

## Notes
- جميع الـ Foreign Keys تم تأسيسها بنجاح ✅
- جميع الـ Indexes موجودة للأداء الأفضل ✅
- البيانات مطابقة بين الجداول ✅

الآن تقدر تفتح DBeaver وتشوف الـ Diagram! 🎉
