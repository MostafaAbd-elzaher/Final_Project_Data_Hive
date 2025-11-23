import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg as _avg, stddev as _stddev,
    count as _count, lit, when, expr, to_json, struct, row_number, abs, session_window,
    date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType, IntegerType
)
from pyspark.sql.window import Window as SparkWindow
import pyspark.sql.functions as F
from pyspark.sql.pandas.functions import pandas_udf
# ===========================
# CONFIG - تعديل حسب الحاجة
# ===========================
KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "farmSensors"
OUTPUT_EVENTS_TOPIC = "farmInsights"   # enriched per-event
OUTPUT_TRENDS_TOPIC = "farmTrends"     # 5min trend insights
OUTPUT_KPIS_TOPIC = "farmKpis"         # daily/week KPIs (periodic)
HOME_PATH = "/home/mostafa"
PARQUET_BASE_PATH = f"{HOME_PATH}/spark_project_data/farm_iot_parquet"
CHECKPOINT_BASE = f"{HOME_PATH}/spark_project_data/checkpoints/farm_iot_full_pipeline"
PROCESSING_TRIGGER = "30 seconds"

DB_URL = "jdbc:postgresql://localhost:5432/farm_dwh"
DB_PROPERTIES = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}
KPI_TABLE_NAME = "daily_farm_kpis"
DIM_LOCATION_TABLE_NAME = "dim_location"
FACT_TABLE_NAME = "fact_sensor_events"
SESSIONS_TABLE_NAME = "farm_dry_sessions"
DIM_DATE_TABLE_NAME = "dim_date" # إضافة: اسم جدول بُعد التاريخ
# ===========================
# SCHEMA
# ===========================
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("date", StringType()),
    StructField("time", StringType()),
    StructField("season", StringType()),
    StructField("day_period", StringType()),
    StructField("daytime", BooleanType()),
    StructField("soil_temperature_c", FloatType()),
    StructField("air_temperature_c", FloatType()),
    StructField("soil_humidity_percent", FloatType()),
    StructField("air_humidity_percent", FloatType()),
    StructField("soil_ph", FloatType()),
    StructField("soil_salinity_ds_m", FloatType()),
    StructField("light_intensity_lux", FloatType()),
    StructField("water_level_percent", FloatType()),
    StructField("location", StringType()),
    StructField("is_error", BooleanType())
])

# ===========================
# Spark session
# ===========================

spark = (
    SparkSession.builder
    .appName("FarmIoTFullPipeline_v2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session started successfully.")

# ===========================
# 1) Dimension Tables Setup (One-Time)
# ===========================

# ----------------------------------------------------
# دالة إعداد جدول KPI في Postgres
# ----------------------------------------------------
def setup_kpi_table_schema(spark_session):
    """
    يضمن أن جدول KPI مُنشأ بالـ Schema الصحيح (LowerCase) قبل بدء التدفق.
    """
    print(f"--- ⚙️ Setting up KPI table schema for '{KPI_TABLE_NAME}'... ---")
    
    # تعريف الـ Schema يدوياً لضمان تطابق الأسماء والأنواع
    kpi_temp_schema = StructType([
        StructField("window_start", StringType()),
        StructField("window_end", StringType()),
        StructField("location", StringType()),
        StructField("avg_env_health_score_1d", FloatType()),
        StructField("pct_time_dry", FloatType()),
        StructField("anomaly_count_day", IntegerType()),
        StructField("error_count_day", IntegerType()),
        StructField("records_day", IntegerType()),
        StructField("farm_health_grade", StringType()),
    ])

    try:
        # إنشاء DataFrame فارغ بالـ Schema الصحيح
        empty_kpi_df = spark_session.createDataFrame([], kpi_temp_schema)

        # الكتابة بـ mode="overwrite" لضمان إعادة إنشاء الجدول بالـ Schema الجديد (LowerCase)
        (empty_kpi_df
         .write
         .jdbc(url=DB_URL,
               table=KPI_TABLE_NAME,
               mode="overwrite", # نستخدم overwrite هنا لضمان مسح وإعادة إنشاء الجدول
               properties=DB_PROPERTIES)
        )
        print(f"--- ✅ Schema for '{KPI_TABLE_NAME}' successfully created/overwritten in DWH. ---")
    except Exception as e:
        print(f"--- 🔥 Critical Error in KPI Schema Setup: {e} ---")
        raise # يجب إيقاف التشغيل إذا فشل هذا الجزء
# ----------------------------------------------------


# 1a) dim_location Setup 
location_dim_data = [
    (1, "القاهرة، مصر", "Tomatoes", 30.0444, 31.2357),
    (2, "الإسكندرية، مصر", "Cucumbers", 31.2001, 29.9187)
]
location_dim_schema = StructType([
    StructField("location_id", IntegerType(), False),
    StructField("location_name", StringType(), True),
    StructField("crop_type", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
])

print(f"--- 🔄 Writing Dimension Table '{DIM_LOCATION_TABLE_NAME}' to DWH... ---")
try:
    location_dim_df = spark.createDataFrame(
        data=location_dim_data,
        schema=location_dim_schema
    )
    (location_dim_df
     .write
     .jdbc(url=DB_URL,
           table=DIM_LOCATION_TABLE_NAME,
           mode="overwrite",
           properties=DB_PROPERTIES)
    )
    print(f"--- ✅ Successfully created/overwritten '{DIM_LOCATION_TABLE_NAME}' in DWH. ---")
except Exception as e:
    print(f"--- 🔥 Error writing Dimension Table: {e} ---")
    pass


# 1b) dim_date Setup 
print(f"--- 🔄 Ensuring structure for Dimension Table '{DIM_DATE_TABLE_NAME}' in DWH... ---")
try:
    date_dim_schema = StructType([
        StructField("date_key", IntegerType(), False), # PK: YYYYMMDD
        StructField("date_full", StringType(), True),
        StructField("day_of_month", IntegerType(), True),
        StructField("day_of_week_name", StringType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("season", StringType(), True),
        StructField("day_period_name", StringType(), True)
    ])
    date_dim_df_empty = spark.createDataFrame([], date_dim_schema)
    
    (date_dim_df_empty.limit(0)
     .write
     .jdbc(url=DB_URL,
           table=DIM_DATE_TABLE_NAME,
           mode="append", 
           properties=DB_PROPERTIES)
    )
    print(f"--- ✅ Structure for '{DIM_DATE_TABLE_NAME}' confirmed/created in DWH. ---")
except Exception as e:
    print(f"--- ⚠️ Could not ensure {DIM_DATE_TABLE_NAME} structure: {e} ---")
    pass


# 1c) KPI Table Setup (الخطوة المضافة لحل مشكلة Schema Mismatch)
setup_kpi_table_schema(spark)


# ===========================
# 2) Read from Kafka and Enrichment 
# ===========================
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
       .select("data.*")
       .withColumn("event_ts", to_timestamp(col("timestamp")))
)
parsed = parsed.withWatermark("event_ts", "1 hour")

# Stream-Static Join (Location)
dim_location_dwh = (
    spark.read
    .jdbc(url=DB_URL,
          table=DIM_LOCATION_TABLE_NAME, 
          properties=DB_PROPERTIES)
)

parsed_enriched_static = parsed.join(
    dim_location_dwh,
    parsed.location == dim_location_dwh.location_name,
    how="left"
).drop("location_name")

# Advanced Rule-based Cleaning
events_cleaned = (
    parsed_enriched_static
    .withColumn("soil_temp_cleaned",
    when((col("soil_temperature_c") > 60) | (col("soil_temperature_c") < -10), lit(None))
    .otherwise(col("soil_temperature_c"))
    )
    .withColumn("soil_ph_cleaned",
        when((col("soil_ph") > 14) | (col("soil_ph") < 0), lit(None))
        .otherwise(col("soil_ph"))
    )
    .drop("soil_temperature_c", "soil_ph")
    .withColumnRenamed("soil_temp_cleaned", "soil_temperature_c")
    .withColumnRenamed("soil_ph_cleaned", "soil_ph")
)

# Per-event enrichment: flags, scores, derived cols
events_enriched = (
    events_cleaned
    # differences
    .withColumn("temp_diff_air_soil", col("air_temperature_c") - col("soil_temperature_c"))
    .withColumn("humidity_diff_air_soil", col("air_humidity_percent") - col("soil_humidity_percent"))

    # pH classification
    .withColumn("ph_status",
        when(col("soil_ph").isNull(), lit("Unknown"))
        .when(col("soil_ph") < 6.0, lit("Acidic"))
        .when(col("soil_ph") > 8.0, lit("Alkaline"))
        .otherwise(lit("Normal"))
    )
    # salinity classification
    .withColumn("salinity_status",
        when(col("soil_salinity_ds_m") < 2.0, lit("Low"))
        .when((col("soil_salinity_ds_m") >= 2.0) & (col("soil_salinity_ds_m") < 4.0), lit("Moderate"))
        .otherwise(lit("High"))
    )
    # anomalies rules (rule-based)
    .withColumn("is_anomaly_temp",
        when((col("soil_temperature_c") > 40) | (col("air_temperature_c") > 40) | (col("soil_temperature_c") < -20), lit(1)).otherwise(lit(0))
    )
    .withColumn("is_anomaly_humidity",
        when((col("soil_humidity_percent") < 30) | (col("soil_humidity_percent") > 100), lit(1)).otherwise(lit(0))
    )
    .withColumn("is_sensor_error", when(col("is_error") == True, lit(1)).otherwise(lit(0)))
    # immediate environmental indicators
    .withColumn("needs_watering", when(col("soil_humidity_percent") < 30, lit(1)).otherwise(lit(0)))
    .withColumn("possible_overheating", when((col("soil_temperature_c") > 40) | (col("air_temperature_c") > 40), lit(1)).otherwise(lit(0)))
    .withColumn("ph_not_optimal", when((col("soil_ph") < 6) | (col("soil_ph") > 8), lit(1)).otherwise(lit(0)))
    # environmental health score (0-100)
    .withColumn("env_health_score",
        (lit(100)
         - (expr("abs(soil_ph - 7.0) * 12"))
         - (col("soil_salinity_ds_m") * 6)
         - when(col("soil_humidity_percent") < 30, lit(12)).otherwise(lit(0)) )
    )
    .withColumn("crop_type", col("crop_type"))
    .withColumn("latitude", col("latitude"))
    .withColumn("longitude", col("longitude"))
)

# ML Model Application (native Spark)
events_enriched_ml = events_enriched.withColumn(
    "ml_anomaly_score",
    F.when(
        (F.col("soil_temperature_c").isNotNull()) & 
        (F.col("soil_humidity_percent").isNotNull()) &
        (F.col("soil_temperature_c") > 35) & 
        (F.col("soil_humidity_percent") < 40),
        F.lit(1)
    ).otherwise(F.lit(0))
)

# ===========================
# 3) Fact Table Preparation for Star Schema
# ===========================
# اشتقاق المفتاح الزمني (Date Key) لجدول الحقائق
events_for_fact = (
    events_enriched_ml
    .withColumn("date_key", date_format(col("event_ts"), "yyyyMMdd").cast(IntegerType()))
)
print("✅ Fact Table preparation: 'date_key' created.")

# 4) تم إلغاء منطق Z-Score Stream-Stream Join لتجنب الأخطاء

print("✅ Z-Score (Outlier) detection logic moved to foreachBatch function.")


# ===========================
# SINK FUNCTIONS (foreachBatch) 
# ===========================

# ----------------------------------------------------
# الدالة لـ Star Schema (بُعد التاريخ) - تمت إضافة التصفية
# ----------------------------------------------------
def update_dim_date_batch(batch_df, batch_id):
    """
    يستخرج التواريخ الفريدة في الدفعة ويُضيفها إلى جدول dim_date في الـ DWH.
    """
    print(f"--- 📅 Processing dim_date update Batch {batch_id} ---")
    try:
        # ⚠️ الخطوة الأولى والجوهرية: تصفية السجلات التي يكون فيها المفتاح (date_key) صالحاً
        filtered_dates_df = batch_df.filter(col("date_key").isNotNull()) 
        
        # 1. استخراج التواريخ الفريدة المطلوبة لـ dim_date
        unique_dates = (
            filtered_dates_df # استخدام الـ DataFrame المصفى
            .select(
                col("date_key"), 
                date_format(col("event_ts"), "yyyy-MM-dd").alias("date_full"),
                F.dayofmonth("event_ts").alias("day_of_month"),
                F.dayofweek("event_ts").alias("day_of_week_name"),
                F.month("event_ts").alias("month"),
                F.year("event_ts").alias("year"),
                col("season"),
                col("day_period")
            )
            .distinct()
        )

        # 2. تحديد اسم يوم الأسبوع (للتوافق مع الـ Schema)
        unique_dates = unique_dates.withColumn(
            "day_of_week_name", 
            when(col("day_of_week_name") == 1, lit("Sunday"))
            .when(col("day_of_week_name") == 2, lit("Monday"))
            .when(col("day_of_week_name") == 3, lit("Tuesday"))
            .when(col("day_of_week_name") == 4, lit("Wednesday"))
            .when(col("day_of_week_name") == 5, lit("Thursday"))
            .when(col("day_of_week_name") == 6, lit("Friday"))
            .when(col("day_of_week_name") == 7, lit("Saturday"))
        )
        # 3. إعادة تسمية الأعمدة لضمان الأحرف الصغيرة في Postgres
        date_dim_final = unique_dates.select(
            col("date_key").alias("date_key"),
            col("date_full").alias("date_full"),
            col("day_of_month").alias("day_of_month"),
            col("day_of_week_name").alias("day_of_week_name"),
            col("month").alias("month"),
            col("year").alias("year"),
            col("season").alias("season"),
            col("day_period").alias("day_period_name")
        )
        
        # إضافة خاصية JDBC لضمان الـ Commit
        jdbc_properties_commit = DB_PROPERTIES.copy()
        jdbc_properties_commit["v2_mode"] = "true" 
        jdbc_properties_commit["defaultRowCommitMode"] = "External" 
        
        if filtered_dates_df.count() > 0: # التأكد من وجود بيانات صالحة للإدخال بعد التصفية
            (date_dim_final
             .write
             .jdbc(url=DB_URL,
                   table=DIM_DATE_TABLE_NAME,
                   mode="append", 
                   properties=jdbc_properties_commit)
            )
            print(f"--- ✅ dim_date update Batch {batch_id} successfully written to DWH. ---")
        else:
             print(f"--- ⚠️ dim_date Batch {batch_id}: No valid dates found after filtering. ---")

    except Exception as e:
        print(f"--- 🔥 Error writing dim_date Batch {batch_id} to SQL: {e} ---")
# ----------------------------------------------------


def write_trends_to_kafka(batch_df, batch_id):
    """
    Calculates 5-min deltas (trends) using lag() within the micro-batch
    and writes the results to Kafka. (Consumes the 5-min SLIDING window)
    """
    print(f"--- 📈 Processing Trends Batch {batch_id} ---")
    
    window_spec_lag = (
        SparkWindow
        .partitionBy("location")
        .orderBy("window_start")
    )

    batch_with_lag = (
        batch_df
        .withColumn("prev_avg_soil_temp_5m", 
                    F.lag("avg_soil_temp_5m").over(window_spec_lag))
        .withColumn("prev_avg_soil_humidity_5m", 
                    F.lag("avg_soil_humidity_5m").over(window_spec_lag))
    )
    batch_with_delta = (
        batch_with_lag
        .withColumn("delta_temp_5m",
                    (col("avg_soil_temp_5m") - col("prev_avg_soil_temp_5m")))
        .withColumn("delta_humidity_5m",
                    (col("avg_soil_humidity_5m") - col("prev_avg_soil_humidity_5m")))
        .fillna(0.0, subset=["delta_temp_5m", "delta_humidity_5m"])
    )

    final_trends_batch = batch_with_delta.select(
        to_json(struct(
            col("window_start"),
            col("window_end"),
            col("location"),
            col("avg_soil_temp_5m"),
            col("delta_temp_5m"),
            col("avg_soil_humidity_5m"),
            col("delta_humidity_5m"),
            col("anomaly_temp_count_5m"),
            col("error_count_5m")
        )).alias("value")
    )

    try:
        (final_trends_batch
         .write
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", OUTPUT_TRENDS_TOPIC)
         .save())
        print(f"--- ✅ Trends Batch {batch_id} successfully written to Kafka ---")
    except Exception as e:
        print(f"--- 🔥 Error writing Trends Batch {batch_id} to Kafka: {e} ---")
# === نهاية الدالة ===


def write_kpis_to_sql_batch(batch_df, batch_id):
    """
    Function to process each micro-batch of aggregated KPIs (Gold Layer)
    and write them to the SQL Data Warehouse. (Consumes 1-day TUMBLING window)
    
    NOTE: batch_df received here is ALREADY fully prepared (window expanded and columns lowercased).
    """
    print(f"--- 🚀 Writing KPI Batch {batch_id} to SQL DWH ({KPI_TABLE_NAME}) ---")
    
    try:
        # دمج الخصائص الجديدة لضمان الـ Commit
        jdbc_properties_commit = DB_PROPERTIES.copy()
        jdbc_properties_commit["v2_mode"] = "true" 
        jdbc_properties_commit["defaultRowCommitMode"] = "External" 
        
        # NOTE: We use batch_df directly as it is prepared outside foreachBatch
        (batch_df
         .write
         .jdbc(url=DB_URL,
               table=KPI_TABLE_NAME,
               mode="append",
               properties=jdbc_properties_commit)
        )
        print(f"--- ✅ Batch {batch_id} successfully written to {KPI_TABLE_NAME} ---")
    except Exception as e:
        # ⚠️ هنا ستبقى المشكلة حتى يتم التأكد من تعريف جدول Postgres
        print(f"--- 🔥 Error writing Batch {batch_id} to SQL: {e} ---")
# === نهاية الدالة ===


# ----------------------------------------------------
# الدالة المحدثة لـ Fact Table (تتضمن حساب Z-Score)
# ----------------------------------------------------
def write_events_to_sql_batch(batch_df, batch_id):
    """
    Writes the fully enriched event-level data to the Fact Table in the DWH.
    *** يتضمن الآن حساب Z-Score ضمن الدفعة (Batch) لتجنب Stream-Stream Join ***
    """
    print(f"--- 🚀 Writing Fact Table Batch {batch_id} to SQL DWH ({FACT_TABLE_NAME}) ---")

    try:
        # 1. حساب الإحصائيات (AVG و STDDEV) للدفعة الحالية (كبديل لـ Tumbling Window)
        batch_stats = (
            batch_df
            .groupBy("location")
            .agg(
                _avg("soil_temperature_c").alias("avg_soil_temp_batch"),
                _stddev("soil_temperature_c").alias("std_soil_temp_batch"),
                _avg("soil_humidity_percent").alias("avg_soil_humidity_batch"),
                _stddev("soil_humidity_percent").alias("std_soil_humidity_batch"),
            )
        )
        
        # 2. ربط الأحداث بالإحصائيات المحسوبة داخل الدفعة (Batch-Batch Join)
        events_with_stats = batch_df.join(
            batch_stats,
            on="location",
            how="left"
        ).cache()

        # 3. حساب Z-score (Outlier Flags)
        k_temp = 3.0
        k_hum = 3.0
        events_outliers = events_with_stats.withColumn(
            "z_temp",
            when(col("std_soil_temp_batch").isNull() | (col("std_soil_temp_batch") == 0), lit(0.0))
            .otherwise( (col("soil_temperature_c") - col("avg_soil_temp_batch")) / col("std_soil_temp_batch") )
        ).withColumn(
            "z_hum",
            when(col("std_soil_humidity_batch").isNull() | (col("std_soil_humidity_batch") == 0), lit(0.0))
            .otherwise( (col("soil_humidity_percent") - col("avg_soil_humidity_batch")) / col("std_soil_humidity_batch") )
        ).withColumn(
            "is_outlier_temp_z", when(abs(col("z_temp")) > k_temp, lit(1)).otherwise(lit(0))
        ).withColumn(
            "is_outlier_hum_z", when(abs(col("z_hum")) > k_hum, lit(1)).otherwise(lit(0))
        )
        
        # 4. اختيار المفاتيح (Keys) والمقاييس (Measures) فقط والكتابة
        batch_df_filled = events_outliers.fillna(0, subset=["is_outlier_temp_z", "is_outlier_hum_z", "ml_anomaly_score"])

        fact_data = batch_df_filled.select(
            # المفاتيح الخارجية (Foreign Keys)
            "event_ts",           
            "location_id",        
            "date_key",           

            # المقاييس (Measures)
            "soil_temperature_c",
            "air_temperature_c",
            "soil_humidity_percent",
            "air_humidity_percent",
            "soil_ph",
            "soil_salinity_ds_m",
            "light_intensity_lux",
            "water_level_percent",
            "env_health_score",
            
            # مقاييس/أعلام (Metrics/Flags) - يتم حسابها في هذه الدفعة
            "is_outlier_temp_z", 
            "is_outlier_hum_z",  
            "ml_anomaly_score",
            "is_sensor_error"
        )
        
        jdbc_properties_commit = DB_PROPERTIES.copy()
        jdbc_properties_commit["v2_mode"] = "true" 
        jdbc_properties_commit["defaultRowCommitCommitMode"] = "External" 
        
        (fact_data
         .write
         .jdbc(url=DB_URL,
               table=FACT_TABLE_NAME,
               mode="append",
               properties=jdbc_properties_commit)
        )
        
        events_with_stats.unpersist() 
        print(f"--- ✅ Fact Batch {batch_id} successfully written to {FACT_TABLE_NAME} ---")
    except Exception as e:
        print(f"--- 🔥 Error writing Fact Batch {batch_id} to SQL: {e} ---")
# === نهاية الدالة ===
# ----------------------------------------------------


def write_sessions_to_sql_batch(batch_df, batch_id):
    """
    Writes the aggregated session data (e.g., dry spells)
    to the DWH. (Consumes the 'dry_sessions' stream)
    """
    print(f"--- 📊 Writing Sessions Batch {batch_id} to SQL DWH ({SESSIONS_TABLE_NAME}) ---")
    
    try:
        processed_batch_df = (
            batch_df
            .withColumn("session_start", col("session_window").start)
            .withColumn("session_end", col("session_window").end)
            .drop("session_window")
            .select(
                "session_start",
                "session_end",
                "location",
                "avg_humidity_during_dry_session",
                "event_count_in_session",
                "session_duration_minutes"
            )
        )
        jdbc_properties_commit = DB_PROPERTIES.copy()
        jdbc_properties_commit["v2_mode"] = "true" 
        jdbc_properties_commit["defaultRowCommitMode"] = "External"
        
        (processed_batch_df
         .write
         .jdbc(url=DB_URL,
               table=SESSIONS_TABLE_NAME,
               mode="append",
               properties=jdbc_properties_commit)
        )
        print(f"--- ✅ Sessions Batch {batch_id} successfully written to {SESSIONS_TABLE_NAME} ---")
    except Exception as e:
        print(f"--- 🔥 Error writing Sessions Batch {batch_id} to SQL: {e} ---")
# === نهاية الدالة ===


# ===========================
# 8) Output Sinks (Start all streams)
# ===========================

# --- 8a) Event-Level Sinks (Kafka, Delta Lake, DWH Fact Table) ---
# المصدر: events_for_fact 

# Sink 1: Enriched events to Kafka (بدون Z-Score)
events_to_kafka = events_for_fact.select( 
    to_json(struct(*[c for c in events_for_fact.columns])).alias("value")
)

events_kafka_q = (
    events_to_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_EVENTS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_kafka")
    .outputMode("append")
    .start()
)

# Sink 2: Enriched events to Delta Lake (Silver Layer)
events_lake_q = (
    events_for_fact 
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("path", f"{PARQUET_BASE_PATH}/delta_lake/all_events")
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_delta_lake")
    .start()
)

# Sink 3a: Update dim_date 
date_dim_update_q = (
    events_for_fact # استخدمنا events_for_fact كمصدر
    .writeStream
    .outputMode("append")
    .foreachBatch(update_dim_date_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/dim_date_update_dwh_v2")
    .start()
)
print("✅ Dimension Date Update Sink started.")


# Sink 3b: Enriched events to SQL DWH (Fact Table) - يتضمن Z-Score داخلياً
events_sql_dwh_q = (
    events_for_fact # استخدمنا events_for_fact كمصدر
    .writeStream
    .outputMode("append")
    .foreachBatch(write_events_to_sql_batch) # تحتوي على منطق Z-Score الآن
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_fact_table_sql_dwh_v2")
    .start()
)
print("✅ Fact Table Sink (Star Schema) started.")


# --- 8b) Aggregation-Level Sinks ---

# Sink 4: Trend insights (5m SLIDING) to Kafka
# (يستخدم events_for_fact)
agg_5m_sliding = (
    events_for_fact 
    .groupBy(window(col("event_ts"), "5 minutes", "1 minute"), col("location")) 
    .agg(
        _avg("soil_temperature_c").alias("avg_soil_temp_5m"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_5m"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_temp_count_5m"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_5m")
    )
)
agg5_time_sliding = agg_5m_sliding.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("location"),
    "avg_soil_temp_5m", "avg_soil_humidity_5m", "anomaly_temp_count_5m", "error_count_5m"
)
trends_delta_q = (
    agg5_time_sliding 
    .writeStream
    .outputMode("update")
    .foreachBatch(write_trends_to_kafka)
    .option("checkpointLocation", CHECKPOINT_BASE + "/trends_delta_kafka")
    .start()
)

# KPIs (1 Day Tumbling)
kpi_daily = (
    events_for_fact
    .groupBy(window(col("event_ts"), "1 day"), col("location"))
    .agg(
        # تم تثبيت النوع لـ Float
        _avg("env_health_score").cast(FloatType()).alias("avg_env_health_score_1d"),
        (_count(when(col("needs_watering") == 1, True)) / _count("*")).cast(FloatType()).alias("pct_time_dry"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_count_day"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_day"),
        _count("*").alias("records_day")
    )
)
kpi_daily_grade = kpi_daily.withColumn(
    "farm_health_grade",
    when(col("avg_env_health_score_1d") >= 80, lit("A"))
    .when(col("avg_env_health_score_1d") >= 60, lit("B"))
    .when(col("avg_env_health_score_1d") >= 40, lit("C"))
    .otherwise(lit("D"))
)

# 1. Expand the 'window' struct column
kpi_daily_expanded = (
    kpi_daily_grade
    .withColumn("window_start", col("window").start)
    .withColumn("window_end", col("window").end)
    .drop("window")
)

# 2. Rename all columns to lowercase for strict PostgreSQL compatibility
# يجب أن نضمن هنا أن الأعمدة الناتجة هي بالضبط ما يتوقعه الجدول (الذي يفترض أنه كله lowercase)
kpi_final_for_dwh = kpi_daily_expanded.select(
    [col(c).alias(c.lower()) for c in kpi_daily_expanded.columns]
)
# اختيار الأعمدة النهائية (لضمان ترتيبها ومطابقتها لجدول PostgreSQL الذي يفترض أن يكون بأحرف صغيرة)
kpi_final_for_dwh = kpi_final_for_dwh.select(
    "window_start", "window_end", "location", 
    "avg_env_health_score_1d", "pct_time_dry", 
    "anomaly_count_day", "error_count_day", 
    "records_day", "farm_health_grade"
)

kpi_daily_sql_q = (
    kpi_final_for_dwh # استخدام الـ DataFrame الجاهز والموحد
    .writeStream
    .outputMode("update")
    .foreachBatch(write_kpis_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/kpi_daily_sql_dwh_v2")
    .start()
)

# Sessionization (15 min gap)
dry_events = (
    events_for_fact
    .filter(col("needs_watering") == 1)
    .select("event_ts", "location", "soil_humidity_percent")
)
dry_sessions = (
    dry_events
    .withWatermark("event_ts", "20 minutes")
    .groupBy(
        session_window(col("event_ts"), "15 minutes"),
        col("location")
    )
    .agg(
        F.avg("soil_humidity_percent").alias("avg_humidity_during_dry_session"),
        F.count("*").alias("event_count_in_session"),
        F.max("event_ts").alias("session_end_time"),
        F.min("event_ts").alias("session_start_time")
    )
    .withColumn("session_duration_minutes",
                (col("session_end_time").cast("long") - col("session_start_time").cast("long")) / 60
    )
    .filter(col("session_duration_minutes") > 2) 
)

# Sink 9: Dry Sessions (SESSIONIZATION) to SQL DWH
dry_sessions_q = (
    dry_sessions
    .writeStream
    .outputMode("append")
    .foreachBatch(write_sessions_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/dry_sessions_sql_dwh")
    .start()
)

# Sink 10: Final console sample for debugging events
console_events_q = (
    events_for_fact
    .select("event_ts", "location", "soil_temperature_c", "soil_humidity_percent",
                           "needs_watering",
                           "possible_overheating", "env_health_score","ml_anomaly_score")
    .writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

print("\n--- 🚀 All Streaming Queries Started 🚀 ---")
print("Writing to Kafka topics:", OUTPUT_EVENTS_TOPIC, OUTPUT_TRENDS_TOPIC, OUTPUT_KPIS_TOPIC)
print("Writing to DWH tables:", FACT_TABLE_NAME, KPI_TABLE_NAME, SESSIONS_TABLE_NAME, DIM_DATE_TABLE_NAME)
print("Writing to Delta Lake paths:", f"{PARQUET_BASE_PATH}/delta_lake/")
print("--- Awaiting termination ---")
spark.streams.awaitAnyTermination()