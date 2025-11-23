"""
Spark Structured Streaming - Full IoT Insights Pipeline (V3 - Production Grade)

Reads:    Kafka INPUT_TOPIC ("farm_sensors") with JSON sensor events
Produces: Enriched events -> Kafka OUTPUT_EVENTS_TOPIC ("farm_insights")
          Trend insights (5min sliding) -> Kafka OUTPUT_TRENDS_TOPIC ("farm_trends")
          KPI aggregates (daily, weekly) -> Kafka OUTPUT_KPIS_TOPIC ("farm_kpis")
          DWH Tables (Fact, Dimension, Aggregates)
          Delta Lake (Silver Layer)

Features implemented:
- Per-event enrichment (env_health_score, ph_status, salinity_status, flags)
- ML Model UDF (Anomaly Detection)
- Windowed aggregates (5min sliding, 5min tumbling, 1h sliding, 1d tumbling)
- Delta & trend detection (lag on sliding aggregates)
- Outlier detection (Z-score) using Tumbling window to prevent duplication
- Sensor reliability score
- Top-N anomalies
- Sessionization (for dry periods)
- Writes results to Kafka, Delta Lake, and SQL DWH
"""
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg as _avg, stddev as _stddev,
    count as _count, lit, when, expr, to_json, struct, row_number, abs, session_window
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
DIM_TABLE_NAME = "dim_location"
FACT_TABLE_NAME = "fact_sensor_events"
SESSIONS_TABLE_NAME = "farm_dry_sessions"
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
    .appName("FarmIoTFullPipeline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ===========================
# ML Model UDF Definition
# ===========================

# features_for_model = struct(
#     col("soil_temperature_c").alias("soil_temp"),
#     col("soil_humidity_percent").alias("soil_hum"),
#     col("soil_ph"),
#     col("light_intensity_lux")
# )

# def predict_anomaly(series: pd.Series) -> pd.Series:
#     def check_row(x):
#         if x is None: return 0
#         try:
#             if x['soil_temp'] > 35 and x['soil_hum'] < 40:
#                 return 1 # (Anomaly)
#             else:
#                 return 0 # (Normal)
#         except (TypeError, KeyError):
#             return 0
#     return series.apply(check_row)
# def predict_anomaly_vectorized(features_series: pd.Series) -> pd.Series:
#     # نقوم بتطبيق المنطق على كل صف في دفعة الإدخال
    
#     # 1. استخراج الأعمدة المطلوبة
#     soil_temp = features_series.apply(lambda x: x['soil_temp'])
#     soil_hum = features_series.apply(lambda x: x['soil_hum'])
    
#     # 2. تطبيق منطق التحقق (vectorized operation)
#     # نستخدم np.where أو عمليات منطقية لضمان إرجاع Series بنفس الحجم
    
#     # تحويل المشغل (if/else) إلى عملية منطقية متجهة (Boolean Series)
#     anomaly_condition = (soil_temp > 35) & (soil_hum < 40)
    
#     # إرجاع 1 إذا تحقق الشرط، و 0 إذا لم يتحقق (يجب أن يكون الحجم متطابقاً)
#     return pd.Series(anomaly_condition.astype(int))


# ml_anomaly_udf = pandas_udf(predict_anomaly_vectorized, returnType=IntegerType())
print("✅ ML Anomaly Detection UDF is defined.")


# ===========================
# (جديد) 1b) Write Dimension Table to DWH (One-Time)
# ===========================

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

print(f"--- 🔄 Writing Dimension Table '{DIM_TABLE_NAME}' to DWH... ---")
try:
    location_dim_df = spark.createDataFrame(
        data=location_dim_data,
        schema=location_dim_schema
    )
    (location_dim_df
     .write
     .jdbc(url=DB_URL,
           table=DIM_TABLE_NAME,
           mode="overwrite",
           properties=DB_PROPERTIES)
    )
    print(f"--- ✅ Successfully created/overwritten '{DIM_TABLE_NAME}' in DWH. ---")
except Exception as e:
    print(f"--- 🔥 Error writing Dimension Table: {e} ---")
    pass


# ===========================
# Read from Kafka
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

# ===========================
# 1a) Stream-Static Join (Data Enrichment)
# ===========================

dim_location_dwh = (
    spark.read
    .jdbc(url=DB_URL,
          table=DIM_TABLE_NAME,
          properties=DB_PROPERTIES)
)
print("✅ Dimension Table (dim_location) loaded from DWH for Join.")

parsed_enriched_static = parsed.join(
    dim_location_dwh,
    parsed.location == dim_location_dwh.location_name,
    how="left"
).drop("location_name")

print("✅ Stream-Static Join (Enrichment) is configured.")

# ===========================
# 1b) Advanced Rule-based Cleaning
# ===========================
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
print("✅ Rule-based Cleaning is configured.")


# ===========================
# 1c) Per-event enrichment: flags, scores, derived cols
# ===========================
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

# ===========================
# 1d) ML Model Application (Per-Event)
# ===========================
events_enriched_ml = events_enriched.withColumn(
    "ml_anomaly_score",
    F.when(
        (F.col("soil_temperature_c") > 35) & (F.col("soil_humidity_percent") < 40),
        F.lit(1)
    ).otherwise(F.lit(0))
)
# print("✅ ML UDF applied. 'events_enriched_ml' is the new source of truth.")
print("✅ ML Logic (native Spark) applied. 'events_enriched_ml' is the new source of truth.")


# ===========================
# 2) Windowed aggregates (Separation of Concerns)
#    - 5 min TUMBLING (for Z-Score)
#    - 5 min SLIDING (for Trends)
#    - 1 hour SLIDING (for Reliability)
#    - 1 day TUMBLING (for KPIs)
# ===========================

# 2a) 5-min Tumbling (TUMBLING) -> for Z-Score & Fact Table (Prevents Duplication)
### (تعديل) تم تغيير الاسم إلى _tumbling لتمييزه
agg_5m_tumbling = (
    events_enriched_ml
    .groupBy(window(col("event_ts"),
                     "5 minutes"), col("location")) # <-- Tumbling
    .agg(
        _avg("soil_temperature_c").alias("avg_soil_temp_5m"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_5m"),
        _avg("soil_salinity_ds_m").alias("avg_salinity_5m"),
        _avg("env_health_score").alias("avg_env_health_score_5m"),
        _stddev("soil_temperature_c").alias("std_soil_temp_5m"),
        _stddev("soil_humidity_percent").alias("std_soil_humidity_5m"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_temp_count_5m"),
        _count(when(col("is_anomaly_humidity") == 1, True)).alias("anomaly_humidity_count_5m"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_5m"),
        _count("*").alias("records_5m")
    )
)

# 2b) 5-min Sliding (SLIDING) -> for real-time Trend Dashboard
### (إضافة) تم إضافة هذا الـ DataFrame المنزلق خصيصاً للـ Trends
agg_5m_sliding = (
    events_enriched_ml
    .groupBy(window(col("event_ts"), "5 minutes", "1 minute"), col("location")) # <-- Sliding
    .agg(
        _avg("soil_temperature_c").alias("avg_soil_temp_5m"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_5m"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_temp_count_5m"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_5m")
    )
)

# 2c) 1-hour Sliding -> for Reliability & Top-N
agg_1h = (
    events_enriched_ml
    .groupBy(window(col("event_ts"), "1 hour", "5 minutes"), col("location"))
    .agg(
        _avg("soil_temperature_c").alias("avg_soil_temp_1h"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_1h"),
        _avg("soil_salinity_ds_m").alias("avg_salinity_1h"),
        _avg("env_health_score").alias("avg_env_health_score_1h"),
        _stddev("soil_temperature_c").alias("std_soil_temp_1h"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_1h"),
        _count("*").alias("records_1h"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_temp_count_1h"),
    )
)

# 2d) 1-day Tumbling -> for Daily KPIs
agg_1d = (
    events_enriched_ml
    .groupBy(window(col("event_ts"), "1 day"), col("location"))
    .agg(
        _avg("env_health_score").alias("avg_env_health_score_1d"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_1d"),
        _avg("soil_temperature_c").alias("avg_soil_temp_1d"),
        _avg("soil_salinity_ds_m").alias("avg_salinity_1d"),
        _count(when(col("needs_watering") == 1, True)).alias("needs_watering_count_1d"),
        _count(when(col("possible_overheating") == 1, True)).alias("overheat_count_1d"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_1d"),
        _count("*").alias("records_1d")
    )
)
print("✅ All Aggregation streams (Tumbling & Sliding) are configured.")

# ===========================
# 3) Prepare aggregation streams for Sinks
# ===========================
# 3a) Prepare 5m Tumbling stream (for Z-Score)
### (تعديل) تم تغيير الاسم إلى _tumbling
agg5_time_tumbling = agg_5m_tumbling.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("location"),
    "avg_soil_temp_5m", "avg_soil_humidity_5m", "avg_salinity_5m",
    "std_soil_temp_5m", "std_soil_humidity_5m",
    "anomaly_temp_count_5m", "anomaly_humidity_count_5m", "error_count_5m", "records_5m"
)

# 3b) Prepare 5m Sliding stream (for Trend Sink)
### (إضافة) إعداد الـ DataFrame المنزلق للـ Sink
agg5_time_sliding = agg_5m_sliding.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("location"),
    "avg_soil_temp_5m",
    "avg_soil_humidity_5m",
    "anomaly_temp_count_5m",
    "error_count_5m"
)

# ===========================
# 4) Outlier detection (Z-score) - Using Tumbling Window
# ===========================

### (تعديل) تم تغيير الاسم إلى _tumbling
agg5_compact_tumbling = agg5_time_tumbling.select(
    col("location"),
    col("window_start"),
    col("window_end"),
    "avg_soil_temp_5m", "std_soil_temp_5m",
    "avg_soil_humidity_5m", "std_soil_humidity_5m"
)
### (تعديل) تم تغيير الاسم إلى _tumbling
agg5_compact_tumbling = agg5_compact_tumbling.withWatermark("window_start", "10 minutes")

# Join events with their 5-min TUMBLING window aggregates (1:1 join)
events_with_window = events_enriched_ml.alias("events").join(
    ### (تعديل) تم تغيير الاسم إلى _tumbling
    agg5_compact_tumbling.alias("agg"),
    (col("events.location") == col("agg.location")) &
    (col("events.event_ts") >= col("agg.window_start")) &
    (col("events.event_ts") < col("agg.window_end")),
    how="left"
).drop(col("agg.location"))

# Compute Z-score outlier flags
k_temp = 3.0
k_hum = 3.0
events_outliers = events_with_window.withColumn(
    "z_temp",
    when(col("std_soil_temp_5m").isNull(), lit(0.0))
    .otherwise( (col("soil_temperature_c") - col("avg_soil_temp_5m")) / col("std_soil_temp_5m") )
).withColumn(
    "z_hum",
    when(col("std_soil_humidity_5m").isNull(), lit(0.0))
    .otherwise( (col("soil_humidity_percent") - col("avg_soil_humidity_5m")) / col("std_soil_humidity_5m") )
).withColumn(
    "is_outlier_temp_z", when(abs(col("z_temp")) > k_temp, lit(1)).otherwise(lit(0))
).withColumn(
    "is_outlier_hum_z", when(abs(col("z_hum")) > k_hum, lit(1)).otherwise(lit(0))
)
print("✅ Z-Score (Outlier) detection stream is configured (using Tumbling Window).")


# ===========================
# 5) Sensor reliability score (sliding window)
# ===========================
reliability_1h = (
    agg_1h
    .select("window", "location", "avg_soil_temp_1h", "avg_soil_humidity_1h", "avg_salinity_1h", "std_soil_temp_1h", "error_count_1h", "records_1h")
    .withColumn("error_ratio_1h", col("error_count_1h") / col("records_1h"))
    .withColumn("variance_ratio_temp", col("std_soil_temp_1h") / (col("avg_soil_temp_1h") + lit(0.0001)))
    .withColumn("sensor_reliability_score",
        (lit(100) - (col("error_ratio_1h") * lit(100) * lit(2.0)) - (col("variance_ratio_temp") * lit(50)))
    )
)

# ===========================
# 6) Top 5 sensors by anomaly frequency (sliding 1h)
# ===========================
top_anomalies_1h = (
    agg_1h
    .withColumn("anomaly_total_1h", col("anomaly_temp_count_1h") + lit(0))
    .select(col("location"), col("anomaly_temp_count_1h"), col("records_1h"))
    .withColumn("anomaly_rate_1h", col("anomaly_temp_count_1h") / col("records_1h"))
)


# ===========================
# 7) KPI: daily/week KPIs streaming (tumbling windows)
# ===========================
kpi_daily = (
    events_enriched_ml ### (تعديل) توحيد المصدر
    .groupBy(window(col("event_ts"), "1 day"), col("location"))
    .agg(
        _avg("env_health_score").alias("avg_env_health_score_day"),
        (_count(when(col("needs_watering") == 1, True)) / _count("*")).alias("pct_time_dry"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_count_day"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_day"),
        _count("*").alias("records_day")
    )
)

kpi_weekly = (
    events_enriched_ml ### (تعديل) توحيد المصدر
    .groupBy(window(col("event_ts"), "7 days"), col("location"))
    .agg(
        _avg("env_health_score").alias("avg_env_health_score_week"),
        (_count(when(col("needs_watering") == 1, True)) / _count("*")).alias("pct_time_dry_week"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_count_week"),
        _count("*").alias("records_week")
    )
)

kpi_daily_grade = kpi_daily.withColumn(
    "farm_health_grade",
    when(col("avg_env_health_score_day") >= 80, lit("A"))
    .when(col("avg_env_health_score_day") >= 60, lit("B"))
    .when(col("avg_env_health_score_day") >= 40, lit("C"))
    .otherwise(lit("D"))
)


# ===========================
# 7b) Sessionization for 'needs_watering' events
# ===========================
dry_events = (
    events_enriched_ml ### (تعديل) توحيد المصدر
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
print("✅ Sessionization (dry_sessions) is configured.")


# ===========================
# SINK FUNCTIONS (foreachBatch)
# ===========================

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
    """
    print(f"--- 🚀 Writing KPI Batch {batch_id} to SQL DWH ({KPI_TABLE_NAME}) ---")
    
    try:
        processed_batch_df = (
            batch_df
            .withColumn("window_start", col("window").start)
            .withColumn("window_end", col("window").end)
            .drop("window")
            .select("window_start", "window_end", "location",
                    "avg_env_health_score_day", "pct_time_dry",
                    "anomaly_count_day", "error_count_day",
                    "records_day", "farm_health_grade")
        )

        (processed_batch_df
         .write
         .jdbc(url=DB_URL,
               table=KPI_TABLE_NAME,
               mode="append",
               properties=DB_PROPERTIES)
        )
        print(f"--- ✅ Batch {batch_id} successfully written to {KPI_TABLE_NAME} ---")
    except Exception as e:
        print(f"--- 🔥 Error writing Batch {batch_id} to SQL: {e} ---")
# === نهاية الدالة ===


def process_top_anomalies_batch(batch_df, batch_id):
    """
    Function to process each micro-batch of aggregated anomalies (1-hour SLIDING)
    to calculate Top-N.
    """
    print(f"\n--- Processing Top 5 Anomalies Batch: {batch_id} ---")
    w_rank_batch = SparkWindow.orderBy(col("anomaly_rate_1h").desc_nulls_last())

    top5_batch = (
        batch_df
        .withColumn("rank", row_number().over(w_rank_batch))
        .filter(col("rank") <= 5)
        .select("location", "anomaly_rate_1h", "rank")
    )
    top5_batch.show(truncate=False)
# === نهاية الدالة ===


def write_events_to_sql_batch(batch_df, batch_id):
    """
    Writes the fully enriched event-level data (Silver/Gold Layer)
    to the Fact Table in the DWH. (Consumes the final 'events_outliers' stream)
    """
    print(f"--- 🚀 Writing Fact Table Batch {batch_id} to SQL DWH ({FACT_TABLE_NAME}) ---")

    try:
        # (تم إصلاح خطأ الـ Syntax هنا)
        fact_data = batch_df.select(
            "event_ts",
            "location_id",
            "season",
            "day_period",
            "soil_temperature_c",
            "air_temperature_c",
            "soil_humidity_percent",
            "air_humidity_percent",
            "soil_ph",
            "soil_salinity_ds_m",
            "light_intensity_lux",
            "water_level_percent",
            "env_health_score",
            "ml_anomaly_score",
            "is_sensor_error"
        )
        jdbc_properties_commit = DB_PROPERTIES.copy()
        jdbc_properties_commit["v2_mode"] = "true" 
        jdbc_properties_commit["defaultRowCommitMode"] = "External"
        
        (fact_data
         .write
         .jdbc(url=DB_URL,
               table=FACT_TABLE_NAME,
               mode="append",
               properties=jdbc_properties_commit)
        )
        print(f"--- ✅ Fact Batch {batch_id} successfully written to {FACT_TABLE_NAME} ---")
    except Exception as e:
        print(f"--- 🔥 Error writing Fact Batch {batch_id} to SQL: {e} ---")
# === نهاية الدالة ===


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
               properties=jdbc_properties_commit,
               )
        )
        print(f"--- ✅ Sessions Batch {batch_id} successfully written to {SESSIONS_TABLE_NAME} ---")
    except Exception as e:
        print(f"--- 🔥 Error writing Sessions Batch {batch_id} to SQL: {e} ---")
# === نهاية الدالة ===


# ===========================
# 8) Output Sinks (Start all streams)
# ===========================

# --- 8a) Event-Level Sinks (Kafka, Delta Lake, DWH Fact Table) ---
# المصدر: events_outliers (يحتوي على Z-Score + ML Score)
# (تنبيه: تم إزالة التعريف الزائد لـ 'events_final_ml')

# Sink 1: Enriched events to Kafka
events_to_kafka = events_outliers.select(
    to_json(struct(*[c for c in events_outliers.columns])).alias("value")
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
    events_enriched_ml ### (تعديل) المصدر هو events_outliers
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("path", f"{PARQUET_BASE_PATH}/delta_lake/all_events")
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_delta_lake")
    .start()
)

# Sink 3: Enriched events to SQL DWH (Fact Table)
events_sql_dwh_q = (
    events_enriched_ml
    .writeStream
    .outputMode("append")
    .foreachBatch(write_events_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_fact_table_sql_dwh")
    .start()
)
print("✅ Event Sinks (Kafka, Delta, DWH Fact) started.")


# --- 8b) Aggregation-Level Sinks ---

# Sink 4: Trend insights (5m SLIDING) to Kafka
trends_delta_q = (
    agg5_time_sliding ### (تعديل) المصدر هو النافذة المنزلقة
    .writeStream
    .outputMode("update")
    .foreachBatch(write_trends_to_kafka)
    .option("checkpointLocation", CHECKPOINT_BASE + "/trends_delta_kafka")
    .start()
)
print("✅ Trends (Sliding) Sink (trends_delta_q) started.")

# Sink 5: KPIs daily (1d TUMBLING) to Kafka
kpi_daily_kafka = kpi_daily_grade.select(
    to_json(struct(
        col("window").start.alias("window_start"),
        col("window").end.alias("window_end"),
        col("location"),
        col("avg_env_health_score_day"),
        col("pct_time_dry"),
        col("anomaly_count_day"),
        col("error_count_day"),
        col("records_day"),
        col("farm_health_grade")
    )).alias("value")
)

kpi_daily_q = (
    kpi_daily_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_KPIS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_BASE + "/kpi_daily_kafka")
    .outputMode("update")
    .start()
)

# Sink 6: KPIs daily (1d TUMBLING) to SQL DWH (Gold Table)
kpi_daily_sql_q = (
    kpi_daily_grade
    .writeStream
    .outputMode("update")
    .foreachBatch(write_kpis_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/kpi_daily_sql_dwh")
    .start()
)
print("✅ KPI Sinks (Kafka, DWH Gold) started.")

# Sink 7: Reliability (1h SLIDING) to Parquet/Delta (Monitoring)
reliability_parquet_q = (
    reliability_1h
    .withColumn("window_start", col("window").start)
    .withColumn("window_end", col("window").end)
    .drop("window")
    .writeStream
    .format("delta") # (الأفضل استخدام Delta بدلاً من Parquet)
    .outputMode("append")
    .option("path", f"{PARQUET_BASE_PATH}/delta_lake/reliability_1h")
    .option("checkpointLocation", CHECKPOINT_BASE + "/reliability_delta")
    .start()
)

# Sink 8: Top5 anomalies (1h SLIDING) to console
top5_q = (
    top_anomalies_1h.writeStream
    .foreachBatch(process_top_anomalies_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_BASE + "/top5_console")
    .start()
)

# Sink 9: Dry Sessions (SESSIONIZATION) to SQL DWH
dry_sessions_q = (
    dry_sessions
    .writeStream
    .outputMode("append") # (ملاحظة: append هو الصحيح للجلسات)
    .foreachBatch(write_sessions_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/dry_sessions_sql_dwh")
    .start()
)
print("✅ Reliability, Top5, and Session Sinks started.")

# Sink 10: Final console sample for debugging events
console_events_q = (
    events_enriched_ml
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
print("Writing to DWH tables:", FACT_TABLE_NAME, KPI_TABLE_NAME, SESSIONS_TABLE_NAME)
print("Writing to Delta Lake paths:", f"{PARQUET_BASE_PATH}/delta_lake/")
print("--- Awaiting termination ---")
spark.streams.awaitAnyTermination()