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
import os
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = "farmSensors"
OUTPUT_EVENTS_TOPIC = "farmInsights"
OUTPUT_TRENDS_TOPIC = "farmTrends"
OUTPUT_KPIS_TOPIC = "farmKpis"
HOME_PATH = os.getenv('HOME', '/root')
PARQUET_BASE_PATH = os.getenv('PARQUET_BASE_PATH', f"{HOME_PATH}/spark_project_data/farm_iot_parquet")
CHECKPOINT_BASE = os.getenv('CHECKPOINT_BASE', f"{HOME_PATH}/spark_project_data/checkpoints/farm_iot_full_pipeline")
PROCESSING_TRIGGER = "30 seconds"

DB_URL = os.getenv('DB_URL', 'jdbc:postgresql://postgres:5432/farm_dwh')
DB_PROPERTIES = {
    "user": os.getenv('POSTGRES_USER', 'spark_user'),
    "password": os.getenv('POSTGRES_PASSWORD', 'spark_password'),
    "driver": "org.postgresql.Driver"
}
KPI_TABLE_NAME = "daily_farm_kpis"      
DIM_TABLE_NAME = "dim_location"
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
spark = (
    SparkSession.builder
    .appName("FarmIoTFullPipeline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

features_for_model = struct(
    col("soil_temperature_c").alias("soil_temp"),
    col("soil_humidity_percent").alias("soil_hum"),
    col("soil_ph"),
    col("light_intensity_lux")
)

def predict_anomaly(series: pd.Series) -> pd.Series:

    def check_row(x):
        if x is None: return 0
        try:
            if x['soil_temp'] > 35 and x['soil_hum'] < 40:
                return 1
            else:
                return 0
        except (TypeError, KeyError):
            return 0

    return series.apply(check_row)

ml_anomaly_udf = pandas_udf(predict_anomaly, returnType=IntegerType())

print("✅ ML Anomaly Detection UDF is defined.")

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

location_dim_df = spark.createDataFrame(
    data=location_dim_data,
    schema=location_dim_schema
)

try:
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
events_enriched = (
    events_cleaned
    .withColumn("temp_diff_air_soil", col("air_temperature_c") - col("soil_temperature_c"))
    .withColumn("humidity_diff_air_soil", col("air_humidity_percent") - col("soil_humidity_percent"))
    .withColumn("ph_status",
        when(col("soil_ph").isNull(), lit("Unknown"))
        .when(col("soil_ph") < 6.0, lit("Acidic"))
        .when(col("soil_ph") > 8.0, lit("Alkaline"))
        .otherwise(lit("Normal"))
    )
    .withColumn("salinity_status",
        when(col("soil_salinity_ds_m") < 2.0, lit("Low"))
        .when((col("soil_salinity_ds_m") >= 2.0) & (col("soil_salinity_ds_m") < 4.0), lit("Moderate"))
        .otherwise(lit("High"))
    )
    .withColumn("is_anomaly_temp",
        when((col("soil_temperature_c") > 40) | (col("air_temperature_c") > 40) | (col("soil_temperature_c") < -20), lit(1)).otherwise(lit(0))
    )
    .withColumn("is_anomaly_humidity",
        when((col("soil_humidity_percent") < 30) | (col("soil_humidity_percent") > 100), lit(1)).otherwise(lit(0))
    )
    .withColumn("is_sensor_error", when(col("is_error") == True, lit(1)).otherwise(lit(0)))
    .withColumn("needs_watering", when(col("soil_humidity_percent") < 30, lit(1)).otherwise(lit(0)))
    .withColumn("possible_overheating", when((col("soil_temperature_c") > 40) | (col("air_temperature_c") > 40), lit(1)).otherwise(lit(0)))
    .withColumn("ph_not_optimal", when((col("soil_ph") < 6) | (col("soil_ph") > 8), lit(1)).otherwise(lit(0)))
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
agg_5m = (
    events_enriched
    .groupBy(window(col("event_ts"), "5 minutes", "1 minute"), col("location"))
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

agg_1h = (
    events_enriched
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

agg_1d = (
    events_enriched
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
agg5_time = agg_5m.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("location"),
    "avg_soil_temp_5m", "avg_soil_humidity_5m", "avg_salinity_5m",
    "std_soil_temp_5m", "std_soil_humidity_5m",
    "anomaly_temp_count_5m", "anomaly_humidity_count_5m", "error_count_5m", "records_5m"
)
agg5_compact = agg5_time.select(
    col("location"),
    col("window_start"),
    col("window_end"),
    "avg_soil_temp_5m", "std_soil_temp_5m",
    "avg_soil_humidity_5m", "std_soil_humidity_5m"
)
agg5_compact = agg5_compact.withWatermark("window_start", "10 minutes")

events_with_window = events_enriched.alias("events").join(
    agg5_compact.alias("agg"),
    (col("events.location") == col("agg.location")) &
    (col("events.event_ts") >= col("agg.window_start")) &
    (col("events.event_ts") < col("agg.window_end")),
    how="left"
).drop(col("agg.location"))
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
reliability_1h = (
    agg_1h
    .select("window", "location", "avg_soil_temp_1h", "avg_soil_humidity_1h", "avg_salinity_1h", "std_soil_temp_1h", "error_count_1h", "records_1h")
    .withColumn("error_ratio_1h", col("error_count_1h") / col("records_1h"))
    .withColumn("variance_ratio_temp", col("std_soil_temp_1h") / (col("avg_soil_temp_1h") + lit(0.0001)))
    .withColumn("sensor_reliability_score",
        (lit(100) - (col("error_ratio_1h") * lit(100) * lit(2.0)) - (col("variance_ratio_temp") * lit(50)))
    )
)
top_anomalies_1h = (
    agg_1h
    .withColumn("anomaly_total_1h", col("anomaly_temp_count_1h") + lit(0))
    .select(col("location"), col("anomaly_temp_count_1h"), col("records_1h"))
    .withColumn("anomaly_rate_1h", col("anomaly_temp_count_1h") / col("records_1h"))
)
kpi_daily = (
    events_enriched
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
    events_enriched
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
dry_events = (
    events_enriched
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

def write_trends_to_kafka(batch_df, batch_id):
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

def write_kpis_to_sql_batch(batch_df, batch_id):
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

def process_top_anomalies_batch(batch_df, batch_id):
print(f"\n--- Processing Top 5 Anomalies Batch: {batch_id} ---")
    w_rank_batch = SparkWindow.orderBy(col("anomaly_rate_1h").desc_nulls_last())
    top5_batch = (
        batch_df
        .withColumn("rank", row_number().over(w_rank_batch))
        .filter(col("rank") <= 5)
        .select("location", "anomaly_rate_1h", "rank")
    )
    top5_batch.show(truncate=False)

FACT_TABLE_NAME = "fact_sensor_events"
SESSIONS_TABLE_NAME = "farm_dry_sessions"

def write_events_to_sql_batch(batch_df, batch_id):
print(f"--- 🚀 Writing Fact Table Batch {batch_id} to SQL DWH ({FACT_TABLE_NAME}) ---")

    try:
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
            "is_outlier_temp_z",
            "is_outlier_hum_z",
            "ml_anomaly_score",
            "is_sensor_error"
        )
        (fact_data
         .write
         .jdbc(url=DB_URL,
               table=FACT_TABLE_NAME,
               mode="append",
               properties=DB_PROPERTIES)
        )
        
        print(f"--- ✅ Fact Batch {batch_id} successfully written to {FACT_TABLE_NAME} ---")
    
    except Exception as e:
        print(f"--- 🔥 Error writing Fact Batch {batch_id} to SQL: {e} ---")

def write_sessions_to_sql_batch(batch_df, batch_id):
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
        (processed_batch_df
         .write
         .jdbc(url=DB_URL,
               table=SESSIONS_TABLE_NAME,
               mode="append",
               properties=DB_PROPERTIES)
        )
        
        print(f"--- ✅ Sessions Batch {batch_id} successfully written to {SESSIONS_TABLE_NAME} ---")
    
    except Exception as e:
        print(f"--- 🔥 Error writing Sessions Batch {batch_id} to SQL: {e} ---")

events_final_ml = events_outliers.withColumn(
    "ml_anomaly_score", ml_anomaly_udf(features_for_model)
)
events_to_kafka = events_final_ml.select(
    to_json(struct(*[c for c in events_final_ml.columns])).alias("value")
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
events_lake_q = (
    events_final_ml.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", f"{PARQUET_BASE_PATH}/delta_lake/all_events")
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_delta_lake")
    .start()
)

events_sql_dwh_q = (
    events_final_ml
    .writeStream
    .outputMode("append")
    .foreachBatch(write_events_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_fact_table_sql_dwh")
    .start()
)
print("✅ Fact Table SQL DWH Sink (events_sql_dwh_q) started.")

trends_delta_q = (
    agg5_time
    .writeStream
    .outputMode("update")
    .foreachBatch(write_trends_to_kafka)
    .option("checkpointLocation", CHECKPOINT_BASE + "/trends_delta_kafka")
    .start()
)
print("✅ Trends (Delta) Sink (trends_delta_q) started.")
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
kpi_daily_sql_q = (
    kpi_daily_grade
    .writeStream
    .outputMode("update")
    .foreachBatch(write_kpis_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/kpi_daily_sql_dwh")
    .start()
)
print("✅ SQL DWH Sink (kpi_daily_sql_q) started.")
reliability_parquet_q = (
    reliability_1h
    .withColumn("window_start", col("window").start)
    .withColumn("window_end", col("window").end)
    .drop("window")
    .writeStream
    .format("parquet")
    .option("path", f"{PARQUET_BASE_PATH}/reliability_1h")
    .option("checkpointLocation", CHECKPOINT_BASE + "/reliability_parquet")
    .outputMode("append")
    .start()
)
top5_q = (
    top_anomalies_1h.writeStream
    .foreachBatch(process_top_anomalies_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_BASE + "/top5_console")
    .start()
)

dry_sessions_q = (
    dry_sessions
    .writeStream
    .outputMode("append")
    .foreachBatch(write_sessions_to_sql_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "/dry_sessions_sql_dwh")
    .start()
)
print("✅ Dry Sessions SQL DWH Sink (dry_sessions_q) started.")
console_events_q = (
    events_final_ml.select("event_ts", "location", "soil_temperature_c", "soil_humidity_percent",
                           "is_outlier_temp_z", "is_outlier_hum_z", "needs_watering",
                           "possible_overheating", "env_health_score","ml_anomaly_score")
    .writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

print("Streaming started. Topics ->", OUTPUT_EVENTS_TOPIC, OUTPUT_TRENDS_TOPIC, OUTPUT_KPIS_TOPIC)
spark.streams.awaitAnyTermination()
