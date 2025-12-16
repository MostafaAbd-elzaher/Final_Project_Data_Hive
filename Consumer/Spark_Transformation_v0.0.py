import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg as _avg, stddev as _stddev,
    count as _count, lit, when, expr, to_json, struct, row_number, abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType
)
from pyspark.sql.window import Window

import os
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = "farmSensors"
OUTPUT_EVENTS_TOPIC = "farmInsights"
OUTPUT_TRENDS_TOPIC = "farmTrends"
OUTPUT_KPIS_TOPIC = "farmKpis"
HOME_PATH = os.getenv('HOME', '/root')
PARQUET_BASE_PATH = os.getenv('PARQUET_BASE_PATH', f"{HOME_PATH}/spark_project_data/farm_iot_parquet")
CHECKPOINT_BASE = f"{HOME_PATH}/spark_project_data/checkpoints/farm_iot_full_pipeline"
PROCESSING_TRIGGER = "30 seconds"
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
events_enriched = (
    parsed
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
events_with_window = events_enriched.join(
    agg5_compact,
    (events_enriched.location == agg5_compact.location) &
    (events_enriched.event_ts >= agg5_compact.window_start) &
    (events_enriched.event_ts < agg5_compact.window_end),
    how="left"
).drop(agg5_compact.location)
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

def process_top_anomalies_batch(batch_df, batch_id):
print(f"\n--- Processing Top 5 Anomalies Batch: {batch_id} ---")
    w_rank_batch = Window.orderBy(col("anomaly_rate_1h").desc_nulls_last())
    top5_batch = (
        batch_df
        .withColumn("rank", row_number().over(w_rank_batch))
        .filter(col("rank") <= 5)
        .select("location", "anomaly_rate_1h", "rank")
    )
    top5_batch.show(truncate=False)
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
events_lake_q = (
    events_outliers.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", f"{PARQUET_BASE_PATH}/delta_lake/all_events")
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_delta_lake")
    .start()
)

DB_URL = os.getenv('DB_URL', 'jdbc:postgresql://postgres:5432/farm_dwh')
DB_PROPERTIES = {
    "user": os.getenv('POSTGRES_USER', 'spark_user'),
    "password": os.getenv('POSTGRES_PASSWORD', 'spark_password'),
    "driver": "org.postgresql.Driver"
}
KPI_TABLE_NAME = "daily_farm_kpis"

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
trends_for_kafka = agg5_time.select(
    to_json(struct(
        col("window_start").alias("window_start"),
        col("window_end").alias("window_end"),
        col("location"),
        col("avg_soil_temp_5m"),
        col("avg_soil_humidity_5m"),
        col("anomaly_temp_count_5m"),
        col("error_count_5m")
    )).alias("value")
)

trends_kafka_q = (
    trends_for_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TRENDS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_BASE + "/trends_to_kafka")
    .outputMode("update")
    .start()
)
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
console_events_q = (
    events_outliers.select("event_ts", "location", "soil_temperature_c", "soil_humidity_percent",
                           "is_outlier_temp_z", "is_outlier_hum_z", "needs_watering",
                           "possible_overheating", "env_health_score")
    .writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

print("Streaming started. Topics ->", OUTPUT_EVENTS_TOPIC, OUTPUT_TRENDS_TOPIC, OUTPUT_KPIS_TOPIC)
spark.streams.awaitAnyTermination()
