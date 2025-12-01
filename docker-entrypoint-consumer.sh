#!/bin/bash
set -e

echo "==============================================="
echo "🚀 Starting Spark Consumer Service..."
echo "==============================================="

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
while ! nc -z kafka 29092; do
  sleep 2
  echo "⏳ Still waiting for Kafka..."
done
echo "✅ Kafka is ready!"

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
while ! nc -z postgres 5432; do
  sleep 2
  echo "⏳ Still waiting for PostgreSQL..."
done
echo "✅ PostgreSQL is ready!"

# Additional wait to ensure services are fully initialized
echo "⏳ Waiting for services to fully initialize..."
sleep 10

echo "==============================================="
echo "🔥 Starting Spark Streaming Job..."
echo "==============================================="

# Run Spark Consumer
exec spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.sql.streaming.checkpointLocation.deleteOnStop=false \
  /app/Consumer/Spark_Transformation_v1.1.py
