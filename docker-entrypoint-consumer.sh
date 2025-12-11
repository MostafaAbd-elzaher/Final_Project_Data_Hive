#!/bin/bash
set -e

echo "==============================================="
echo "🚀 Starting Spark Consumer Service..."
echo "==============================================="

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
max_attempts=30
attempt=0
while ! nc -z kafka 29092; do
  attempt=$((attempt+1))
  if [ $attempt -ge $max_attempts ]; then
    echo "❌ Kafka failed to start after $max_attempts attempts"
    exit 1
  fi
  sleep 2
  echo "⏳ Still waiting for Kafka... (attempt $attempt/$max_attempts)"
done
echo "✅ Kafka is ready!"

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
attempt=0
while ! nc -z postgres 5432; do
  attempt=$((attempt+1))
  if [ $attempt -ge $max_attempts ]; then
    echo "❌ PostgreSQL failed to start after $max_attempts attempts"
    exit 1
  fi
  sleep 2
  echo "⏳ Still waiting for PostgreSQL... (attempt $attempt/$max_attempts)"
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
