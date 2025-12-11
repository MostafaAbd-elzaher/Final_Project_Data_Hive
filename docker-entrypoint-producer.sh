#!/bin/bash
set -e

echo "==============================================="
echo "🚀 Starting Producer Service..."
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
echo "==============================================="
echo "🌱 Starting IoT Sensor Simulator..."
echo "==============================================="

# Run the Producer
exec python3 /app/Producer/IotSystem_Version1.1.py
