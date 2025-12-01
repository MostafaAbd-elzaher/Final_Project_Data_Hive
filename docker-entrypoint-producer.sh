#!/bin/bash
set -e

echo "==============================================="
echo "🚀 Starting Producer Service..."
echo "==============================================="

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
while ! nc -z kafka 29092; do
  sleep 2
  echo "⏳ Still waiting for Kafka..."
done

echo "✅ Kafka is ready!"
echo "==============================================="
echo "🌱 Starting IoT Sensor Simulator..."
echo "==============================================="

# Run the Producer
exec python3 /app/Producer/IotSystem_Version1.1.py
