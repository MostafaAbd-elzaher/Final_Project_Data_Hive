#!/bin/bash
#
# start_consumer.sh - Start the Consumer (Spark) service on Linux
#

set -e

echo "=================================================="
echo "🔥 Farm IoT Consumer (Spark) - Linux Startup"
echo "=================================================="

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Load environment variables
export $(cat "$SCRIPT_DIR/.env" | grep -v '#' | xargs)

echo "📝 Environment:"
echo "  - Kafka Bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
echo "  - PostgreSQL Host: $POSTGRES_HOST"
echo "  - Parquet Base Path: $PARQUET_BASE_PATH"

# Create data directories
mkdir -p "$PARQUET_BASE_PATH"
mkdir -p "$(echo $CHECKPOINT_BASE | sed 's#/[^/]*$##')"

echo ""
echo "🚀 Starting Consumer service..."
docker-compose up -d kafka postgres consumer

echo "⏳ Waiting for Consumer to start..."
sleep 10

echo "📊 Consumer logs:"
docker-compose logs -f consumer
