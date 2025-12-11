#!/bin/bash
#
# start_producer.sh - Start the Producer service on Linux
#

set -e

echo "=================================================="
echo "🌱 Farm IoT Producer - Linux Startup"
echo "=================================================="

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Load environment variables
export $(cat "$SCRIPT_DIR/.env" | grep -v '#' | xargs)

echo "📝 Environment:"
echo "  - Kafka Bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
echo "  - Output Directory: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo ""
echo "🚀 Starting Producer service..."
docker-compose up -d zookeeper kafka producer

echo "⏳ Waiting for Producer to start..."
sleep 5

echo "📊 Producer logs:"
docker-compose logs -f producer
