#!/bin/bash
#
# start_system.sh - Start the entire Farm IoT System on Linux
# This script starts Docker containers and services
#

set -e

echo "=================================================="
echo "🌱 Farm IoT System - Linux Startup Script"
echo "=================================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "📁 Project directory: $SCRIPT_DIR"

# Load environment variables from .env (but do not override the running user's HOME)
if [ -f "$SCRIPT_DIR/.env" ]; then
    # Read .env line-by-line, skip comments/empty lines and skip any HOME assignment
    while IFS= read -r line || [ -n "$line" ]; do
        # skip empty lines and comment lines that start with '#'
        if [ -z "$line" ] || printf '%s\n' "$line" | grep -q '^#'; then
            continue
        fi
        key="${line%%=*}"
        if [ "$key" = "HOME" ]; then
            echo "⚠️  Skipping HOME variable from .env to avoid overriding user HOME"
            continue
        fi
        export "$line"
    done < "$SCRIPT_DIR/.env"
    echo "✅ Loaded environment variables from .env (HOME skipped if present)"
else
    echo "⚠️  .env file not found. Using default values."
fi

# Create required directories for data persistence
echo "📂 Creating required directories..."
mkdir -p "$HOME/spark_project_data/output"
mkdir -p "$HOME/spark_project_data/farm_iot_parquet"
mkdir -p "$HOME/spark_project_data/checkpoints/farm_iot_full_pipeline"

# Pull latest images
echo "📥 Pulling latest Docker images..."
docker-compose pull

# Start services
echo "🚀 Starting Docker Compose services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check service health
echo "🔍 Checking service health..."
docker-compose ps

echo ""
echo "=================================================="
echo "✅ System startup complete!"
echo "=================================================="
echo ""
echo "📊 Services started:"
echo "  - Zookeeper: localhost:2181"
echo "  - Kafka: localhost:9092 (internal: kafka:29092)"
echo "  - PostgreSQL: localhost:5432"
echo "  - InfluxDB: http://localhost:8086"
echo "  - Grafana: http://localhost:3001 (admin/admin)"
echo "  - Backend API: http://localhost:8000"
echo "  - Frontend: http://localhost:3000"
echo ""
echo "📝 Useful commands:"
echo "  View logs: docker-compose logs -f"
echo "  Stop system: ./stop_system.sh"
echo "  View specific service logs: docker-compose logs -f <service_name>"
echo ""
