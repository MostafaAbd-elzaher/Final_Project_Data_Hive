#!/bin/bash
#
# start_backend.sh - Start the Backend Dashboard service on Linux
#

set -e

echo "=================================================="
echo "📊 Farm IoT Backend Dashboard - Linux Startup"
echo "=================================================="

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Load environment variables
export $(cat "$SCRIPT_DIR/.env" | grep -v '#' | xargs)

echo "📝 Environment:"
echo "  - PostgreSQL Host: $POSTGRES_HOST"
echo "  - Backend Port: 8000"

echo ""
echo "🚀 Starting Backend service..."
docker-compose up -d postgres grafana backend

echo "⏳ Waiting for Backend to start..."
sleep 5

echo "📊 Backend logs:"
docker-compose logs -f backend
