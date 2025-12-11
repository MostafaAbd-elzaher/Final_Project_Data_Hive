#!/bin/bash
#
# start_frontend.sh - Start the Frontend Dashboard service on Linux
#

set -e

echo "=================================================="
echo "🎨 Farm IoT Frontend Dashboard - Linux Startup"
echo "=================================================="

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "📝 Frontend Configuration:"
echo "  - Frontend Port: 3000"
echo "  - Backend URL: http://localhost:8000"

echo ""
echo "🚀 Starting Frontend service..."
docker-compose up -d frontend

echo "⏳ Waiting for Frontend to start..."
sleep 5

echo "📊 Frontend logs:"
docker-compose logs -f frontend

echo ""
echo "✅ Frontend is running at: http://localhost:3000"
