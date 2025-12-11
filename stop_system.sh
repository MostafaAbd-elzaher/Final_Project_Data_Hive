#!/bin/bash
#
# stop_system.sh - Stop the entire Farm IoT System on Linux
#

set -e

echo "=================================================="
echo "🌱 Farm IoT System - Linux Shutdown Script"
echo "=================================================="

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "⏹️  Stopping Docker Compose services..."
docker-compose down

echo ""
echo "=================================================="
echo "✅ System stopped successfully!"
echo "=================================================="
echo ""
echo "💾 Data volumes are preserved. Run './start_system.sh' to restart."
echo ""
