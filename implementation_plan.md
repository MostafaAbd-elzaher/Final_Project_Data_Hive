# Run IoT Data Hive Project

## Goal Description
Run the full IoT Data Hive system with a single location simulation (Cairo) and corrected Kafka bootstrap settings (`kafka:29092`). Ensure all services start correctly and the dashboard displays live data.

## Proposed Changes
- No further code changes needed (Kafka bootstrap already fixed, location simulation simplified).
- No new files required.

## Verification Plan
### Automated Tests
- **Docker Compose Build & Up**: `docker-compose up -d --build`
- **Service Health Checks**:
  - `docker exec kafka kafka-topics --list --bootstrap-server kafka:29092`
  - `docker exec postgres psql -U spark_user -d farm_dwh -c "\dt"`
  - `curl -s http://localhost:8086/health`
  - `curl -s http://localhost:3001/api/health`
- **Log Monitoring**: `docker-compose logs -f` to ensure no errors.

### Manual Verification
1. Open `http://localhost:3000` in a browser and confirm the dashboard shows live sensor data.
2. Open Grafana at `http://localhost:3001` (admin/admin) and verify the InfluxDB data source is populated.
3. Check the backend API docs at `http://localhost:8000/docs` for a healthy Swagger UI.
