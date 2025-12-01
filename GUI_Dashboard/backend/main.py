from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import json
import os
from kafka_consumer import consume_messages
from database import get_db_connection, init_db

app = FastAPI(title="Farm IoT Dashboard")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connection Manager for WebSockets
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    init_db()
    # Start Kafka Consumer in background
    asyncio.create_task(consume_messages("farmSensors", manager))

@app.get("/")
def read_root():
    return {"status": "online", "service": "Farm IoT Backend"}

@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# API Endpoints for Dashboard
@app.get("/api/kpis/daily")
def get_daily_kpis():
    conn = get_db_connection()
    if not conn:
        return {"total_yield": 0, "avg_moisture": 0, "active_sensors": 0}
    
    try:
        cur = conn.cursor()
        # Mock logic for now, replace with real queries
        return {
            "total_yield": 1250.5,
            "avg_moisture": 45.2,
            "active_sensors": 24
        }
    finally:
        conn.close()

@app.get("/api/locations")
def get_locations():
    # Return static locations for map
    return [
        {"id": 1, "name": "Greenhouse A", "latitude": 30.05, "longitude": 31.25, "status": "active"},
        {"id": 2, "name": "Field North", "latitude": 30.06, "longitude": 31.24, "status": "warning"},
        {"id": 3, "name": "Field South", "latitude": 30.04, "longitude": 31.26, "status": "active"},
    ]

class ChatMessage(BaseModel):
    message: str

@app.post("/api/chat")
async def chat_endpoint(chat: ChatMessage):
    msg = chat.message.lower()
    if "status" in msg:
        return {"response": "System is fully operational. All sensors are active."}
    elif "temperature" in msg:
        return {"response": "Current average temperature is 24.5°C."}
    elif "moisture" in msg:
        return {"response": "Soil moisture levels are optimal at 45%."}
    elif "hello" in msg or "hi" in msg:
        return {"response": "Hello! I am your Farm Assistant. How can I help you today?"}
    else:
        return {"response": "I'm not sure about that. Try asking about status, temperature, or moisture."}
