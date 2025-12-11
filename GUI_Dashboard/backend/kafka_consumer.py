import asyncio
import json
import os
import logging
from kafka import KafkaConsumer
from database import get_db_connection

logger = logging.getLogger(__name__)

async def consume_messages(topic: str, manager):
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    logger.info(f"--- Connecting to Kafka at {bootstrap_servers} ---")

    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap_servers],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id=f'farm-iot-group-{os.getenv("HOSTNAME", "consumer")}',
                session_timeout_ms=30000
            )
            logger.info(f"✅ Connected to Kafka topic: {topic}")
        except Exception as e:
            logger.error(f"❌ Kafka Connection Failed: {e}")
            await asyncio.sleep(5)

    try:
        loop = asyncio.get_event_loop()
        while True:
            # Non-blocking consume
            msg = await loop.run_in_executor(None, next, consumer)
            data = msg.value
            
            # Broadcast to WebSocket
            await manager.broadcast(json.dumps(data))
            
            # Here you can add logic to save to DB if needed
            
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")
