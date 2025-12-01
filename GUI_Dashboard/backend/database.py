import os
import psycopg2
from psycopg2.extras import RealDictCursor

import time

def get_db_connection():
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                database=os.getenv('POSTGRES_DB', 'farm_dwh'),
                user=os.getenv('POSTGRES_USER', 'spark_user'),
                password=os.getenv('POSTGRES_PASSWORD', 'spark_password')
            )
            return conn
        except Exception as e:
            print(f"DB Connection Error: {e}")
            retries -= 1
            if retries > 0:
                print(f"Retrying in 2 seconds... ({retries} attempts left)")
                time.sleep(2)
            else:
                return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            # Create tables if not exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS readings (
                    id SERIAL PRIMARY KEY,
                    sensor_id VARCHAR(50),
                    value FLOAT,
                    unit VARCHAR(20),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            print("✅ Database initialized.")
        except Exception as e:
            print(f"DB Init Error: {e}")
        finally:
            conn.close()
