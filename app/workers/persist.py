import os
import time
import logging
from datetime import datetime, timedelta
import redis
import psycopg2
from psycopg2.extras import execute_values
import json
from app.config.persist import PERSIST_INTERVAL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Environment config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "yourdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "youruser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "yourpassword")

# Redis client
r = redis.Redis(host=REDIS_HOST, port=6379, db=0)

def _buffer_key(device_id: str) -> str:
    return f"telemetry_buffer:{device_id}"

def get_devices():
    """
    Retrieve all device IDs from Redis keys pattern.
    Assumes telemetry buffers are stored as telemetry_buffer:<device_id>
    """
    keys = r.keys("telemetry_buffer:*")
    device_ids = [k.decode().split(":")[1] for k in keys]
    return device_ids

def get_buffer(device_id, starts_at=None):
    """
    Retrieve buffered telemetry for a device, optionally filtering by timestamp.
    """
    raw_entries = r.lrange(_buffer_key(device_id), 0, -1)
    entries = []
    for raw in raw_entries:
        try:
            entry = json.loads(raw)
            if starts_at:
                entry_ts = datetime.fromisoformat(entry["timestamp"])
                if entry_ts <= starts_at:
                    continue
            entries.append(entry)
        except Exception as e:
            logging.warning(f"Failed to parse telemetry for device {device_id}: {e}")
    return entries

def aggregate_telemetry(entries):
    """
    Aggregate telemetry data, e.g. average voltage, current, pf.
    Returns a dict of aggregated values.
    """
    if not entries:
        return None

    voltage_sum = 0
    current_sum = 0
    pf_sum = 0
    count = 0

    for e in entries:
        voltage_sum += e.get("voltage", 0)
        current_sum += e.get("current", 0)
        pf_sum += e.get("pf", 0)
        count += 1

    return {
        "avg_voltage": voltage_sum / count,
        "avg_current": current_sum / count,
        "avg_pf": pf_sum / count,
        "sample_count": count,
        "aggregated_at": datetime.utcnow().isoformat()
    }

def persist_aggregation(conn, device_id, agg):
    """
    Insert aggregated data into PostgreSQL table.
    Assumes a table like:
    CREATE TABLE telemetry_aggregates (
        id SERIAL PRIMARY KEY,
        device_id TEXT NOT NULL,
        avg_voltage FLOAT,
        avg_current FLOAT,
        avg_pf FLOAT,
        sample_count INT,
        aggregated_at TIMESTAMP
    );
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO telemetry_aggregates
            (device_id, avg_voltage, avg_current, avg_pf, sample_count, aggregated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (device_id, agg["avg_voltage"], agg["avg_current"], agg["avg_pf"], agg["sample_count"], agg["aggregated_at"])
        )
    conn.commit()

def main():
    logging.info("Starting telemetry persist worker")
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    while True:
        start_time = datetime.utcnow() - timedelta(seconds=PERSIST_INTERVAL)
        device_ids = get_devices()
        logging.info(f"Found devices to aggregate: {device_ids}")

        for device_id in device_ids:
            entries = get_buffer(device_id, starts_at=start_time)
            if not entries:
                logging.info(f"No new data for device {device_id}, skipping.")
                continue

            agg = aggregate_telemetry(entries)
            if agg:
                persist_aggregation(pg_conn, device_id, agg)
                logging.info(f"Persisted aggregation for device {device_id}")

        logging.info(f"Sleeping for {PERSIST_INTERVAL} seconds...")
        time.sleep(PERSIST_INTERVAL)

if __name__ == "__main__":
    main()
