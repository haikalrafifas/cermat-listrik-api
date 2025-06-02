import redis
import json
from datetime import datetime
from app.config.buffer import REDIS_HOST

r = redis.Redis(host=REDIS_HOST, port=6379, db=0)

def _buffer_key(device_id: str) -> str:
    return f"telemetry_buffer:{device_id}"

def add_to_buffer(device_id: str, entry: dict) -> int:
    """
    Add a telemetry entry to the buffer for the specific device_id.
    Returns the buffer count.
    """
    key = _buffer_key(device_id)
    r.rpush(key, json.dumps(entry))
    return r.llen(key)

def get_buffer(device_id: str, starts_at: datetime = None):
    """
    Retrieve all buffered entries for the given device_id.
    If starts_at is provided, only return entries with timestamp > starts_at.
    """
    key = _buffer_key(device_id)
    raw_entries = r.lrange(key, 0, -1)
    
    result = []
    for raw in raw_entries:
        try:
            entry = json.loads(raw)
            if starts_at:
                entry_ts = datetime.fromisoformat(entry['timestamp'])
                if entry_ts <= starts_at:
                    continue
            result.append(entry)
        except Exception as e:
            print(f"[get_buffer] Failed to parse entry: {e}")
            continue
    return result

def clear_buffer(device_id: str):
    """
    Clear the buffer for the specific device_id.
    """
    key = _buffer_key(device_id)
    r.delete(key)
