import redis
import json
from datetime import datetime, timezone, timedelta
from app.config.buffer import MAX_BUFFER_SIZE, REDIS_HOST

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
    # Keep only latest entries
    r.ltrim(key, -MAX_BUFFER_SIZE, -1)
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
                entry_ts = datetime.fromtimestamp(entry['timestamp'], tz=timezone.utc)
                if entry_ts <= starts_at:
                    continue
            result.append(entry)
        except Exception as e:
            print(f"[get_buffer] Failed to parse entry: {e}")
            continue
    return result

def get_latest_buffer(device_id: str, seconds_prior: int):
    buffer = get_buffer(device_id)
    if not buffer:
        return []

    # Assume buffer is sorted ascending by timestamp; otherwise sort it
    buffer_sorted = sorted(buffer, key=lambda x: x['timestamp'])

    latest_entry = buffer_sorted[-1]
    latest_ts = latest_entry['timestamp']

    # Convert latest_ts to datetime if needed (handle int or iso string)
    if isinstance(latest_ts, int):
        latest_dt = datetime.fromtimestamp(latest_ts, tz=timezone.utc)
    else:
        latest_dt = datetime.fromisoformat(latest_ts)
        if latest_dt.tzinfo is None:
            latest_dt = latest_dt.replace(tzinfo=timezone.utc)

    cutoff_time = latest_dt - timedelta(seconds=seconds_prior)
    
    # Now fetch buffer starting from cutoff_time
    return get_buffer(device_id, starts_at=cutoff_time)

def get_buffer_slice(device_id: str, start_idx: int, length: int):
    """
    Retrieve a slice from the buffer starting at start_idx for length entries.
    Uses Redis lrange or similar with indices: lrange start_idx to start_idx+length-1.
    """
    key = _buffer_key(device_id)
    end_idx = start_idx + length - 1
    raw_entries = r.lrange(key, start_idx, end_idx)  # Redis lrange supports this indexing

    result = []
    for raw in raw_entries:
        try:
            entry = json.loads(raw)
            result.append(entry)
        except Exception as e:
            print(f"[get_buffer_slice] Failed to parse entry: {e}")
    return result


def clear_buffer(device_id: str):
    """
    Clear the buffer for the specific device_id.
    """
    key = _buffer_key(device_id)
    r.delete(key)
