import paho.mqtt.client as mqtt
import json
from app.storage.telemetry_buffer import add_to_buffer, get_buffer_slice
from app.modules.anomaly_detection import detect_anomalies
from app.config.mqtt import (
    MQTT_BROKER, POWER_TELEMETRY_SUBTOPIC, ANOMALY_SUBTOPIC,
    MQTT_BASE_TOPIC, DEVICE_ID
)
from app.config.anomaly_detection import WINDOW_SIZE, STEP_SIZE
from datetime import datetime, timezone
import traceback

MQTT_SUB_TOPIC = f"{MQTT_BASE_TOPIC}/{DEVICE_ID}/{POWER_TELEMETRY_SUBTOPIC}"
MQTT_PUB_TOPIC = f"{MQTT_BASE_TOPIC}/{DEVICE_ID}/{ANOMALY_SUBTOPIC}"

last_processed_index = None

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker", flush=True)
    client.subscribe(MQTT_SUB_TOPIC)

def on_message(client, userdata, msg):
    global last_processed_index
    try:
        data = json.loads(msg.payload.decode())
        buffer_count = add_to_buffer(DEVICE_ID, data)

        if buffer_count >= WINDOW_SIZE:
            if last_processed_index is None:
                # First time: start from the end of buffer minus WINDOW_SIZE (or 0 if smaller)
                start_idx = max(0, buffer_count - WINDOW_SIZE)
                # Fetch last WINDOW_SIZE entries (if get_buffer_slice supports it)
                buffer = get_buffer_slice(DEVICE_ID, start_idx, WINDOW_SIZE)

                result = detect_anomalies(DEVICE_ID, buffer)
                if result:
                    send_anomalies_to_clients(client, result)
                
                # Set last_processed_index to the index of last processed entry
                last_processed_index = start_idx + WINDOW_SIZE - STEP_SIZE  # leave STEP_SIZE for next detection
            else:
                next_start = last_processed_index + STEP_SIZE

                # Fetch STEP_SIZE entries from buffer starting at next_start
                buffer = get_buffer_slice(DEVICE_ID, next_start, STEP_SIZE)
                if len(buffer) < STEP_SIZE:
                    return

                # Run detection only on this STEP_SIZE chunk
                result = detect_anomalies(DEVICE_ID, buffer)
                if result:
                    send_anomalies_to_clients(client, result)

                # Update last_processed_index
                last_processed_index = next_start

    except Exception as e:
        traceback.print_exc()
        print(f"Error: {e}")

def send_anomalies_to_clients(client, result):
    client.publish(MQTT_PUB_TOPIC, json.dumps(result))

if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_forever()
