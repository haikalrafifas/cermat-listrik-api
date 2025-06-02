import paho.mqtt.client as mqtt
import json
from app.storage.telemetry_buffer import add_to_buffer, get_buffer, clear_buffer
from app.modules.anomaly_detection import detect_anomalies
from app.config.mqtt import (
    MQTT_BROKER, POWER_TELEMETRY_SUBTOPIC, ANOMALY_SUBTOPIC,
    MQTT_BASE_TOPIC, DEVICE_ID
)
from app.config.anomaly_detection import STEP_SIZE

MQTT_SUB_TOPIC = f"{MQTT_BASE_TOPIC}/{DEVICE_ID}/{POWER_TELEMETRY_SUBTOPIC}"
MQTT_PUB_TOPIC = f"{MQTT_BASE_TOPIC}/{DEVICE_ID}/{ANOMALY_SUBTOPIC}"

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    client.subscribe(MQTT_SUB_TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        buffer_count = add_to_buffer(DEVICE_ID, data)
        if buffer_count > STEP_SIZE:
            buffer = get_buffer(DEVICE_ID)
            result = detect_anomalies(DEVICE_ID, buffer)
            if result:
                send_anomalies_to_clients(client, result)
            clear_buffer()
    except Exception as e:
        print(f"Error: {e}")

def send_anomalies_to_clients(client, result):
    client.publish(MQTT_PUB_TOPIC, json.dumps(result))

if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_forever()
