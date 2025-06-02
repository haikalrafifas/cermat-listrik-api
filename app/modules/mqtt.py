import paho.mqtt.client as mqtt
import json
from storage.telemetry_buffer import add_to_buffer

# MQTT Configuration
MQTT_BROKER = "broker.emqx.io"
MQTT_TOPIC = "cermatlistrik/anonymous-smartlamp-001/telemetry/power-consumption"

# Callback when connected to MQTT Broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(MQTT_TOPIC)

# Callback when a message is received
def on_message(client, userdata, msg):
    # Parse the incoming message
    try:
        payload = json.loads(msg.payload.decode())
        # Add the data to the buffer
        add_to_buffer(payload)
    except Exception as e:
        print(f"Error processing message: {e}")

# MQTT client setup
def start_mqtt_client():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_start()
