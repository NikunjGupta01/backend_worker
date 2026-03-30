import asyncio
import threading

import paho.mqtt.client as mqtt

from config import MQTT_BROKER, MQTT_PASSWORD, MQTT_PORT, MQTT_USERNAME
from services.ingestion import handle_mqtt_message
from services.subscription_sync import sync_mqtt_subscriptions


mqtt_client = mqtt.Client()

_app_loop = None
_mqtt_started = False


def set_app_loop(loop):
    global _app_loop
    _app_loop = loop


def get_mqtt_client():
    return mqtt_client


def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        if _app_loop:
            asyncio.run_coroutine_threadsafe(sync_mqtt_subscriptions(mqtt_client), _app_loop)
        else:
            print("FastAPI loop not ready; skipping initial sync")
    else:
        print("MQTT connect failed:", rc)


def _on_message(client, userdata, msg):
    if _app_loop:
        asyncio.run_coroutine_threadsafe(
            handle_mqtt_message(msg.topic, msg.payload),
            _app_loop,
        )
    else:
        print("FastAPI loop not ready; dropped incoming MQTT message")


def _start_mqtt_service():
    mqtt_client.on_connect = _on_connect
    mqtt_client.on_message = _on_message

    if MQTT_USERNAME:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_forever()


def ensure_mqtt_started():
    global _mqtt_started
    if _mqtt_started:
        return

    threading.Thread(target=_start_mqtt_service, daemon=True).start()
    _mqtt_started = True

