import json
import asyncio
import threading
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

from config import (MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD, TOPICS, MONGO_URI, MONGO_DB)

# ==== MONGO INIT ====
mongo_client = AsyncIOMotorClient(
    MONGO_URI,
    serverSelectionTimeoutMS=30000,
    socketTimeoutMS=30000,
    connectTimeoutMS=30000
)

db = mongo_client[MONGO_DB]
raw_collection = db["raw_data"]
analytics_collection = db["analytics_data"]

# ==== EVENT LOOP ====
loop = asyncio.new_event_loop()

def start_background_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_background_loop, daemon=True).start()

# ==== PAYLOAD PARSER ====
def parse_payload(payload_str):
    try:
        return json.loads(payload_str)
    except json.JSONDecodeError:
        return {"raw": payload_str}

# ==== TYPE DETECTOR ====
def detect_type(payload):
    if not isinstance(payload, dict):
        return "unknown"
    if "MB" in payload and "PID" in payload:
        return "sensor_array"
    if "status" in payload and "rpm" in payload:
        return "status_report"
    if "deviceId" in payload and "packets publish:" in payload:
        return "health_report"
    if "packet" in payload and "imei" in payload:
        return "kell_packet"
    if "MA" in payload and "TS" in payload:
        return "beep3_packet"
    if "raw" in payload:
        return "raw_payload"
    return "unknown"

# ==== TIMESTAMP NORMALIZER ====
def normalize_timestamp(payload):
    try:
        if "ts" in payload:
            ts_ms = int(payload["ts"])
            dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            payload["ts_iso"] = dt_utc.astimezone(ZoneInfo("Asia/Kolkata")).isoformat()

        if isinstance(payload.get("timestamp"), str):
            try:
                dt = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                payload["timestamp_iso"] = dt.astimezone(ZoneInfo("Asia/Kolkata")).isoformat()
            except:
                pass

        if isinstance(payload.get("TS"), str):
            try:
                dt = datetime.fromisoformat(payload["TS"].replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                payload["TS_iso"] = dt.astimezone(ZoneInfo("Asia/Kolkata")).isoformat()
            except:
                pass

    except:
        pass

    return payload

# ==== DB INSERT ====
async def insert_data(topic, payload):
    try:
        now_utc = datetime.now(timezone.utc)
        now_ist = now_utc.astimezone(ZoneInfo("Asia/Kolkata"))

        normalized = normalize_timestamp(payload if isinstance(payload, dict) else {"raw": payload})

        raw_doc = {
            "topic": topic,
            "raw": payload,
            "received_at_utc": now_utc,
            "received_at_ist": now_ist.isoformat()
        }
        await raw_collection.insert_one(raw_doc)

        typed_doc = {
            "topic": topic,
            "type": detect_type(normalized),
            "payload": normalized,
            "timestamp_utc": now_utc
        }
        # Store analytics if needed
        # await analytics_collection.insert_one(typed_doc)

        print(f"Stored → {topic} | {now_utc.isoformat()} | {now_ist.isoformat()}")

    except Exception as e:
        print(f"Mongo insert error: {e}")

# ==== MQTT CALLBACKS ====
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected → {MQTT_BROKER}:{MQTT_PORT}")
        for t in TOPICS:
            client.subscribe(t, qos=0)
            print(f"Subscribed: {t}")
    else:
        print(f"Connect failed: {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode()
        print(f"[{msg.topic}] {payload_str}")

        parsed = parse_payload(payload_str)
        loop.call_soon_threadsafe(asyncio.create_task, insert_data(msg.topic, parsed))

    except Exception as e:
        print(f"Message error: {e}")

# ==== MAIN ====
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.connect(MQTT_BROKER, MQTT_PORT, 10)
    print("MQTT loop started...")
    client.loop_forever()

if __name__ == "__main__":
    main()
