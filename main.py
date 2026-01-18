"""
FINAL WORKER
Rule: Whatever goes to raw_data MUST go to analytics_data
"""

import json
import asyncio
import threading
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

from config import (
    MQTT_BROKER, MQTT_PORT,
    MQTT_USERNAME, MQTT_PASSWORD,
    TOPICS, MONGO_URI, MONGO_DB
)

# ------------------ Mongo Init ------------------

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

raw_collection = db["raw_data"]
analytics_collection = db["analytics_data"]
devices_master = db["devices_master"]

# ------------------ Async Loop ------------------

IST = ZoneInfo("Asia/Kolkata")
loop = asyncio.new_event_loop()


def start_background_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()


threading.Thread(target=start_background_loop, daemon=True).start()

# ------------------ Time Helpers ------------------


def current_ist_naive():
    aware = datetime.now(timezone.utc).astimezone(IST)
    return aware.replace(tzinfo=None)


def extract_device_raw_timestamp(raw: dict):
    for key in ("timestamp", "ts", "TS", "timestamp_iso", "ts_iso", "raw_timestamp"):
        v = raw.get(key)
        if v:
            s = str(v).strip()
            if s.lower() not in ("", "null"):
                return s
    return None


# ------------------ Normalizers ------------------


def safe_int(v):
    try:
        return int(str(v).strip())
    except:
        return None


def safe_float(v):
    try:
        return float(str(v).replace("km/hr", "").replace("km/h", "").strip())
    except:
        return None


# ------------------ Analytics Builder ------------------


def build_analytics_record(topic: str, raw: dict) -> dict:
    """
    ALWAYS produce an analytics document
    No conditional logic, no dropping
    """

    flat_raw = {}
    for k, v in raw.items():
        flat_raw[f"raw_{k}"] = (
            v if isinstance(v, (str, int, float)) or v is None else str(v)
        )

    doc = {
        "_id": ObjectId(),

        # routing
        "topic": topic,
        "imei": raw.get("imei") or raw.get("IMEI") or raw.get("raw_imei"),

        # common known fields (maybe None)
        "packet": raw.get("packet"),
        "Alert": raw.get("Alert"),

        "interval": safe_int(raw.get("interval") or raw.get("Interval")),
        "Geoid": raw.get("Geoid") or raw.get("geoId"),

        "latitude": raw.get("latitude") or raw.get("lat"),
        "longitude": raw.get("longitude") or raw.get("lon") or raw.get("long"),

        "speed": safe_float(raw.get("speed")),
        "Battery": raw.get("Battery"),
        "Signal": raw.get("Signal"),

        # timestamps
        "device_raw_timestamp": extract_device_raw_timestamp(raw),
        "device_timestamp": current_ist_naive(),

        # classification (never blocks insert)
        "type": (
            f"packet_{raw.get('packet')}"
            if raw.get("packet")
            else "config_or_misc"
        ),
    }

    doc.update(flat_raw)
    return doc


# ------------------ Device Master ------------------


async def ensure_device_master(topic: str, raw: dict):
    exists = await devices_master.find_one({"topic": topic})
    if exists:
        return

    await devices_master.insert_one({
        "_id": ObjectId(),
        "topic": topic,
        "imei": raw.get("imei") or raw.get("IMEI"),
        "created_at": current_ist_naive()
    })


# ------------------ MQTT Handler ------------------


async def handle_mqtt_message(topic: str, raw: dict):

    await ensure_device_master(topic, raw)

    # RAW = EXACT PAYLOAD
    await raw_collection.insert_one({
        "_id": ObjectId(),
        "topic": topic,
        **{f"raw_{k}": v for k, v in raw.items()}
    })

    # ANALYTICS = NORMALIZED + RAW MIRROR
    analytics_doc = build_analytics_record(topic, raw)
    await analytics_collection.insert_one(analytics_doc)

    print(f"[STORED] {topic} type={analytics_doc['type']}")


# ------------------ MQTT ------------------


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        for t in TOPICS:
            client.subscribe(t)
            print("Subscribed:", t)
    else:
        print("MQTT connect failed:", rc)


def on_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8", errors="ignore"))
    except:
        raw = {"raw_body": msg.payload.decode("utf-8", errors="ignore")}

    loop.call_soon_threadsafe(
        asyncio.create_task,
        handle_mqtt_message(msg.topic, raw)
    )


# ------------------ MAIN ------------------


def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()
