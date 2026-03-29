"""
FINAL WORKER – STRICT & CORRECT

Rules:
1. raw_data ALWAYS stores incoming payload
2. analytics_data ALWAYS stores interpreted record
3. device_master:
   - Created ONLY on first NORMAL packet
   - Stores Geoid & interval from first NORMAL packet
   - Updates Geoid / interval ONLY if changed
   - Ignores config / text messages completely
"""

import json
import asyncio
import threading
from bson import ObjectId
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

from config import (MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD,  MONGO_URI, MONGO_DB)

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

subscribed_topics = set()
threading.Thread(target=start_background_loop, daemon=True).start()

# ------------------ Helpers ------------------


def current_ist_naive():
    aware = datetime.now(timezone.utc).astimezone(IST)
    return aware.replace(tzinfo=None)


def is_normal_packet(raw: dict) -> bool:
    return bool(raw.get("packet"))


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

def extract_imei_from_topic(topic: str):
    if not topic:
        return None
    return topic.split("/")[0]

def extract_geoid(raw: dict):
    v = raw.get("Geoid") or raw.get("geoId")
    if v in (None, "", "Null", "null"):
        return None
    return v


def extract_interval(raw: dict):
    v = raw.get("interval") or raw.get("Interval")
    return safe_int(v)


def extract_device_raw_timestamp(raw: dict):
    for key in ("timestamp", "ts", "TS", "timestamp_iso", "ts_iso", "raw_timestamp"):
        v = raw.get(key)
        if v:
            s = str(v).strip()
            if s.lower() not in ("", "null"):
                return s
    return None

async def fetch_device_master_topics():
    """Read active MQTT topics from Mongo devices_master collection."""
    topics = []
    cursor = devices_master.find({}, {"topic": 1})  # only fetch topic field

    async for doc in cursor:
        topic = str(doc.get("topic", "")).strip()
        if topic:
            topics.append(topic)

    return topics

def sync_mqtt_subscriptions(client):
    future = asyncio.run_coroutine_threadsafe(fetch_device_master_topics(), loop)
    topics = future.result(timeout=10)

    for topic in topics:
        if topic not in subscribed_topics:
            client.subscribe(topic)
            subscribed_topics.add(topic)
            print("Subscribed (device_master):", topic)

            

# ------------------ Analytics Builder ------------------


def build_analytics_record(topic: str, raw: dict) -> dict:
    flat_raw = {
        f"raw_{k}": v if isinstance(v, (str, int, float)) or v is None else str(v)
        for k, v in raw.items()
    }
    print("flat_raw: ", flat_raw)
    event_kind = "telemetry" if is_normal_packet(raw) else "config_event"
    topic_imei = extract_imei_from_topic(topic)

    doc = {
        "_id": ObjectId(),
        "topic": topic,
        "event_kind": event_kind,

        "imei": (raw.get("imei") or raw.get("IMEI") or topic_imei),
        "packet": raw.get("packet"),
        "Alert": raw.get("Alert"),

        "interval": extract_interval(raw),
        "Geoid": extract_geoid(raw),

        "latitude": raw.get("latitude") or raw.get("lat"),
        "longitude": raw.get("longitude") or raw.get("lon") or raw.get("long"),
        "speed": safe_float(raw.get("speed")),

        "Battery": raw.get("Battery"),
        "Signal": raw.get("Signal"),

        "device_raw_timestamp": extract_device_raw_timestamp(raw),
        "device_timestamp": current_ist_naive(),

        "type": (
            f"packet_{raw.get('packet')}"
            if event_kind == "telemetry"
            else "config_or_misc"
        ),
    }

    doc.update(flat_raw)
    return doc

# ------------------ Device Master Sync ------------------


async def sync_device_master(topic: str, raw: dict):
    # Ignore non-normal packets
    if not is_normal_packet(raw):
        return

    imei = (raw.get("imei") or raw.get("IMEI") or extract_imei_from_topic(topic))
    geoid = extract_geoid(raw)
    interval = extract_interval(raw)

    device = await devices_master.find_one({"topic": topic})

    # ---------- First NORMAL packet ----------
    if not device:
        await devices_master.insert_one({
            "_id": ObjectId(),
            "topic": topic,
            "imei": imei,
            "Geoid": geoid,
            "interval": interval,
            "created_at": current_ist_naive(),
            "updated_at": current_ist_naive()
        })
        return

    # ---------- Subsequent NORMAL packets ----------
    updates = {}

    if geoid is not None and geoid != device.get("Geoid"):
        updates["Geoid"] = geoid

    if interval is not None and interval != device.get("interval"):
        updates["interval"] = interval

    if updates:
        updates["updated_at"] = current_ist_naive()
        await devices_master.update_one(
            {"_id": device["_id"]},
            {"$set": updates}
        )

# ------------------ MQTT Handler ------------------


async def handle_mqtt_message(topic: str, raw: dict):

    # 1. RAW DATA (exact payload)
    await raw_collection.insert_one({
        "_id": ObjectId(),
        "topic": topic,
        **{f"raw_{k}": v for k, v in raw.items()}
    })

    # 2. DEVICE MASTER (only normal packets)
    await sync_device_master(topic, raw)

    # 3. ANALYTICS
    analytics_doc = build_analytics_record(topic, raw)
    await analytics_collection.insert_one(analytics_doc)

    print(f"[STORED] {topic} event={analytics_doc['event_kind']} type={analytics_doc['type']}")

# ------------------ MQTT ------------------


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        sync_mqtt_subscriptions(client=client)
    
    else:
        print("MQTT connect failed:", rc)


def on_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8", errors="ignore"))
    except:
        raw = {"raw_body": msg.payload.decode("utf-8", errors="ignore")}
    print(f"topic: {msg.topic} , raw: {msg.payload}")
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
