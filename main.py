import json
import asyncio
import threading
import re
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD,
    TOPICS, MONGO_URI, MONGO_DB
)

# ------------------ Mongo Init ------------------

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

raw_collection = db["raw_data"]
analytics_collection = db["analytics_data"]
devices_master = db["devices_master"]

# ------------------ Constants ------------------

IST = ZoneInfo("Asia/Kolkata")
loop = asyncio.new_event_loop()


def start_background_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()


threading.Thread(target=start_background_loop, daemon=True).start()

# ------------------ Time Helpers ------------------


def current_ist_naive():
    """
    Convert current time to IST, but store WITHOUT tzinfo.
    Odmantic/Pydantic require naive datetime objects.
    """
    aware = datetime.now(timezone.utc).astimezone(IST)
    return aware.replace(tzinfo=None)


def parse_device_raw_timestamp(raw_ts: str | None):
    """
    Device timestamp stored AS-IS (string only).
    No conversion. No validation. No timezone adjustments.
    """
    if not raw_ts:
        return None
    s = str(raw_ts).strip()
    return s if s.lower() not in ("", "null") else None


def extract_raw_timestamp(raw):
    """Extract raw timestamp key from payload."""
    for key in ("timestamp", "ts", "TS", "timestamp_iso", "ts_iso"):
        if key in raw:
            return parse_device_raw_timestamp(raw[key])
    return None


# ------------------ Extract Helpers ------------------


def extract_interval(raw):
    for key in ("interval", "Interval", "INT", "int"):
        if key in raw:
            try:
                v = str(raw[key]).strip()
                return int(v) if v.isdigit() else None
            except:
                return None
    return None


def extract_geoid(raw):
    for key in ("Geoid", "GeoId", "geoId", "GEOID"):
        if key in raw:
            v = str(raw[key]).strip()
            return v if v.lower() not in ("null", "") else None
    return None


def extract_speed(raw):
    v = raw.get("speed") or raw.get("Speed")
    if v is None:
        return None
    try:
        s = str(v).lower()
        for junk in ("km/hr", "km/h", "kmph", "km"):
            s = s.replace(junk, "")
        s = s.strip()
        return float(s) if "." in s else int(s)
    except:
        return None


# ------------------ Type Detection ------------------


def detect_type(raw):
    if "packet" in raw:
        return f"packet_{raw.get('packet')}"
    if "Alert" in raw:
        return "alert"
    return "unknown"


# ------------------ ANALYTICS RECORD ------------------


def build_analytics_record(topic, raw):

    # 1. Save EXACT raw timestamp (string)
    device_raw_ts = extract_raw_timestamp(raw)

    # 2. Save IST timestamp for sorting (naive datetime)
    device_ts = current_ist_naive()

    # 3. Flatten raw payload (BUT skip timestamp keys to avoid duplicates)
    flat_raw = {}
    for k, v in raw.items():
        if k.lower() in ("timestamp", "ts", "ts_iso", "timestamp_iso"):
            continue
        flat_raw[f"raw_{k}"] = (
            v if isinstance(v, (str, int, float)) or v is None else str(v)
        )

    doc = {
        "_id": ObjectId(),

        "topic": topic,
        "imei": raw.get("imei") or raw.get("IMEI"),

        "interval": extract_interval(raw),
        "Geoid": extract_geoid(raw),
        "packet": raw.get("packet"),
        "Alert": raw.get("Alert"),

        "speed": extract_speed(raw),
        "latitude": raw.get("latitude") or raw.get("lat"),
        "longitude": raw.get("longitude") or raw.get("lon") or raw.get("long"),

        "Battery": raw.get("Battery"),
        "Signal": raw.get("Signal"),

        # FINAL CLEAN TIMESTAMP SCHEMA
        "device_raw_timestamp": device_raw_ts,     # STRING from device
        "device_timestamp": device_ts,             # NAIVE IST datetime (NO tzinfo!)

        "type": detect_type(raw),
    }

    doc.update(flat_raw)
    return doc


# ------------------ DEVICE MASTER ------------------


async def ensure_device_master(topic, raw):
    exists = await devices_master.find_one({"topic": topic})
    if exists:
        return

    doc = {
        "_id": ObjectId(),
        "topic": topic,
        "imei": raw.get("imei") or raw.get("IMEI"),
        "interval": extract_interval(raw),
        "Geoid": extract_geoid(raw),
        "created_at": current_ist_naive(),
    }

    await devices_master.insert_one(doc)
    print(f"[MASTER CREATED] {topic}")


# ------------------ HANDLE MQTT MESSAGE ------------------


async def handle_mqtt_message(topic, raw):

    await ensure_device_master(topic, raw)

    # Store incoming raw message
    await raw_collection.insert_one({
        "_id": ObjectId(),
        "topic": topic,
        **{f"raw_{k}": v for k, v in raw.items()}
    })

    # Store analytics processed doc
    doc = build_analytics_record(topic, raw)
    await analytics_collection.insert_one(doc)

    print(f"[LIVE STORED] {topic}")


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
        asyncio.create_task, handle_mqtt_message(msg.topic, raw)
    )


# ------------------ MAIN ------------------


def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.connect(MQTT_BROKER, MQTT_PORT, 10)
    client.loop_forever()


if __name__ == "__main__":
    main()
