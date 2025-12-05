import json
import asyncio
import threading
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId  # <-- IMPORTANT

from config import (MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD, TOPICS, MONGO_URI, MONGO_DB)

# ------------------ Mongo init ------------------

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

# ------------------ Time Functions ------------------

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def now_ist_iso():
    return datetime.now(timezone.utc).astimezone(IST).isoformat()

# ------------------ Payload Parsing ------------------

def parse_payload(payload_str):
    try:
        return json.loads(payload_str)
    except:
        return {"raw_body": payload_str}

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
            val = str(raw[key]).strip()
            return val if val.lower() not in ("null", "") else None
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

# ------------------ Timestamp Handling ------------------

def normalize_timestamp(value):
    if value is None:
        return None
    try:
        s = str(value).strip()
        if s.lower() in ("null", ""):
            return None

        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")

        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(IST).isoformat()

    except:
        try:
            num = float(value)
            dt = datetime.fromtimestamp(num / 1000 if num > 1e12 else num, tz=timezone.utc)
            return dt.astimezone(IST).isoformat()
        except:
            return None


def extract_timestamp(raw):
    for key in ("timestamp", "timestamp_iso", "ts_iso", "ts", "TS"):
        if key in raw:
            iso = normalize_timestamp(raw[key])
            if iso:
                return iso
    return None

# ------------------ Type ------------------

def detect_type(raw):
    if "packet" in raw:
        return f"packet_{raw.get('packet')}"
    if "Alert" in raw:
        return "alert"
    return "unknown"

# ------------------ ANALYTICS RECORD ------------------

def build_analytics_record(topic, raw):

    device_ts = extract_timestamp(raw) or now_ist_iso()

    flat_raw = {}
    for k, v in raw.items():
        flat_raw[f"raw_{k}"] = v if isinstance(v, (str, int, float)) or v is None else str(v)

    doc = {
        "_id": ObjectId(),   # <-- ObjectId
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

        "device_timestamp": device_ts,
        "received_at_utc": now_utc(),

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
        "_id": ObjectId(),   # <-- ObjectId
        "topic": topic,
        "imei": raw.get("imei") or raw.get("IMEI"),
        "interval": extract_interval(raw),
        "Geoid": extract_geoid(raw),
        "created_at": now_utc(),
    }

    await devices_master.insert_one(doc)
    print(f"[MASTER CREATED] {topic}")

# ------------------ HANDLE MQTT MESSAGE ------------------

async def handle_mqtt_message(topic, raw):

    await ensure_device_master(topic, raw)

    # RAW data — let MongoDB generate ObjectId automatically
    await raw_collection.insert_one({
        "topic": topic,
        "received_at_utc": now_utc(),
        **{f"raw_{k}": v for k, v in raw.items()}
    })

    # ANALYTICS
    doc = build_analytics_record(topic, raw)
    await analytics_collection.insert_one(doc)

    print(f"[LIVE STORED] {topic}")

# ------------------ MQTT ------------------

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        for t in TOPICS:
            client.subscribe(t)
            print("Subscribed", t)
    else:
        print("MQTT connect failed:", rc)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="ignore")
        raw = parse_payload(payload)
        loop.call_soon_threadsafe(asyncio.create_task, handle_mqtt_message(msg.topic, raw))
    except Exception as e:
        print("MQTT error:", e)

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
