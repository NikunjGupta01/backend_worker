import json
import asyncio
import threading
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD,
    TOPICS, MONGO_URI, MONGO_DB
)

# ------------------ Mongo init ------------------

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

raw_collection = db["raw_data"]
analytics_collection = db["analytics_data"]
devices_master = db["devices_master"]

# ------------------ Event loop ------------------

IST_ZONE = ZoneInfo("Asia/Kolkata")
loop = asyncio.new_event_loop()

def start_background_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_background_loop, daemon=True).start()

# ------------------ Helper Functions ------------------

def now_utc():
    return datetime.now(timezone.utc)

def now_ist_iso():
    return now_utc().astimezone(IST_ZONE).isoformat()

def parse_payload(payload_str):
    try:
        return json.loads(payload_str)
    except:
        return {"raw": payload_str}

def extract_interval(raw):
    for key in ("interval", "Interval", "INT", "int"):
        if key in raw:
            try:
                val = str(raw[key]).strip()
                if val.lower() == "null":
                    return None
                if val.isdigit():
                    return int(val)
            except:
                pass
    return None

def extract_geoid(raw):
    for key in ("Geoid", "GeoId", "geoId", "GEOID"):
        if key in raw:
            val = raw[key]
            if val is None:
                return None
            sval = str(val).strip()
            if sval.lower() == "null" or sval == "":
                return None
            return sval
    return None

def extract_timestamp(raw):
    for key in ("timestamp_iso", "timestamp", "ts_iso", "ts", "TS"):
        if key in raw:
            return normalize_timestamp(raw[key])
    return None

def normalize_timestamp(value):
    try:
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(IST_ZONE).isoformat()

        s = str(value)
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")

        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST_ZONE).isoformat()
    except:
        try:
            num = float(value)
            if num > 1e12:
                dt = datetime.fromtimestamp(num/1000, tz=timezone.utc)
            else:
                dt = datetime.fromtimestamp(num, tz=timezone.utc)
            return dt.astimezone(IST_ZONE).isoformat()
        except:
            return None

def detect_type(raw):
    if not isinstance(raw, dict):
        return "raw_text"
    if "packet" in raw:
        return f"packet_{raw.get('packet')}"
    if "Alert" in raw:
        return "alert"
    if "VC" in raw and "GPU" in raw:
        return "device_info"
    return "unknown"

def build_analytics_record(topic, raw, received_at_ist):
    doc = {
        "topic": topic,
        "imei": raw.get("imei") or raw.get("IMEI"),
        "interval": extract_interval(raw),
        "Geoid": extract_geoid(raw),
        "packet": raw.get("packet"),
        "latitude": raw.get("latitude") or raw.get("lat"),
        "longitude": raw.get("longitude") or raw.get("lon") or raw.get("long"),
        "speed": raw.get("speed"),
        "Battery": raw.get("Battery"),
        "Signal": raw.get("Signal"),
        "Alert": raw.get("Alert"),
        "raw_text": raw["raw"] if isinstance(raw.get("raw"), str) else None,
        "timestamp_normalized": extract_timestamp(raw),
        "received_at_ist": received_at_ist,
        "processed_at": now_utc(),
        "type": detect_type(raw)
    }

    # Copy native raw timestamp fields if they exist
    for k in ("timestamp", "ts", "TS", "timestamp_iso", "ts_iso"):
        if k in raw:
            doc[k] = raw[k]

    return doc


# ------------------ Device Master Logic (Corrected) ------------------

async def ensure_device_master(topic, raw):
    existing = await devices_master.find_one({"topic": topic})
    if existing:
        return

    # First try from current record
    interval = extract_interval(raw)
    geoid = extract_geoid(raw)
    imei = raw.get("imei") or raw.get("IMEI")

    # If not found, scan all raw_data for this topic
    if interval is None or geoid is None or imei is None:
        cursor = raw_collection.find({"topic": topic})
        async for row in cursor:
            r = row.get("raw", {})
            if not isinstance(r, dict):
                continue

            if interval is None:
                iv = extract_interval(r)
                if iv is not None:
                    interval = iv

            if geoid is None:
                gd = extract_geoid(r)
                if gd is not None:
                    geoid = gd

            if imei is None:
                im = r.get("imei") or r.get("IMEI")
                if im:
                    imei = im

            if interval is not None and geoid is not None and imei is not None:
                break

    doc = {
        "topic": topic,
        "imei": imei,
        "interval": interval,
        "Geoid": geoid,
        "created_at": now_utc()
    }

    await devices_master.insert_one(doc)
    print(f"[DEVICE MASTER SAVED] {topic} interval={interval} geoid={geoid} imei={imei}")


# ------------------ Bootstrap Before MQTT ------------------

async def bootstrap_before_mqtt():
    print("Bootstrap started...")

    cursor = raw_collection.find({})
    count = 0

    async for row in cursor:
        topic = row["topic"]
        raw = row.get("raw", {})

        if not isinstance(raw, dict):
            try:
                raw = json.loads(raw)
            except:
                raw = {"raw": raw}

        # device master ensure
        await ensure_device_master(topic, raw)

        received_at_ist = row.get("received_at_ist")
        if received_at_ist is None:
            received_at_utc = row.get("received_at_utc")
            if isinstance(received_at_utc, datetime):
                received_at_ist = received_at_utc.astimezone(IST_ZONE).isoformat()
            else:
                received_at_ist = now_ist_iso()

        analytics_doc = build_analytics_record(topic, raw, received_at_ist)

        exists = await analytics_collection.find_one({
            "topic": topic,
            "received_at_ist": received_at_ist
        })

        if not exists:
            await analytics_collection.insert_one(analytics_doc)
            count += 1

    print(f"Bootstrap complete. Added {count} analytics records.\n")


# ------------------ Live MQTT Insert ------------------

async def handle_mqtt_message(topic, raw):
    await ensure_device_master(topic, raw)

    utc_now = now_utc()
    ist_now = utc_now.astimezone(IST_ZONE).isoformat()

    await raw_collection.insert_one({
        "topic": topic,
        "raw": raw,
        "received_at_utc": utc_now,
        "received_at_ist": ist_now
    })

    analytics_doc = build_analytics_record(topic, raw, ist_now)

    exists = await analytics_collection.find_one({
        "topic": topic,
        "received_at_ist": ist_now
    })

    if not exists:
        await analytics_collection.insert_one(analytics_doc)

    print(f"[LIVE] {topic} stored")


# ------------------ MQTT Callbacks ------------------

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected.")
        for t in TOPICS:
            client.subscribe(t)
            print(f"Subscribed {t}")
    else:
        print("MQTT connect failed:", rc)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="ignore")
        raw = parse_payload(payload)
        loop.call_soon_threadsafe(asyncio.create_task, handle_mqtt_message(msg.topic, raw))
    except Exception as e:
        print("MQTT message error:", e)


# ------------------ MAIN ------------------

def main():
    future = asyncio.run_coroutine_threadsafe(bootstrap_before_mqtt(), loop)
    future.result()

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.connect(MQTT_BROKER, MQTT_PORT, 10)
    client.loop_forever()


if __name__ == "__main__":
    main()
