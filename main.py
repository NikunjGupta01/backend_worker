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

# ---------- Mongo init ----------
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

raw_collection = db["raw_data"]
analytics_collection = db["analytics_data"]
devices_master = db["devices_master"]

# ---------- Event loop ----------
loop = asyncio.new_event_loop()

def start_background_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_background_loop, daemon=True).start()

# ---------- Utilities ----------
IST_ZONE = ZoneInfo("Asia/Kolkata")

def now_utc():
    return datetime.now(timezone.utc)

def now_ist_iso():
    return now_utc().astimezone(IST_ZONE).isoformat()

def parse_payload_str(payload_str):
    try:
        return json.loads(payload_str)
    except Exception:
        return {"raw": payload_str}

def normalize_geoid(raw):
    # Accept Geoid, GeoId, geoId, "Null" and None
    for k in ("Geoid", "GeoId", "geoId", "geoID", "GEOID"):
        if k in raw:
            v = raw.get(k)
            if isinstance(v, str) and v.strip().lower() in ("null", ""):
                return None
            return v
    return None

def extract_interval(raw):
    # interval can be numeric string, int or in different keys
    for k in ("interval", "INT", "int", "Interval"):
        if k in raw:
            try:
                v = raw.get(k)
                if v is None:
                    return None
                # if like "300" -> int
                if isinstance(v, str) and v.isdigit():
                    return int(v)
                if isinstance(v, (int, float)):
                    return int(v)
                # sometimes strings with spaces
                vs = str(v).strip()
                if vs.isdigit():
                    return int(vs)
            except Exception:
                pass
    return None

def extract_timestamp(raw):
    # Prefer timestamp_iso -> timestamp -> ts -> TS
    # Return a normalized ISO string (IST) if possible, otherwise None
    try:
        # timestamp_iso may already be in ISO with timezone
        for k in ("timestamp_iso", "ts_iso"):
            if k in raw and raw.get(k):
                return _to_ist_iso(raw.get(k))
        # timestamp -> could be "2025-11-04T14:59:32" (no tz)
        for k in ("timestamp", "ts", "TS"):
            v = raw.get(k)
            if v:
                return _to_ist_iso(v)
    except Exception:
        pass
    return None

def _to_ist_iso(value):
    # Try various formats; aim to return ISO string with IST offset
    # If already datetime-like (pymongo stored), handle it.
    try:
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(IST_ZONE).isoformat()
        s = str(value)
        # if value contains 'Z', replace with +00:00 for fromisoformat
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        # If has timezone offset already, fromisoformat handles it
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            # treat as local naive -> assume UTC (as earlier pipeline did)
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST_ZONE).isoformat()
    except Exception:
        # last-ditch: try to parse as epoch milliseconds or seconds
        try:
            num = float(value)
            # if large, assume ms
            if num > 1e12:
                dt = datetime.fromtimestamp(num/1000, tz=timezone.utc)
            elif num > 1e9:
                dt = datetime.fromtimestamp(num, tz=timezone.utc)
            else:
                return None
            return dt.astimezone(IST_ZONE).isoformat()
        except Exception:
            return None

def extract_common_fields(topic, raw, received_at_ist):
    # Build the "complete" analytics payload from raw and metadata
    # Include as many fields as practical and normalize interval & geoid & timestamps.
    sj = {}

    sj["topic"] = topic
    sj["imei"] = raw.get("imei") or raw.get("IMEI") or None
    sj["packet"] = raw.get("packet") or raw.get("Packet") or None

    # GPS
    sj["latitude"] = raw.get("latitude") or raw.get("lat") or None
    sj["longitude"] = raw.get("longitude") or raw.get("lon") or raw.get("long") or None
    sj["speed"] = raw.get("speed") or None
    sj["temperature"] = raw.get("temperature") or raw.get("temp") or None

    # telemetry/battery/signal
    sj["Battery"] = raw.get("Battery") or raw.get("battery") or None
    sj["Signal"] = raw.get("Signal") or raw.get("signal") or None

    # interval and geoid
    sj["interval"] = extract_interval(raw)
    sj["Geoid"] = normalize_geoid(raw)

    # timestamps
    sj["timestamp_normalized"] = extract_timestamp(raw)  # IST ISO or None
    # Also keep original timestamp fields if present
    for k in ("timestamp", "ts", "TS", "timestamp_iso", "ts_iso"):
        if k in raw:
            sj[k] = raw.get(k)

    # copy other useful keys if present
    # (Alert, Alert code, VC, GPU etc)
    if "Alert" in raw:
        sj["Alert"] = raw.get("Alert")
    if "VC" in raw:
        sj["VC"] = raw.get("VC")
    if "GPU" in raw:
        sj["GPU"] = raw.get("GPU")
    if "raw" in raw and isinstance(raw.get("raw"), str):
        sj["raw_text"] = raw.get("raw")

    # metadata
    sj["type"] = _detect_type_from_raw(raw)
    sj["processed_at"] = now_utc().isoformat()
    # IMPORTANT: uniqueness key per your decision:
    # use received_at_ist from raw record when bootstrapping; for live messages we'll use current IST
    sj["received_at_ist"] = received_at_ist

    return sj

def _detect_type_from_raw(raw):
    if not isinstance(raw, dict):
        return "raw_string"
    if "packet" in raw:
        return f"packet_{raw.get('packet')}"
    if "VC" in raw and "GPU" in raw:
        return "device_info"
    if "raw" in raw and isinstance(raw.get("raw"), str):
        return "message"
    return "unknown"

# ---------- Device master ensure ----------
async def ensure_device_master_by_topic(topic, raw):
    # Find by topic (user requested topic-based)
    exists = await devices_master.find_one({"topic": topic})
    if exists:
        return

    # Try to populate IMEI/interval/Geoid if available
    imei = raw.get("imei") or raw.get("IMEI") or None
    interval = extract_interval(raw)
    geoid = normalize_geoid(raw)

    doc = {
        "topic": topic,
        "imei": imei,
        "interval": interval,
        "Geoid": geoid,
        "created_at": now_utc()
    }
    try:
        await devices_master.insert_one(doc)
        print(f"[DEVICE ADDED] topic={topic} imei={imei}")
    except Exception as e:
        print("devices_master insert error:", e)

# ---------- Bootstrap from existing raw_data (before MQTT) ----------
async def bootstrap_before_mqtt():
    print("Starting bootstrap from raw_data...")

    # ensure indexes for speed (optional)
    try:
        await devices_master.create_index("topic", unique=True)
        await analytics_collection.create_index([("topic", 1), ("received_at_ist", 1)], unique=False)
    except Exception:
        pass

    cursor = raw_collection.find({})
    count = 0
    async for row in cursor:
        try:
            topic = row.get("topic")
            raw = row.get("raw", {})
            # if raw is string, try parse json
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    raw = {"raw": raw}

            # Some records in raw_data might not have received_at_ist (but yours do)
            received_at_ist = row.get("received_at_ist")
            if received_at_ist is None:
                # if only received_at_utc present, convert it
                ru = row.get("received_at_utc")
                if isinstance(ru, datetime):
                    received_at_ist = ru.astimezone(IST_ZONE).isoformat()
                else:
                    received_at_ist = now_ist_iso()

            # Step 1: ensure device master entry by topic
            await ensure_device_master_by_topic(topic, raw)

            # Step 2: build analytics entry (complete)
            analytics_doc = extract_common_fields(topic, raw, received_at_ist)

            # Duplicate check using topic + received_at_ist
            exists = await analytics_collection.find_one({
                "topic": topic,
                "received_at_ist": analytics_doc["received_at_ist"]
            })
            if not exists:
                await analytics_collection.insert_one(analytics_doc)
                count += 1
                # print a small progress signal but not too verbose
            # else: already present - skip

        except Exception as e:
            print("bootstrap row error:", e)
            continue

    print(f"Bootstrap completed. Inserted {count} new analytics records.")


# ---------- Live MQTT processing ----------
async def process_and_store_live(topic, raw_payload):
    # raw_payload expected dict
    if not isinstance(raw_payload, dict):
        # try to parse or wrap
        raw_payload = {"raw": str(raw_payload)}

    # Ensure device master for new topics
    try:
        await ensure_device_master_by_topic(topic, raw_payload)
    except Exception as e:
        print("ensure device error:", e)

    # Insert into raw_data with timestamps
    nowu = now_utc()
    now_ist = nowu.astimezone(IST_ZONE).isoformat()
    try:
        await raw_collection.insert_one({
            "topic": topic,
            "raw": raw_payload,
            "received_at_utc": nowu,
            "received_at_ist": now_ist
        })
    except Exception as e:
        print("raw_collection insert error:", e)

    # Build analytics doc using current received_at_ist
    analytics_doc = extract_common_fields(topic, raw_payload, now_ist)

    # Avoid duplicates based on topic + received_at_ist
    try:
        exists = await analytics_collection.find_one({
            "topic": topic,
            "received_at_ist": analytics_doc["received_at_ist"]
        })
        if not exists:
            await analytics_collection.insert_one(analytics_doc)
            print(f"[LIVE ANALYTICS INSERTED] topic={topic} received_at_ist={analytics_doc['received_at_ist']}")
        else:
            print(f"[LIVE ANALYTICS SKIPPED duplicate] topic={topic} received_at_ist={analytics_doc['received_at_ist']}")
    except Exception as e:
        print("analytics_collection insert/check error:", e)

# ---------- MQTT callbacks ----------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
        for t in TOPICS:
            client.subscribe(t, qos=0)
            print(f"Subscribed {t}")
    else:
        print("MQTT connect failed rc=", rc)

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode(errors="ignore")
        raw_payload = parse_payload_str(payload_str)
        # schedule async processing in the background loop
        loop.call_soon_threadsafe(asyncio.create_task, process_and_store_live(msg.topic, raw_payload))
    except Exception as e:
        print("on_message error:", e)

# ---------- Main ----------
def main():
    # Run bootstrap in our background loop and wait for finish
    future = asyncio.run_coroutine_threadsafe(bootstrap_before_mqtt(), loop)
    try:
        future.result()  # block until bootstrap done
    except Exception as e:
        print("Bootstrap failed:", e)

    # Start MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.connect(MQTT_BROKER, MQTT_PORT, 10)
    print("MQTT loop starting...")
    client.loop_forever()

if __name__ == "__main__":
    main()
