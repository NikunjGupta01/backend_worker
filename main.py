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
from fastapi import FastAPI
import asyncio
import threading
from bson import ObjectId
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from helper.http_responses import HttpResponses

from config import (
    MQTT_BROKER,
    MQTT_PORT,
    MQTT_USERNAME,
    MQTT_PASSWORD,
    MONGO_URI,
    MONGO_DB,
    APP_NAME,
    APP_VERSION,
)

# ------------------ Mongo Init ------------------

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

raw_collection = db["raw_data"]
analytics_collection = db["analytics_data"]
devices_master = db["devices_master"]

# ------------------ Async Loop ------------------
global mqtt_client
mqtt_client = mqtt.Client()
mqtt_started = False

# Fast APi
app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/resync-topic")
async def resync_topic(topic: str):
    try:
        record = await devices_master.find_one({"topic": topic})
        if record is None:
            return HttpResponses.error(message="Device Does Not Exist", code=404)

        if not mqtt_client.is_connected():
            return HttpResponses.error(message="MQTT client not connected", code=503)

        if bool(record.get("is_active", True)) is True:
            result, _mid = mqtt_client.subscribe(topic, qos=0)
            if result == mqtt.MQTT_ERR_SUCCESS:
                await devices_master.update_one(
                    {"topic": topic},
                    {
                        "$set": {
                            "is_subscribed": True,
                            "updated_at": current_ist_naive(),
                        }
                    },
                )
                return HttpResponses.success(message="Topic Subscribed Successfully")
            else:
                return HttpResponses.error(
                    message=f"Subscribe failed for topic={topic}, code={result}",
                    code=500,
                )
        else:
            result, _mid = mqtt_client.unsubscribe(topic)
            if result == mqtt.MQTT_ERR_SUCCESS:
                await devices_master.update_one(
                    {"topic": topic},
                    {
                        "$set": {
                            "is_subscribed": False,
                            "updated_at": current_ist_naive(),
                        }
                    },
                )
                return HttpResponses.success(message="Topic Unsubscribed Successfully")
            return HttpResponses.error(
                message=f"Unsubscribe failed for topic={topic}, code={result}",
                code=500,
            )
    except Exception as e:
        return HttpResponses.error(message=f"Unexpected error: {str(e)}", code=500)


IST = ZoneInfo("Asia/Kolkata")

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


async def sync_mqtt_subscriptions():
    """
    DB is the source of truth:
      - subscribe docs where is_active=True and is_subscribed!=True
      - unsubscribe docs where is_active=False and is_subscribed=True
    """
    # Subscribe missing active topics
    to_subscribe = devices_master.find(
        {"is_active": True, "is_subscribed": {"$ne": True}},
        {"topic": 1},
    )

    async for doc in to_subscribe:
        topic = str(doc.get("topic", "")).strip()
        if not topic:
            continue

        result, _mid = mqtt_client.subscribe(topic, qos=0)
        if result == mqtt.MQTT_ERR_SUCCESS:
            await devices_master.update_one(
                {"_id": doc["_id"]},
                {"$set": {"is_subscribed": True, "updated_at": current_ist_naive()}},
            )
            print("Subscribed (device_master):", topic)
        else:
            print(f"Subscribe failed for topic={topic}, code={result}")

    # Unsubscribe inactive topics that are still marked subscribed
    to_unsubscribe = devices_master.find(
        {"is_active": False, "is_subscribed": True},
        {"topic": 1},
    )

    async for doc in to_unsubscribe:
        topic = str(doc.get("topic", "")).strip()
        if not topic:
            continue

        result, _mid = mqtt_client.unsubscribe(topic)
        if result == mqtt.MQTT_ERR_SUCCESS:
            await devices_master.update_one(
                {"_id": doc["_id"]},
                {"$set": {"is_subscribed": False, "updated_at": current_ist_naive()}},
            )
            print("Unsubscribed (device_master):", topic)
        else:
            print(f"Unsubscribe failed for topic={topic}, code={result}")


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

    imei = raw.get("imei") or raw.get("IMEI") or extract_imei_from_topic(topic)
    geoid = extract_geoid(raw)
    interval = extract_interval(raw)

    device = await devices_master.find_one({"topic": topic})

    # ---------- First NORMAL packet ----------
    if not device:
        await devices_master.insert_one(
            {
                "_id": ObjectId(),
                "topic": topic,
                "imei": imei,
                "Geoid": geoid,
                "interval": interval,
                "created_at": current_ist_naive(),
                "updated_at": current_ist_naive(),
                "is_active": True,
                "is_subscribed": False,
            }
        )
        return

    # ---------- Subsequent NORMAL packets ----------
    updates = {}

    if geoid is not None and geoid != device.get("Geoid"):
        updates["Geoid"] = geoid

    if interval is not None and interval != device.get("interval"):
        updates["interval"] = interval

    if updates:
        updates["updated_at"] = current_ist_naive()
        await devices_master.update_one({"_id": device["_id"]}, {"$set": updates})


# ------------------ MQTT Handler ------------------


async def handle_mqtt_message(topic: str, raw: dict):

    # 1. RAW DATA (exact payload)
    await raw_collection.insert_one(
        {"_id": ObjectId(), "topic": topic, **{f"raw_{k}": v for k, v in raw.items()}}
    )

    # 2. DEVICE MASTER (only normal packets)
    await sync_device_master(topic, raw)

    # 3. ANALYTICS
    analytics_doc = build_analytics_record(topic, raw)
    await analytics_collection.insert_one(analytics_doc)

    print(
        f"[STORED] {topic} event={analytics_doc['event_kind']} type={analytics_doc['type']}"
    )


# ------------------ MQTT ------------------


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        app_loop = getattr(app.state, "loop", None)
        if app_loop:
            asyncio.run_coroutine_threadsafe(sync_mqtt_subscriptions(), app_loop)
        else:
            print("FastAPI loop not ready; skipping initial sync")

    else:
        print("MQTT connect failed:", rc)


def on_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8", errors="ignore"))
    except:
        raw = {"raw_body": msg.payload.decode("utf-8", errors="ignore")}
    print(f"topic: {msg.topic} , raw: {msg.payload}")
    app_loop = getattr(app.state, "loop", None)
    if app_loop:
        asyncio.run_coroutine_threadsafe(handle_mqtt_message(msg.topic, raw), app_loop)
    else:
        print("FastAPI loop not ready; dropped incoming MQTT message")


def start_mqtt_service():
    """Connect MQTT client and start network loop."""
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    if MQTT_USERNAME:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_forever()


@app.on_event("startup")
async def startup_event():
    """Start MQTT service once when FastAPI boots."""
    global mqtt_started
    app.state.loop = asyncio.get_running_loop()
    if mqtt_started:
        return

    threading.Thread(target=start_mqtt_service, daemon=True).start()
    mqtt_started = True


# ------------------ MAIN ------------------


def main():
    # Local direct run fallback. Docker/compose uses uvicorn command.
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=9001, reload=True)


if __name__ == "__main__":
    main()
