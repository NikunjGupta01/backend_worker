import json
from bson import ObjectId

from db import analytics_collection, devices_master, raw_collection
from services.time_utils import current_ist_naive


def is_normal_packet(raw: dict) -> bool:
    return bool(raw.get("packet"))


def safe_int(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None


def safe_float(v):
    try:
        return float(str(v).replace("km/hr", "").replace("km/h", "").strip())
    except Exception:
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


def build_analytics_record(topic: str, raw: dict) -> dict:
    flat_raw = {
        f"raw_{k}": v if isinstance(v, (str, int, float)) or v is None else str(v)
        for k, v in raw.items()
    }
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


async def sync_device_master(topic: str, raw: dict):
    if not is_normal_packet(raw):
        return

    imei = raw.get("imei") or raw.get("IMEI") or extract_imei_from_topic(topic)
    geoid = extract_geoid(raw)
    interval = extract_interval(raw)

    device = await devices_master.find_one({"topic": topic})
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

    updates = {}
    if geoid is not None and geoid != device.get("Geoid"):
        updates["Geoid"] = geoid
    if interval is not None and interval != device.get("interval"):
        updates["interval"] = interval

    if updates:
        updates["updated_at"] = current_ist_naive()
        await devices_master.update_one({"_id": device["_id"]}, {"$set": updates})


async def handle_mqtt_message(topic: str, payload_bytes: bytes):
    print(f"topic: {topic} , raw: {payload_bytes}")

    try:
        raw = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        raw = {"raw_body": payload_bytes.decode("utf-8", errors="ignore")}

    await raw_collection.insert_one(
        {"_id": ObjectId(), "topic": topic, **{f"raw_{k}": v for k, v in raw.items()}}
    )

    await sync_device_master(topic, raw)
    analytics_doc = build_analytics_record(topic, raw)
    await analytics_collection.insert_one(analytics_doc)

    print(
        f"[STORED] {topic} event={analytics_doc['event_kind']} type={analytics_doc['type']}"
    )
