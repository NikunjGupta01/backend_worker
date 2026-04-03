import json
from bson import ObjectId

from db import analytics_collection, devices_master, raw_collection, device_settings
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


SETTINGS_KEYS = (
    "NormalSendingInterval",
    "SOSSendingInterval",
    "NormalScanningInterval",
    "AirplaneInterval",
    "TemperatureLimit",
    "SpeedLimit",
    "LowbatLimit",
    "phonenum1",
    "phonenum2",
    "controlroomnum",
    "currentprofile",
)
SETTINGS_KEY_MAP = {k.lower(): k for k in SETTINGS_KEYS}


def is_settings_payload(raw: dict) -> bool:
    if not isinstance(raw, dict):
        return False
    raw_keys_lower = {k.lower() for k in raw.keys()}
    return any(k in raw_keys_lower for k in SETTINGS_KEY_MAP)


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

    device = await devices_master.find_one({"topic": topic})
    if not device:
        print(f"[WARN] Device not found for topic={topic}; skipping status sync")
        return
    print(f"Device: {device}")
    # Latest telemetry/status snapshot fields on device master
    latest_status_raw = {
        "packet": raw.get("packet"),
        "latitude": raw.get("latitude") or raw.get("lat"),
        "longitude": raw.get("longitude") or raw.get("lon") or raw.get("long"),
        "speed": raw.get("speed"),
        "temperature": raw.get("temperature"),
        "timestamp": extract_device_raw_timestamp(raw),
        "Battery": raw.get("Battery"),
        "Signal": raw.get("Signal"),
        "GPSStrength": raw.get("GPSStrength"),
    }

    latest_status = {
        field: value for field, value in latest_status_raw.items() if value is not None
    }

    if latest_status:
        latest_status["updated_at"] = current_ist_naive()
        await devices_master.update_one({"_id": device["_id"]}, {"$set": latest_status})


async def save_settings(topic: str, raw: dict):
    device = await devices_master.find_one({"topic": topic})
    if not device:
        print(f"[WARN] Device not found for topic={topic}; skipping settings sync")
        return

    settings_updates = {}
    for key, value in raw.items():
        canon = SETTINGS_KEY_MAP.get(str(key).lower())
        if canon is not None and value is not None:
            settings_updates[canon] = value

    if not settings_updates:
        return

    settings_updates["updated_at"] = current_ist_naive()

    settings = await device_settings.find_one({"device": device["_id"]})
    if settings:
        await device_settings.update_one(
            {"_id": settings["_id"]},
            {"$set": settings_updates},
        )
        return

    settings_updates["_id"] = ObjectId()
    settings_updates["device"] = device["_id"]
    settings_updates["created_at"] = current_ist_naive()
    await device_settings.insert_one(settings_updates)


async def handle_mqtt_message(topic: str, payload_bytes: bytes):
    print(f"topic: {topic} , raw: {payload_bytes}")

    payload_text = payload_bytes.decode("utf-8", errors="ignore")

    try:
        raw = json.loads(payload_text)
    except Exception:
        # Device sometimes sends malformed JSON like ..."}","phonenum2":...
        cleaned = (
            payload_text.replace('"}","', '","')
            .replace('"},"', '","')
            .replace(',"}', ',"')
        )
        try:
            raw = json.loads(cleaned)
            print("Parsed settings after cleanup")
        except Exception:
            raw = {"raw_body": payload_text}

    await raw_collection.insert_one(
        {"_id": ObjectId(), "topic": topic, **{f"raw_{k}": v for k, v in raw.items()}}
    )
    if is_settings_payload(raw):
        print(f"Settings to h {raw}")
        await save_settings(topic=topic, raw=raw)
    await sync_device_master(topic, raw)
    analytics_doc = build_analytics_record(topic, raw)
    await analytics_collection.insert_one(analytics_doc)

    print(
        f"[STORED] {topic} event={analytics_doc['event_kind']} type={analytics_doc['type']}"
    )
