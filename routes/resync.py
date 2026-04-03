from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from bson import ObjectId

from mqtt_runtime import get_mqtt_client
from services.subscription_sync import resync_topic
from db import device_settings, devices_master
from helper.http_responses import HttpResponses
from services.time_utils import current_ist_naive

router = APIRouter()

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


@router.post("/resync-topic")
async def resync_topic_route(topic: str, is_active: bool):
    return await resync_topic(
        topic=topic,
        is_active=is_active,
        mqtt_client=get_mqtt_client(),
    )


@router.get("/test-settings")
async def save_settings(
    topic: str = "862942074957887/pub",
    raw: dict = {
        "NormalSendingInterval": "1800",
        "SOSSendingInterval": "10",
        "NormalScanningInterval": "850",
        "AirplaneInterval": "10",
        "SpeedLimit": "50",
        "LowbatLimit": "30",
        "phonenum1": "",
        "phonenum2": "9576263111",
        "controlroomnum": "9576113111",
        "currentprofile": "Airtel",
    },
):
    # 🔍 Find device
    device = await devices_master.find_one({"topic": topic})
    if not device:
        return HttpResponses.error(
            message=f"[WARN] Device not found for topic={topic}; skipping settings sync",
            code=404,
        )

    # 🧠 Filter only allowed keys
    settings_updates = {
        key: raw.get(key) for key in SETTINGS_KEYS if raw.get(key) is not None
    }

    if not settings_updates:
        return HttpResponses.success(message="No settings updated")

    settings_updates["updated_at"] = current_ist_naive()

    # 🔄 Check if settings already exist
    settings = await device_settings.find_one({"device": device["_id"]})

    # ✅ UPDATE FLOW
    if settings:
        await device_settings.update_one(
            {"_id": settings["_id"]},
            {"$set": settings_updates},
        )

        updated_settings = await device_settings.find_one({"device": device["_id"]})

        return HttpResponses.success(
            data=jsonable_encoder(updated_settings),
            message="Settings updated successfully",
        )

    # ✅ INSERT FLOW
    settings_updates["_id"] = ObjectId()
    settings_updates["device"] = device["_id"]
    settings_updates["created_at"] = current_ist_naive()

    await device_settings.insert_one(settings_updates)

    current_settings = await device_settings.find_one({"device": device["_id"]})

    return HttpResponses.success(
        data=jsonable_encoder(current_settings),
        message="Setting Added Successfully",
    )
