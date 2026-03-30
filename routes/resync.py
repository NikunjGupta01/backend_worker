from fastapi import APIRouter

from mqtt_runtime import get_mqtt_client
from services.subscription_sync import resync_topic


router = APIRouter()


@router.post("/resync-topic")
async def resync_topic_route(topic: str):
    return await resync_topic(topic, get_mqtt_client())

