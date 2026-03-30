import paho.mqtt.client as mqtt

from db import devices_master
from helper.http_responses import HttpResponses
from services.time_utils import current_ist_naive


async def sync_mqtt_subscriptions(mqtt_client: mqtt.Client):
    """
    DB is the source of truth:
      - subscribe docs where is_active=True and is_subscribed!=True
      - unsubscribe docs where is_active=False and is_subscribed=True
    """
    to_subscribe = devices_master.find(
        {"is_active": True},
        {"topic": 1, "_id": 1},
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


async def resync_topic(topic: str, mqtt_client: mqtt.Client):
    try:
        record = await devices_master.find_one({"topic": topic})
        if record is None:
            return HttpResponses.error(message="Device Does Not Exist", code=404)

        if not mqtt_client.is_connected():
            return HttpResponses.error(message="MQTT client not connected", code=503)

        if bool(record.get("is_active", True)):
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

            return HttpResponses.error(
                message=f"Subscribe failed for topic={topic}, code={result}",
                code=500,
            )

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
