from motor.motor_asyncio import AsyncIOMotorClient

from config import MONGO_DB, MONGO_URI


mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

raw_collection = db["raw_data"]
analytics_collection = db["analytics_data"]
devices_master = db["devices_master"]

