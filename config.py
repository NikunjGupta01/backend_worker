import os
from dotenv import load_dotenv

load_dotenv()

# Debug: Print all env variables
print("Environment variables loaded:")
print(f"MONGO_URI: {os.getenv('MONGO_URI')}")
print(f"MONGO_DB: {os.getenv('MONGO_DB')}")
print(f"MONGO_DB type: {type(os.getenv('MONGO_DB'))}")

# ==== MQTT CONFIG ====
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# ==== IMEI LIST ====
IMEIS = os.getenv("IMEIS", "").split(",")
TOPICS = [f"{imei}/pub" for imei in IMEIS if imei.strip()]

# ==== MONGO CONFIG ====
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

# Validate MONGO_DB
if not MONGO_DB:
    raise ValueError("MONGO_DB environment variable is not set or empty")