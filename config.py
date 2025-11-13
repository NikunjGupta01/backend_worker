import os
from dotenv import load_dotenv

load_dotenv()

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
