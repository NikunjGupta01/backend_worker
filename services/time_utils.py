from datetime import datetime, timezone
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def current_ist_naive():
    aware = datetime.now(timezone.utc).astimezone(IST)
    return aware.replace(tzinfo=None)

