import os
import logging

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("pothole-backend")

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]

CLUSTER_RADIUS_M: float   = float(os.getenv("CLUSTER_RADIUS_M", "5.0"))
SAMPLE_RATE_HZ: int       = int(os.getenv("SAMPLE_RATE_HZ", "200"))
SNAPSHOT_INTERVAL_S: int  = int(os.getenv("SNAPSHOT_INTERVAL_S", "900"))
SNAPSHOT_RETENTION_H: int = int(os.getenv("SNAPSHOT_RETENTION_H", "24"))
MILES_TO_METRES: float    = 1_609.344
DEVICE_ID_SALT: str       = os.environ["DEVICE_ID_SALT"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
