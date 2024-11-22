from pathlib import Path
from typing import Literal

# paths
DATA_DIR = Path("/opt/airflow/data")
LANDING_ZONE = DATA_DIR / "landing_zone"
ARCHIVE_ZONE = DATA_DIR / "archive_zone"

DATA_SOURCES = Literal["routy", "voluum", "manual"]

# Airflow Variables
POSTGRES_CONN_ID = "postgres_conn_id"
