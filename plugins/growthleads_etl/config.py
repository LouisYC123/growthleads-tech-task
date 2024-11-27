from pathlib import Path
from typing import Literal

# Paths
DATA_DIR = Path("/opt/airflow/data")
LANDING_ZONE = DATA_DIR / "landing_zone"
ARCHIVE_ZONE = DATA_DIR / "archive_zone"


# Airflow Variables & Settings
POSTGRES_CONN_ID = "postgres_conn_id"
LANDING_POKE_INTERVAL = 5  # Check landing zone for data every 5 seconds
LANDING_TIMEOUT = 60  # Stop checking landing zone for data after 60 seconds
# LANDING_TIMEOUT = 21600  # Stop checking landing zone for data after 6 hours

# Data types
SCHEMA_NAMES = Literal["bronze", "silver", "gold"]
LOAD_TYPES = Literal["append", "replace", "fail"]

# dbt config
DBT_CONFIG = {
    "dbt_project_name": "growth_leads",
    "dbt_root_path": "/usr/local/airflow/dbt/growth_leads",
    "dbt_executable_path": "/usr/local/airflow/.local/bin/dbt",
    "dbt_profiles_dir": "/usr/local/airflow/dbt/growth_leads/profiles.yml",
    "dbt_profile_name": "dbt_profile",
    "dbt_target_name": "dev",
}
DBT_MODEL_TYPES = ["cleaned", "enriched"]
