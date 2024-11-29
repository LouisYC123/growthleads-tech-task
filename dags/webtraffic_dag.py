from datetime import datetime, timedelta

from airflow.decorators import dag

from growthleads_etl import tasks, config, data_sources

DATA_TYPE = "web_traffic"
EXCLUDE_DATA_SOURCES = [
    "scrapers",
]


@dag(
    dag_id="web_traffic_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=3),
    },
)
def web_traffic_dag():
    """
    DAG to Extract, Load and Transform web traffic data.
    """

    extract_webtraffic = tasks.extract_taskgroup(
        group_id=f"extract_{DATA_TYPE}",
        data_sources=data_sources.EVENTS,
        exclude=EXCLUDE_DATA_SOURCES,
    )

    extract_scd = tasks.extract_taskgroup(
        group_id="extract_scd",
        data_sources=data_sources.SCD,
        exclude=EXCLUDE_DATA_SOURCES,
    )

    transform = tasks.transform_taskgroup(
        group_id=f"transform_{DATA_TYPE}",
        dbt_config=config.DBT_CONFIG,
        exclude=EXCLUDE_DATA_SOURCES,
    )

    [extract_webtraffic, extract_scd] >> transform


dag = web_traffic_dag()
