from datetime import datetime, timedelta

from airflow.decorators import dag

from growthleads_etl import tasks, config, data_sources


# TODO: add a check to ensure pg_conn_id is set in Airflow Connections


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def web_traffic_dag():
    """
    DAG to monitor a folder for a CSV file and load it into a DataFrame.
    """
    extract_webtraffic = tasks.extract_taskgroup(
        data_sources.WEB_TRAFFIC, "extract_webtraffic"
    )

    extract_scd = tasks.extract_taskgroup(data_sources.SCD, "extract_scd")

    transform = tasks.transform_taskgroup(config.DBT_CONFIG)

    [extract_webtraffic, extract_scd] >> transform


dag = web_traffic_dag()
