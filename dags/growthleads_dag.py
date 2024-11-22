from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from growthleads_etl import schemas, tasks


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

    with TaskGroup(group_id="extract") as extract:

        extract_routy = tasks.extract_source(
            source="routy",
            schema=schemas.RoutyBronzeDataset,
        )
        extract_voluum = tasks.extract_source(
            source="voluum",
            schema=schemas.VoluumBronzeDataset,
        )

        # Set task dependencies
        [extract_routy, extract_voluum]

    extract  # Reference the task group


dag = web_traffic_dag()
