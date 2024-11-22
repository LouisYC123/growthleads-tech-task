import pandas as pd


from growthleads_etl import extract_etl, tasks
from growthleads_etl.settings import LANDING_ZONE, POSTGRES_CONN_ID

from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.python import PythonSensor


@task
def read_data_task(source: str, landing_zone: str):
    """Read data from the source and return a DataFrame."""
    return extract_etl.read_data(source, landing_zone)


@task
def validate_schema_task(df: pd.DataFrame, schema):
    """Validate the DataFrame against the defined schema."""
    return schema.validate(df, lazy=True)


@task
def load_data_task(df: pd.DataFrame, source: str, layer: str, conn_id: str):
    """Load the DataFrame to the target database."""
    return extract_etl.load_dataframe_to_postgres(df, source, layer, conn_id)


def extract_source(source: str, schema):
    """
    Create a TaskGroup for extracting and processing data from a specific source.

    Args:
        source (str): The name of the data source (e.g., 'routy').
        schema: The validation schema object.

    Returns:
        TaskGroup: A TaskGroup object containing all tasks for the source.
    """
    with TaskGroup(group_id=f"extract_{source}") as extract_group:
        # Task 1: Wait for CSV
        wait_for_csv = PythonSensor(
            task_id=f"wait_for_{source}_csv",
            python_callable=extract_etl.is_csv_available,
            op_args=[str(LANDING_ZONE / source)],  # Convert path to string
            poke_interval=5,
            timeout=21600,  # Timeout after 6 hours
        )

        # Task 2: Read Data
        data = tasks.read_data_task(source=source, landing_zone=LANDING_ZONE)

        # Task 3: Validate Schema
        validated_data = validate_schema_task(
            df=data,
            schema=schema,
        )

        # Task 4: Load Data to db
        load_data = load_data_task(
            df=validated_data,
            source=source,
            layer="bronze",
            conn_id=POSTGRES_CONN_ID,
        )

        # Set dependencies
        wait_for_csv >> data >> validated_data >> load_data

    return extract_group
