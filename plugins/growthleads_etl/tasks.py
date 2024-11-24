import pandas as pd

from growthleads_etl import extract_etl
from growthleads_etl import config

from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.python import PythonSensor
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig


@task
def read_data(source: str):
    """Read data from the source and return a DataFrame."""
    return extract_etl.read_data(source, config.LANDING_ZONE)


@task
def validate_schema(df: pd.DataFrame, schema):
    """Validate the DataFrame against the defined schema."""
    return schema.validate(df, lazy=True)


@task
def load_to_db(df: pd.DataFrame, source: str, layer: str, load_type: config.LOAD_TYPES):
    """Load the DataFrame to the target database."""
    return extract_etl.load_dataframe_to_postgres(
        df, source, layer, config.POSTGRES_CONN_ID, if_exists=load_type
    )


@task
def archive_data(source: str):
    """Archive the CSV files after processing."""
    return extract_etl.move_csv_to_archive(
        source, config.LANDING_ZONE, config.ARCHIVE_ZONE
    )


def extract_source(source: str, source_config: str):
    """
    Create a TaskGroup for extracting and processing data from a specific source.

    Args:
        source (str): The name of the data source (e.g., 'routy').

    Returns:
        TaskGroup: A TaskGroup object containing all tasks for the source.
    """
    with TaskGroup(group_id=f"extract_{source}") as extract_group:
        # Task 1: Wait for CSV
        wait_for_csv = PythonSensor(
            task_id=f"wait_for_{source}_csv",
            python_callable=extract_etl.is_csv_available,
            op_args=[str(config.LANDING_ZONE / source)],
            poke_interval=config.LANDING_POKE_INTERVAL,
            timeout=config.LANDING_TIMEOUT,
        )

        # Task 2: Read Data
        data = read_data(source)

        # Task 3: Validate Schema
        validated_data = validate_schema(
            df=data,
            schema=source_config["schema"],
        )

        # Task 4: Load Data to db
        load_data = load_to_db(
            df=validated_data,
            source=source,
            layer="bronze",
            load_type=source_config["load_type"],
        )

        # Conditionally add Task 5: Archive Data
        if source_config.get("archive", False):
            archive = archive_data(source)
            load_data >> archive

        # Set initial dependencies
        wait_for_csv >> data >> validated_data >> load_data

    return extract_group


def extract_taskgroup(data_sources: dict, group_id: str):
    """
    Dynamically creates tasks for each data source in the config and adds them to a TaskGroup.

    Args:
        config: A dictionary of data sources.
        tasks: A module containing reusable task functions.

    Returns:
        TaskGroup: The dynamically generated TaskGroup.
    """
    with TaskGroup(group_id=group_id) as extract:
        for source_name, source_config in data_sources.items():
            source_task_group = extract_source(source_name, source_config)
            source_task_group
    return extract


def transform_taskgroup(dbt_config: dict):

    with DbtTaskGroup(
        project_config=ProjectConfig(dbt_config["dbt_root_path"]),
        profile_config=ProfileConfig(
            profiles_yml_filepath=dbt_config["dbt_profiles_dir"],
            profile_name=dbt_config["dbt_profile_name"],
            target_name=dbt_config["dbt_target_name"],
        ),
    ) as transform_group:
        pass

    return transform_group
