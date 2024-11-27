import pandas as pd

from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.python import PythonSensor
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig

from growthleads_etl import extract_etl
from growthleads_etl import config


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


def extract_taskgroup(group_id: str, data_sources: dict, exclude: list = None):
    """
    Dynamically creates tasks for each data source in the config and adds them to a TaskGroup.

    Args:
        group_id: The ID of the TaskGroup.
        data_sources: A dictionary of data sources.
        exclude: A list of sources to exclude.

    Returns:
        TaskGroup: The dynamically generated TaskGroup.
    """

    filtered_data_sources = {
        source: config
        for source, config in data_sources.items()
        if exclude is None or source not in exclude
    }

    with TaskGroup(group_id=group_id) as extract:
        for source_name, source_config in filtered_data_sources.items():
            # Create a task group for each source
            source_task_group = extract_source(source_name, source_config)
            if source_config.get("if_missing") == "skip":
                skip_task = DummyOperator(
                    task_id=f"{source_name}_optional", trigger_rule=TriggerRule.ALL_DONE
                )
                source_task_group >> skip_task
            else:
                source_task_group
    return extract


def transform_taskgroup(group_id: str, dbt_config: dict, exclude: list = None):

    render_config = RenderConfig(
        exclude=[
            f"{source}_{model}"
            for source in exclude
            for model in config.DBT_MODEL_TYPES
        ],
    )

    with DbtTaskGroup(
        group_id=group_id,
        render_config=render_config,
        project_config=ProjectConfig(dbt_config["dbt_root_path"]),
        profile_config=ProfileConfig(
            profiles_yml_filepath=dbt_config["dbt_profiles_dir"],
            profile_name=dbt_config["dbt_profile_name"],
            target_name=dbt_config["dbt_target_name"],
        ),
    ) as transform_group:
        pass

    return transform_group
