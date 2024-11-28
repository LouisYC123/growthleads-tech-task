from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig

from growthleads_etl import config


@dag(
    dag_id="combine_results_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def combine_results_dag():
    wait_for_web_traffic = ExternalTaskSensor(
        task_id="wait_for_web_traffic",
        external_dag_id="web_traffic_dag",
        external_task_id=None,  # Wait for the entire DAG to complete
        mode="poke",
        timeout=600,
        poke_interval=30,
    )

    wait_for_scrapers = ExternalTaskSensor(
        task_id="wait_for_scrapers",
        external_dag_id="scrapers_dag",
        external_task_id=None,
        mode="poke",
        timeout=600,
        poke_interval=30,
    )

    render_config = RenderConfig(
        select=["solution_3_marketing_daily", "solution_3_operators_monthly"],
    )
    with DbtTaskGroup(
        group_id="combine_results",
        render_config=render_config,
        project_config=ProjectConfig(config.DBT_CONFIG["dbt_root_path"]),
        profile_config=ProfileConfig(
            profiles_yml_filepath=config.DBT_CONFIG["dbt_profiles_dir"],
            profile_name=config.DBT_CONFIG["dbt_profile_name"],
            target_name=config.DBT_CONFIG["dbt_target_name"],
        ),
    ) as combine_results:
        pass

    [wait_for_web_traffic, wait_for_scrapers] >> combine_results


dag_c_instance = combine_results_dag()
