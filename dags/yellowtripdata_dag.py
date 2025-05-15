import os
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Jakarta")

profile_config = ProfileConfig(
    profile_name="dev",
    target_name="local",
    profiles_yml_filepath="/home/airflow/.dbt",  # chemin dans le conteneur
)

execution_config = ExecutionConfig(
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",  # vérifie ce chemin
)

default_args = {
    "start_date": datetime(2025, 3, 25, tzinfo=local_tz),
    "owner": "harits",
}

@dag(schedule_interval='@hourly', catchup=False, default_args=default_args)
def yellowtripdata_dag():
    start = EmptyOperator(task_id="start")

    yellowtripdata_analytics = DbtTaskGroup(
        group_id="yellowtripdata_analytics",
        project_config=ProjectConfig("/opt/airflow/dbt/dev"),
        profile_config=profile_config,
        execution_config=execution_config
    )

    stop = EmptyOperator(task_id="stop")

    start >> yellowtripdata_analytics >> stop

yellowtripdata_dag()
