"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

import sys
import os
sys.path.append(os.path.abspath(os.environ["AIRFLOW_HOME"]))

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.python import ExternalPythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from airflow.sensors.external_task import ExternalTaskSensor
from cosmos.constants import LoadMode


# adjust for other database types
from pendulum import datetime
import os
from pathlib import Path

CONNECTION_ID = "gcp"

# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/include/dbt"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_cnil",
    target_name="dev",
    profiles_yml_filepath=Path(f"{os.environ['AIRFLOW_HOME']}/include/dbt/profiles.yml")
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

def get_xcom(**kwargs):
    print('this is',  kwargs)
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids=['sourcing.check_if_updated'])
    return xcom_value

@dag(
    start_date=datetime(2023, 8, 1),
    schedule='@quarterly',
    catchup=False,
)
def violations_dbt_dag():

    # sensors_sourcing_task = ExternalTaskSensor(
    #     task_id='trigger_sourcing',
    #     external_dag_id='sourcing_dag',
    #     external_task_id='upload_to_bq',
    # )

    violations_task = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
              load_method=LoadMode.DBT_LS,
              select=['path:models/violations'],
          )
    )
    
    violations_task

violations_dbt_dag()