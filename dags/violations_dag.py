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
from cosmos.constants import LoadMode
from airflow.utils.task_group import TaskGroup
from include.sourcing.task1_get_cnil_indexes import get_cnil_indexes
from include.sourcing.task2_check_if_updated import check_if_updated
from include.sourcing.task3_upload_to_bq import upload_to_bq
from airflow.models.xcom import XCom

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
    schedule=None,
    catchup=False,
)
def violations_dbt_dag():

    with TaskGroup("sourcing") as tg1:
        get_cnil_indexes_task = ExternalPythonOperator(
            task_id='get_cnil_indexes',
            python=os.environ["ASTRO_PYENV_custom_python"],
            python_callable=get_cnil_indexes,
        )

        check_if_updated_task = ExternalPythonOperator(
            task_id='check_if_updated',
            python=os.environ["ASTRO_PYENV_custom_python"],
            provide_context=True,
            python_callable=check_if_updated,
        )

        get_xcom_task = PythonOperator(
            task_id='get_xcom',
            python_callable=get_xcom,
            provide_context=True
        )

        upload_to_bq_task = ExternalPythonOperator(
            task_id='upload_to_bq',
            python=os.environ["ASTRO_PYENV_custom_python"],
            python_callable=upload_to_bq,
            provide_context=True,
            op_kwargs={'xcom_value': get_xcom_task.output}
        )

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
              load_method=LoadMode.DBT_LS,
              select=['path:models/violations'],
          )
    )

    get_cnil_indexes_task >> check_if_updated_task >> get_xcom_task >> upload_to_bq_task

    tg1 >> transform_data


violations_dbt_dag()