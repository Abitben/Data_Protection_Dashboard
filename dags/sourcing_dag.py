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
from include.sourcing.task3_clean_csv import clean_csv
from include.sourcing.task4_upload_to_bq import upload_to_bq
from airflow.models.xcom import XCom

# adjust for other database types
from pendulum import datetime
import os
from pathlib import Path

CONNECTION_ID = "gcp"

def get_xcom(**kwargs):
    print('this is',  kwargs)
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids=['sourcing.check_if_updated'])
    return xcom_value

@dag(
    start_date=datetime(2023, 8, 1),
    schedule='@weekly',
    catchup=False,
)
def sourcing_dag():

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

    clean_csv_task = ExternalPythonOperator(
        task_id='clean_csv',
        python=os.environ["ASTRO_PYENV_custom_python"],
        python_callable=clean_csv,
        provide_context=True,
        op_kwargs={'xcom_value': get_xcom_task.output}
    )

    upload_to_bq_task = ExternalPythonOperator(
        task_id='upload_to_bq',
        python=os.environ["ASTRO_PYENV_custom_python"],
        python_callable=upload_to_bq,
        provide_context=True,
        op_kwargs={'xcom_value': get_xcom_task.output}
    )

    get_cnil_indexes_task >> check_if_updated_task >> get_xcom_task >> clean_csv_task >> upload_to_bq_task

sourcing_dag()