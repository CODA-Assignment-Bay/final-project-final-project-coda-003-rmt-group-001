import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd

default_args = {
    'owner': 'group-001',
    'start_date': dt.datetime(2025, 2, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=15),
}

with DAG('Final_Project',
         default_args=default_args,
         schedule_interval='0 1 * * *',
         catchup=False,
         ) as dag:

    extract_data = BashOperator(task_id='extract_data', bash_command='sudo -u airflow python /opt/airflow/dags/extract.py')

    transform_data = BashOperator(task_id='transform_data', bash_command='sudo -u airflow python /opt/airflow/dags/transform.py')

    load_data = BashOperator(task_id='load_data', bash_command='sudo -u airflow python /opt/airflow/dags/load.py')

    datamart = BashOperator(task_id='datamart', bash_command='sudo -u airflow python /opt/airflow/dags/datamart.py')


extract_data >> transform_data >> load_data >> datamart