import datetime
import numpy as np
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Agregar estos imports
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
	dag_id='test_con_jota',
	schedule_interval=None,
	start_date=datetime.datetime(2022, 4, 1),
	catchup=False,
) as dag:
	sleep = BashOperator(
		task_id='sleep',
		bash_command='sleep 3',
)