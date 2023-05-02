# Cargamos librerías a utilizar
import datetime
from datetime import timedelta
import numpy as np
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Definimos argumentos
default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'email': ['faornat@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5)
}

# Definimos funciones que guarden la info de los logs
def scrape():
    logging.info("performing scraping")
    
def process():
    logging.info("performing processing")
    
def save():
    logging.info("performing saving")
    
# Definimos nuestro DAG
with DAG(
	dag_id = 'test_prueba_fer',
    default_args = default_args,
	schedule_interval = timedelta(days = 1),
	start_date = datetime.datetime(2022, 4, 1),
	catchup = False,
    tags = ['example']
) as dag:
	scrape_task = PythonOperator(task_id = "scrape", python_callable = scrape)
	process_task = PythonOperator(task_id = "process", python_callable = process)
	save_task = PythonOperator(task_id = "save", python_callable = save)

# Las preferencias no van identadas dentro del DAG
scrape_task >> process_task >> save_task    