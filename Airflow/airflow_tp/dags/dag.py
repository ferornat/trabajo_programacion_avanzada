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
	# Cualquier task definida dentro de la cláusula ‘with’ será parte del grupo
	with TaskGroup(group_id="post_process") as post_process_group:
		for i in range(5):
			DummyOperator(task_id=f"dummy_task_{i}")

def sample_normal(mean, std):
	return np.random.normal(loc=mean, scale=std)

random_height = PythonOperator(
	task_id='height',
	python_callable=sample_normal,
	op_kwargs = {"mean" : 170, "std": 15},
)

# Creamos 2 tasks dummy para definir dependencias
first = BashOperator(task_id='first', bash_command='sleep 3 && echo First')
last = BashOperator(task_id='last', bash_command='sleep 3 && echo Last')

# Con el operador >> definimos dependencias, a >> b significa que b se ejecuta una vez que a termina
first >> sleep
first >> random_height

# Podemos agrupar varias tareas al definir precedencias
[sleep, random_height] >> last

# Podemos definir dependencias a nivel grupo de tareas
last >> post_process_group