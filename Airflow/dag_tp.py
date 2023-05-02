# Cargamos librerías a utilizar
import datetime
from datetime import timedelta
import numpy as np
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import psycopg2 # Ésto nos va a servir para insertar los resultados en la base de datos

# Definimos argumentos para el airflow
default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

# Definimos el cliente para poder conectarnos con S3, donde tenemos las bases cargadas
import boto3
import config
MY_API_KEY = config.MY_API_KEY
MY_SECRET_KEY = config.MY_SECRET_KEY

# Definimos el engine para lo que sería la base de datos RDS
USUARIO_POSTGRES = config.USUARIO_POSTGRES
CONTRASENA_POSTGRES = config.CONTRASENA_POSTGRES
DATABASE_POSTGRES = config.DATABASE_POSTGRES
HOST_POSTGRES = config.HOST_POSTGRES
PUERTO_POSTGRES = config.PUERTO_POSTGRES

# Configuramos el engine
engine = psycopg2.connect(
    database = DATABASE_POSTGRES,
    user = USUARIO_POSTGRES,
    password = CONTRASENA_POSTGRES,
    host = HOST_POSTGRES,
    port = PUERTO_POSTGRES
)

# Creamos el cursor
cursor = engine.cursor()


# Definimos funciones que guarden la info de los logs
def FiltrarDatos():
    # Definimos usuarios inactivos
    obj1 = s3.get_object(Bucket = 'bases-fer2', Key = 'advertiser_ids')
    usuarios = pd.read_csv(obj1['Body'])
    usuarios_inactivos = usuarios.tail(5)['advertiser_id'].to_list()

    # Definimos período de tiempo
    yesterday = (datetime.date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Filtramos los logs para el modelo de CTR
    obj2 = s3.get_object(Bucket = 'bases-fer2', Key = 'ads_views')
    total_logs = pd.read_csv(obj2['Body'])
    datos_filtrados1 = total_logs[(~total_logs['advertiser_id'].isin(usuarios_inactivos)) &
                                  (total_logs['date'] == yesterday)]
    
    # Filtramos los logs para el modelo de TopProduct
    obj3 = s3.get_object(Bucket = 'bases-fer2', Key = 'product_views')
    total_products = pd.read_csv(obj3['Body'])
    datos_filtrados2 = total_products[(~total_products['advertiser_id'].isin(usuarios_inactivos)) &
                                      (total_products['date'] == yesterday)]

    # Lanzamos resultados
    return [datos_filtrados1.to_dict(), datos_filtrados2.to_dict()]
    #return [datos_filtrados1, datos_filtrados2]

# Calculamos TOPCTR
def TopCTR(**kwargs):    
    # Obtenemos la base de datos previa
    datos_filtrados = kwargs['ti'].xcom_pull(task_ids = 'FiltrarDatos')[0]
    datos_filtrados = pd.DataFrame.from_dict(datos_filtrados)

    # Hacemos las agrupaciones
    top_ctr = datos_filtrados.groupby(['advertiser_id','product_id'])['type'].value_counts().reset_index().pivot(index = ['advertiser_id','product_id'], columns='type', values='count').reset_index()
    top_ctr.fillna(0, inplace = True)

    # Hacemos el ratio
    top_ctr['ctr'] = round(top_ctr['click']  / top_ctr['impression'],2)

    # Casteamos los valores infinitos como un ctr del 100%, asumiento un typo en el registro de la impresión
    top_ctr['ctr'] = np.where(np.isinf(top_ctr['ctr']),1,top_ctr['ctr'])

    # Ordenamos por los valores más altos
    top_ctr = top_ctr.sort_values(by = ['advertiser_id', 'ctr'], ascending = [True, False])

    # Obtenemos los 20 productos con mayor conversión por advertiser
    resultado_top_ctr = top_ctr.groupby('advertiser_id').head(20)

    # Nos quedamos con las columnas de interés
    resultado_top_ctr['log'] = datetime.date.today().strftime("%Y-%m-%d")
    resultado_top_ctr = resultado_top_ctr[['log','advertiser_id','product_id']]

    # Lanzamos resultados
    return resultado_top_ctr.to_dict()

# Calculamos TopProduct
def TopProduct(**kwargs):
    # Obtenemos la base de datos previa
    datos_filtrados = kwargs['ti'].xcom_pull(task_ids = 'FiltrarDatos')[1]
    datos_filtrados = pd.DataFrame.from_dict(datos_filtrados)
    
    # Hacemos las agrupaciones
    datos_filtrados = datos_filtrados.groupby(['advertiser_id', 'product_id']).count().reset_index()
    
    # Ordenamos
    top_product = datos_filtrados.sort_values(by = ['advertiser_id', 'date'], ascending = [True, False])    

    # Nos quedamos con los 20 productos más vistos por advertiser
    resultados_top_product = top_product.groupby('advertiser_id').head(20)

    # Nos quedamos con las columnas de interés
    resultados_top_product['log'] = datetime.date.today().strftime("%Y-%m-%d")
    resultados_top_product = resultados_top_product[['log','advertiser_id','product_id']]

    # Lanzamos resultados
    return resultados_top_product.to_dict()

def DBWriting(**kwargs):
    # Recuperamos los resultados de TopCTR
    resultados_top_ctr = kwargs['task_instance'].xcom_pull(task_ids = 'TopCTR')
    
    # Covertimos la data filtrada en un dataframe
    resultados_top_ctr = pd.DataFrame.from_dict(resultados_top_ctr)
    resultados_top_ctr = resultados_top_ctr.values.tolist()

    # Guardamos la data para el modelo de TopCTR
    query_creacion_tabla_top_ctr = 'CREATE TABLE IF NOT EXISTS resultados_top_ctr ( log TIMESTAMP, advertiser_id VARCHAR(50), product_id VARCHAR(50) )'
    cursor.execute(query_creacion_tabla_top_ctr)
    
    query_resultados_tabla_top_ctr = 'INSERT INTO resultados_top_ctr VALUES (%s, %s, %s)'
    cursor.executemany(query_resultados_tabla_top_ctr, resultados_top_ctr)
    
    # Recuperamos los resultados de TopProduct
    resultados_top_product = kwargs['ti'].xcom_pull(task_ids = 'TopProduct')
    
    # Covertimos la data filtrada en un dataframe
    resultados_top_product = pd.DataFrame.from_dict(resultados_top_product)
    resultados_top_product = resultados_top_product.values.tolist()
    
    # Guardamos al data filtrada
    query_creacion_tabla_top_product = 'CREATE TABLE IF NOT EXISTS resultados_top_product ( log TIMESTAMP, advertiser_id VARCHAR(50), product_id VARCHAR(50) )'
    cursor.execute(query_creacion_tabla_top_product)

    query_resultados_tabla_top_product = 'INSERT INTO resultados_top_product VALUES (%s, %s, %s)'
    cursor.executemany(query_resultados_tabla_top_product, resultados_top_product)

# Definimos nuestro DAG
with DAG(
	dag_id = 'trabajo_practico',
    default_args = default_args,
	schedule_interval = timedelta(days  = 1),
	start_date = datetime.datetime(2022, 5, 20),
	catchup = False,
    tags = ['trabajo_practico']
) as dag:
    filtrado_task = PythonOperator(task_id = "FiltrarDatos", python_callable = FiltrarDatos, provide_context = True)
    top_ctr_task = PythonOperator(task_id = "TopCTR", python_callable = TopCTR, provide_context = True)
    top_product_task = PythonOperator(task_id = "TopProduct", python_callable = TopProduct, provide_context = True)
    escritura_task = PythonOperator(task_id = "DBWriting", python_callable = DBWriting, provide_context = True)

# Las preferencias no van identadas dentro del DAG
filtrado_task >> [top_ctr_task,top_product_task] >> escritura_task

