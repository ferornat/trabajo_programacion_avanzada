# Cargamos librerías a utilizar
import datetime
from datetime import timedelta
import numpy as np
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Definimos argumentos
default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

# Definimos funciones que guarden la info de los logs
def FiltrarDatos():
    # Definimos usuarios inactivos
    usuarios = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/advertiser_ids')
    usuarios_inactivos = usuarios.tail(5)['advertiser_id'].to_list()

    # Definimos período de tiempo
    yesterday = (datetime.date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Filtramos los logs para el modelo de CTR
    total_logs = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/ads_views')
    datos_filtrados1 = total_logs[(~total_logs['advertiser_id'].isin(usuarios_inactivos)) &
                                  (total_logs['date'] == yesterday)]
    
    # Filtramos los logs para el modelo de TopProduct
    total_products = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/product_views')
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

    # Guardamos la data para el modelo de TopCTR
    resultados_top_ctr.to_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_top_ctr.csv', index = False, header = True)
    
    # Recuperamos los resultados de TopProduct
    resultados_top_product = kwargs['ti'].xcom_pull(task_ids = 'TopProduct')
    
    # Covertimos la data filtrada en un dataframe
    resultados_top_product = pd.DataFrame.from_dict(resultados_top_product)
    
    # Guardamos al data filtrada
    resultados_top_product.to_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_top_product.csv', index = False)

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