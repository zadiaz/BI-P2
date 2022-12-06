# Utilidades de airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Utilidades de python
from datetime  import datetime

# Funciones ETL
from utils.crear_tablas import crear_tablas
from utils.file_util import procesar_datos
from utils.insert_queries import *

default_args= {
    'owner': 'Estudiante',
    'email_on_failure': False,
    'email': ['estudiante@uniandes.edu.co'],
    'start_date': datetime(2022, 5, 5) # inicio de ejecución
}

with DAG(
    "ETL",
    description='ETL',
    schedule_interval='@daily', # ejecución diaría del DAG
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1 - preprocesamiento
    preprocesamiento = PythonOperator(
        task_id='preprocesamiento',
        python_callable=procesar_datos
    )

    # task: 2 crear las tablas en la base de datos postgres
    crear_tablas_db = PostgresOperator(
    task_id="crear_tablas_en_postgres",
    postgres_conn_id="postgres_localhost", # Nótese que es el mismo ID definido en la conexión Postgres de la interfaz de Airflow
    sql= crear_tablas()
    )

    # task: 3 poblar las tablas de dimensiones en la base de datos
    with TaskGroup('poblar_tablas') as poblar_tablas_dimensiones:

        # task: 3.1 poblar tabla city
        poblar_municipios = PostgresOperator(
            task_id="poblar_municipios",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_municipios(csv_path = "dimension_municipios")
        )

        # task: 3.2 poblar tabla customer
        poblar_recursos = PostgresOperator(
            task_id="poblar_recursos",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_recursos(csv_path ="dimension_recursos")
        )

        # task: 3.3 poblar tabla date
        poblar_fechas = PostgresOperator(
            task_id="poblar_fechas",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_fechas(csv_path = "dimension_fechas")
        )

    # task: 3.4 poblar tabla employee
    poblar_actividades_mineras = PostgresOperator(
        task_id="poblar_actividades_mineras",
        postgres_conn_id='postgres_localhost',
        sql=insert_query_actividades_mineras(csv_path = "dimension_actividades_mineras")
    )

    # task: 3.5 poblar tabla stock item
    poblar_homicidios = PostgresOperator(
        task_id="poblar_homicidios",
        postgres_conn_id='postgres_localhost',
        sql=insert_query_homicidios(csv_path = "dimension_homicidios")
    )



    # flujo de ejecución de las tareas  
    preprocesamiento >> crear_tablas_db >> poblar_tablas_dimensiones >> poblar_actividades_mineras >> poblar_homicidios
