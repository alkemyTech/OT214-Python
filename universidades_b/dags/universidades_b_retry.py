from datetime import datetime, timedelta
from os import getenv

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Set args
default_args = {
    'owner': 'alkymer',
    'schedule_interval': '@hourly',
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}


# Definir la función para conectarse a la base de datos
def connect_database():
    load_dotenv()
    conexion = psycopg2.connect(
        user=getenv('_PG_USER'),
        password=getenv('_PG_PASSWD'),
        host=getenv('_PG_HOST'),
        port=getenv('_PG_PORT'),
        database=getenv('_PG_DB'))

    cursor = conexion.cursor()

    cursor.close()
    conexion.close()


# Create DAG to connect to the database
with DAG(
    'connect_b',
    start_date=datetime(2022, 5, 1),
    description='DAG to connect to database with 5 retries',
    default_args=default_args,

) as dag:
    connection_task = PythonOperator(
        task_id='connect_database',
        python_callable=connect_database)

    connection_task
