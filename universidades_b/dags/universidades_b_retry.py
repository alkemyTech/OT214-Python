
import psycopg2
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from os import getenv
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
        user=getenv('DB_USERNAME'),
        password=getenv('DB_PASSWORD'),
        host=getenv('DB_HOST'),
        port=getenv('DB_PORT'),
        database=getenv('DB_DATABASE'))

    cursor = conexion.cursor()

    cursor.close()
    conexion.close()


# Create DAG to connect to the database
with DAG(
    'connect_b',
    start_date=datetime.today(),
    description='DAG to connect to database with 5 retries',
    default_args=default_args,

) as dag:
    connection_task = PythonOperator(
        task_id='connect_database',
        python_callable=connect_database)

    connection_task
