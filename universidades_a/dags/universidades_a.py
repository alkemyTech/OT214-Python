import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from config.logging_config import setup_logging

default_args = {
    'email': ['matiaspariente@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    "schedule_interval": '@hourly'
}


def extract_sql():

    # function that will extract data with SQL querys
    pass


def transform_data():

    # function that will process data with Pandas
    pass


def load_data_flores():

    # function that will upload to S3 universidad de flores transform data
    pass


def load_data_villamaria():

    # function that will upload to S3 universida de Villa Maria transform data
    pass


# Initialization of the DAG for the etl process for universidades_a
with DAG(
    'ETL_universidades_a',
    description='ETL DAG for Universidad de Flores and Villa Maria',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 5, 31)
) as dag:

    # Operator that will extract data with SQL querys
    extract = PythonOperator(
        task_id='extract_sql_task',
        python_callable=extract_sql
        )

    # Operator that will process data with Pandas
    transform = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data
        )

    # Operator that will upload to S3 universidad de flores transform data
    load_flores = PythonOperator(
        task_id='load_data_flores_task',
        python_callable=load_data_flores
        )

    # Operator that will upload to S3 universida de Villa Maria transform data
    load_villamaria = PythonOperator(
        task_id='load_data_villamaria_task',
        python_callable=load_data_villamaria
        )

    extract >> transform >> [load_flores, load_villamaria]
