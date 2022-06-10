import logging
from datetime import datetime, timedelta
from pathlib import Path

from decouple import config
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd


default_args = {
    'email': ['matiaspariente@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    "schedule_interval": '@hourly'
}


def extract_sql():
    '''
    Universities Data is extracted from the database with the saved queries.
    This Data is saved in a csv file corresponding to each universities
    '''
    filepath_universidades_a = Path(__file__).parents[1]
    logger = logging.getLogger("Extract")
    try:
        engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}"
            .format(
                config('_PG_USER'),
                config('_PG_PASSWD'),
                config('_PG_HOST'),
                config('_PG_PORT'),
                config('_PG_DB')))
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        if "port" in error:
            logger.critical("Comunication Error, verify HOST:PORT")
        else:
            logger.critical("Autentication error, verify User / password")
    else:
        logger.info("Database connection success")

    try:
        filepath_flores = Path(
            filepath_universidades_a,
            'sql/flores.sql')
        filepath_villamaria = Path(
            filepath_universidades_a,
            'sql/villaMaria.sql')
        with open(filepath_flores, 'r', encoding="utf-8") as file:
            query_flores = file.read()
        with open(filepath_villamaria, 'r', encoding="utf-8") as file:
            query_villamaria = file.read()
        df_flores = pd.read_sql(query_flores, engine)
        df_villamaria = pd.read_sql(query_villamaria, engine)
    except IOError:
        logger.error("SQL file not appear or exist")
    else:
        logger.info("SQL query reading success")

    try:
        filepath_flores_csv = Path(
            filepath_universidades_a,
            'dags/files/universidad_flores.csv')
        filepath_flores_csv.parent.mkdir(parents=True, exist_ok=True)
        filepath_villamaria_csv = Path(
            filepath_universidades_a,
            'dags/files/universidad_villamaria.csv')
        filepath_villamaria_csv.parent.mkdir(parents=True, exist_ok=True)
        df_flores.to_csv(filepath_flores_csv, index=False)
        df_villamaria.to_csv(filepath_villamaria_csv, index=False)
    except Exception as exc:
        logger.error(exc)
    else:
        logger.info("csv files were generated successfully")


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
