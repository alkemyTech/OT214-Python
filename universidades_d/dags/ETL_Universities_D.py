import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# [END import_module]

# [START default_args]
# These args will get pass on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'alkymer',
    'depends_on_past': False,
    'email': ['juan.i.elizondo@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=2),
    'scheduler_interval': timedelta(hours=1)
}


# credentials for establish a database connection
def get_connection_credentials():
    load_dotenv()
    _PG_USER = os.getenv('_PG_USER')
    _PG_PASSWD = os.getenv('_PG_PASSWD')
    _PG_HOST = os.getenv('_PG_HOST')
    _PG_PORT = os.getenv('_PG_PORT')
    _PG_DB = os.getenv('_PG_DB')
    return f'postgresql://{_PG_USER}:{_PG_PASSWD}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}'


# [END default_args]
class ETL:

    # getting engine for plug connection and test status
    def create_connection(self):
        url = get_connection_credentials()
        engine = create_engine(url)
        return engine

    # START extract function for get data from Database
    def extract(self):
        connection = self.create_connection()
        connection_status = connection.connect()
        connection_check = bool(connection_status)
        if connection_check is False or not connection:
            raise ValueError('Connection to database fails')

    # list of action and process required to this function

    # END extract function

    #  START transform function for data extracted
    def transform(self):
        pass

    # operations needed to manage data

    # END transform function

    #  START load function for store data into S3 repository

    def load(self):
        pass
    # link this task to target data storage

    # END load function

    #  START instantiate_dag


with DAG(
        'ETL_universidades_D',
        default_args=default_args,
        description='ETL DAG for University D data',
        schedule_interval=timedelta(hours=1),
        start_date=datetime.today(),

) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # create object ETL for process
    ETL_DAG = ETL()
    # Execute PythonOperator to the extract function
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=ETL_DAG.extract,
    )

    # Execute PythonOperator to the transform function
    transform_data_task = PythonOperator(
        task_id='transform',
        python_callable=ETL_DAG.transform,
    )

    # Execute PythonOperator to the load function
    load_data_task = PythonOperator(
        task_id='load',
        python_callable=ETL_DAG.load,
    )

    extract_data_task >> transform_data_task >> load_data_task