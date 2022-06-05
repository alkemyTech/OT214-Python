import os
from config.logging_config import logging_configuration
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

# [END import_module]

# start configured log for notice processes and fails
log = logging_configuration()

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
    'retry_delay': timedelta(minutes=5),
    'scheduler_interval': timedelta(hours=1)
}


# [END default_args]
class ETL:

    # credentials for establish a database connection
    @staticmethod
    def __get_connection_credentials():
        try:
            load_dotenv()
        except Exception as e:
            log.error(f'environment file missing or wrong, check file: {e}')
        pg_user = os.getenv('_PG_USER')
        pg_passwd = os.getenv('_PG_PASSWD')
        pg_host = os.getenv('_PG_HOST')
        pg_port = os.getenv('_PG_PORT')
        pg_db = os.getenv('_PG_DB')
        return f'postgresql://{pg_user}:{pg_passwd}@{pg_host}:{pg_port}/{pg_db}'

    # getting engine for plug connection and test status
    @staticmethod
    def __create_connection():
        url = ''
        try:
            log.info('Dag starting extract task operation')
            url = ETL.__get_connection_credentials()
        except Exception as e:
            log.error(f'Connection to database fails, check credentials: {e}')
        engine = create_engine(url)
        return engine

    # collect the data from database into dataframes and then store to csv files
    @staticmethod
    def data_to_csv(connection, fn_utn, fn_tdf, sql1, sql2):
        df_utn = ''
        df_tdf = ''
        try:
            df_utn = pd.read_sql(sql1, con=connection)
            df_tdf = pd.read_sql(sql2, con=connection)
        except Exception as e:
            log.error(f'Pandas fails to read sql query: {e}')
        directory = 'files'
        parent_dir = os.path.dirname(__file__)
        path_f = os.path.join(parent_dir, directory + '/')
        try:
            os.makedirs(path_f, exist_ok=True)
            log.info(f'Directory "{directory}" created successfully')
        except OSError as error:
            log.error(f'Directory "{directory}" can not be created: {error}')
        try:
            path_utn = path_f + fn_utn
            path_tdf = path_f + fn_tdf
            df_utn.to_csv(path_utn, index=False)
            df_tdf.to_csv(path_tdf, index=False)
            log.info('data files csv created successfully')
        except Exception as e:
            log.error(f'Fails to create csv data files: {e}')

    # START extract function for get data from Database
    def extract(self, filename_utn, filename_tdf, **kwargs):
        connection = self.__create_connection()
        connection_status = connection.connect()
        connection_check = bool(connection_status)
        if connection_check:
            log.info('Connection to database successful')
        else:
            log.error('Connection to database fails')
        query_utn = kwargs['templates_dict']['query_utn']
        query_tdf = kwargs['templates_dict']['query_tdf']
        try:
            ETL.data_to_csv(connection, filename_utn, filename_tdf, query_utn, query_tdf)
            log.info('Dag extract task completed')
        except Exception as e:
            log.critical(f'database error runtime query passed: {e}')

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


path_sql = './airflow/sql'

with DAG(
        'ETL_Universities_D',
        default_args=default_args,
        description='ETL DAG for University D data',
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 1, 1),
        template_searchpath=[path_sql]

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
        op_kwargs={'filename_utn': 'universidad_tecnologica_nacional.csv',
                   'filename_tdf': 'universidad_tres_de_febrero.csv'},
        templates_dict={'query_utn': 'utn.sql',
                        'query_tdf': 'tres_de_febrero.sql'},
        templates_exts=['.sql', ],
        python_callable=ETL_DAG.extract,
        dag=dag
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
