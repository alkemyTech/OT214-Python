import os
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from decouple import config

from config.logging_config import lg_connect, lg_process, lg_send, path

# dag default arguments and retries
default_arguments = {
    'owner': 'Maxi Cabrera',
    'start_date': datetime(2022, 5, 1, 00, 00),
    'description': "dag etl proccesed information to universitys",
    'retry_delay': timedelta(minutes=2)

}


# dag start run every hour
with DAG(
    dag_id='university_e',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_arguments,
)as dag:

    # function of connection
    def conn_query():
        database = config("_PG_DATABASE")
        user = config('_PG_USERNAME')
        password = config('_PG_PASSWORD')
        host = config('_PG_HOST')
        port = config('_PG_PORT')

        lg_connect.info('initializing DAG connect.')

        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://'
                                          f'{user}:{password}@{host}:'
                                          f'{port}/{database}')

        conn = engine.connect()
        lg_connect.info('Database connect successfuly.')

        # Db query
        # creating the path to sql files
        path_inter = f'{path}/sql/abierta_interamericana.sql'
        path_pampa = f'{path}/sql/nacional_de_la_pampa.sql'
        path_files = f'{path}/files'
        if not os.path.exists(path_files):
            os.makedirs(path_files)
            lg_connect.info('the files folder was created')

        # reading the content of sql file
        read_inter = open(path_inter, 'r', encoding='utf-8').read()
        read_pampa = open(path_pampa, 'r', encoding='utf-8').read()
        lg_connect.info('read files.')

        # generating query with pandas
        df_query_inter = pd.read_sql(read_inter, conn)
        df_query_pampa = pd.read_sql(read_pampa, conn)
        lg_connect.info('reading sql with pandas')

        # generating the csv files
        df_query_inter.to_csv(f'{path}/files'
                              '/universidad_abierta_interamericana.csv')
        df_query_pampa.to_csv(f'{path}/files'
                              '/universidad_nacional_de_la_pampa.csv')
        lg_connect.info('csv files created.')

    # process data function
    def proccess():
        lg_process.info('initializing DAG connect.')

    # function to send data process to s3
    def send():
        lg_send.info('initializing DAG send data.')

    # pythonoperator for function of connect and queries
    python_connect = PythonOperator(
        task_id="connect_queries",
        python_callable=conn_query,
        retries=5,
    )

    # pythonoperator for function to process data
    python_process = PythonOperator(
        task_id="process",
        python_callable=proccess,
    )

    # pythonoperator for function to send data to s3
    python_send = PythonOperator(
        task_id="send_to_up_cloud",
        python_callable=send,
    )

    python_connect >> python_process >> python_send
