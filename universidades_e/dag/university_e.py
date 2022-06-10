from datetime import datetime, timedelta

import sqlalchemy
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from decouple import config

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

    # function of connection and db queries
    def conn_query():
        database = config("_PG_DATABASE")
        user = config('_PG_USERNAME')
        password = config('_PG_PASSWORD')
        host = config('_PG_HOST')
        port = config('_PG_PORT')
        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://'
                                          f'{user}:{password}@{host}:'
                                          f'{port}/{database}')
        engine.connect()
        print('******** Database connect successfuly *********')

    # process data function
    def proccess():
        print('process data whith pandas')

    # function to send data process to s3
    def send():
        print('send of panda processed info to a3 server')

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
