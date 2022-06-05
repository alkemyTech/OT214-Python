from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# dag default arguments
default_arguments = {
    'owner': 'Maxi Cabrera',
    'start_date': datetime(2022, 5, 1, 00, 00),
    'description': "dag etl proccesed information to universitys",
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
        print('generate a connection whith db')
        print('national university of la pampa querie')
        print('inter-american open university querie')

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
