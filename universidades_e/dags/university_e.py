from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from config.logging_config import lg_connect, lg_process, lg_send

# dag default arguments and retries
default_arguments = {
    'owner': 'Maxi Cabrera',
    'start_date': datetime(2022, 5, 1, 00, 00),
    'description': "dag etl proccesed information to universitys e",
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
        lg_connect.info('initializing DAG connect.')

    # process data function
    def proccess():
        lg_process.info('initializing DAG process.')

        # process data from the national university of pampa
        def u_nacional_la_pampa():
            lg_process.info('initializing process whith '
                            'national university of the pampa.')

            """
            process and normalize the data from csv files
            of the national university of pampa
            with pandas and generate the txt file.

            """

            lg_process.info('txt file for national university '
                            'of the pampa create succesfuly')

        # process data from the Inter-American Open University
        def interamerican_open_u():
            lg_process.info('initializing process whith'
                            'Inter-American Open University.')

            """
            process and normalize the data from csv files
            of the Inter-American Open University
            with pandas and gnerate de txt file.

            """

            lg_process.info('txt file for Inter-American Open '
                            'University create succesfuly')

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
