from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# dag default arguments
default_arguments = {
    'owner': 'Maxi Cabrera',
    'start_date': days_ago(1)
}


# dag start run every hour
with DAG(
    dag_id='create_dag_structure',
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
    python_task1 = PythonOperator(
        task_id="connect_queries",
        python_callable=conn_query,
    )

    # pythonoperator for function to process data
    python_task2 = PythonOperator(
        task_id="process",
        python_callable=proccess,
    )

    # pythonoperator for function to send data to s3
    python_task3 = PythonOperator(
        task_id="send_to_up_cloud",
        python_callable=send,
    )

python_task1 >> python_task2 >> python_task3
