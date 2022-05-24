from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Define ETL functions:
# Here will be the extract function, which will get information 
# needed about Comahue and Salvador Universities by sql queries
def extract():
    pass


# Here will be the transform function, which will do data processing with pandas
def process():
    pass


# Here will be the load function, which will load processed data into S3
def load():
    pass


# Create DAG to execute ETL tasks
with DAG(
    'universidades_b',
    description='DAG performing ETL for B universities - Salvador and Comahue',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1)
) as dag:

    # PythonOperator will execute setted functions
    query_task = PythonOperator(task_id='extract',
                                python_callable=extract)

    process_task = PythonOperator(task_id='process',
                                  python_callable=process)

    load_task = PythonOperator(task_id='load_data',
                               python_callable=load)

    query_task >> process_task >> load_task
