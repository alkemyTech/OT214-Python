from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# DAG default arguments dictionary, running hourly with 5 retries.
default_args = {
    'owner': 'alkimer',
    'schedule_interval': '@hourly',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'tags': '[aceleracion]'
}


# Function to Extract data from source DataBase.
def extract_data():
    pass


# Function to Transform the data extracted.
def transform_data():
    pass


# Function to upload the data to S3.
def upload_data():
    pass


# DAG to execute the ETL on schedule. Dict with default_args injected.
with DAG(
    'university_C',
    start_date=datetime.today(),
    description='ETL for universities: Palermo, Jujuy',
    default_args=default_args,
) as dag:

    # PythonOperator to execute the extract_data function.
    opr_extract_data = PythonOperator(
            task_id='extract_data',
            python_callable=extract_data,
    )

    # PythonOperator to execute the transform_data function.
    opr_transform_data = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
    )
    # PythonOperator to execute the upload_data function.
    opr_upload_data = PythonOperator(
            task_id='upload_data',
            python_callable=upload_data,
    )

    opr_extract_data >> opr_transform_data >> opr_upload_data
