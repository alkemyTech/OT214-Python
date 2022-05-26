from datetime import datetime, timedelta

from decouple import config

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

# DAG default arguments dictionary, running hourly with 5 retries
# in case of connection issues.
default_args = {
    'owner': 'alkymer',
    'schedule_interval': '@hourly',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'tags': '[aceleracion]'
}

# Source DataBase properties dictionary to be injected as **kwargs
# on create_connection funct.
db_args = {
    'user': config('_PG_USER'),
    'passwd': config('_PG_PASSWD'),
    'host': config('_PG_HOST'),
    'port': config('_PG_PORT'),
    'db': config('_PG_DB')
}


# Function to create connection from source DataBase.
def create_connection(**kwargs):
    engine = create_engine(
        'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(**kwargs))
    conn = engine.connect()
    status = bool(conn)
    return status


# DAG to execute the connection on schedule. Dict with default_args injected.
with DAG(
    'university_C',
    start_date=datetime.today(),
    description='Create DATABASE connection for universities C with 5 retries',
    default_args=default_args,
) as dag:

    # PythonOperator to execute the extract_data function.
    # Dict with db_args injected.
    opr_create_connection = PythonOperator(
            task_id='create_conection',
            python_callable=create_connection,
            op_kwargs=db_args
    )

    opr_create_connection
