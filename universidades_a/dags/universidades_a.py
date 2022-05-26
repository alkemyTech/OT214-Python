from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'email_on_retry': ['matiaspariente@hotmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Initialization of the DAG for the etl process for universidades_a
with DAG(
    'ETL_universidades_a',
    description='ETL DAG for Universidad de Flores and Villa Maria',
    detault_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime.now(),
) as dag:

    # Operator that will extract universidad de Flores data with SQL query
    extract_flores = DummyOperator(
        task_id='extract_SQLquery_flores',
        retries=5
        )

    # Operator that will extract universidad de Villa Maria data with SQL query
    extract_villa_maria = DummyOperator(
        task_id='extract_SQLquery_villa_maria',
        retries=5
        )
