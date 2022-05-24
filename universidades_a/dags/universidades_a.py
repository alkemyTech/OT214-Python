from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Initialization of the DAG for the etl process for universidades_a
with DAG(
    'ETL_universidades_a',
    description='ETL DAG for Universidad de Flores and Villa Maria',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.now(),
) as dag:

    # Operator that will extract universidad de Flores data with SQL query
    extract_flores = DummyOperator(task_id='extract_SQLquery_flores')

    # Operator that will extract universidad de Villa Maria data with SQL query
    extract_villa_maria = DummyOperator(task_id='extract_SQLquery_villa_maria')

    # Operator that will process universidad de Flores data with Pandas
    transform_flores = DummyOperator(task_id='transform_pandas_flores')

    # Operator that will process universidad de Villa Maria data with Pandas
    transform_villa_maria = DummyOperator(
        task_id='transform_pandas_villa_maria')

    # Operator that will upload to S3 universidades_a transform data
    load_universidades_a = DummyOperator(task_id='load_S3_universidades_a')

    extract_flores >> transform_flores >> load_universidades_a
    extract_villa_maria >> transform_villa_maria >> load_universidades_a
