from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


with DAG(
    'ETL_branch_university_D',
    description='ETL for processing UTN and TresDeFebrero university data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.now(),
) as dag:
    SQL_extract_1 = DummyOperator(task_id='SQL_Query_UTN')
    SQL_extract_2 = DummyOperator(task_id='SQL_Query_TresDeFebrero')
    # here will run SQL Queries to fetch all data required for further operations
    Transform = DummyOperator(task_id='Data_Processing_Pandas')
    # get the collected data and executes processes for transformation
    Load = DummyOperator(task_id='Storage_AWS_S3')
    # submit the data collection to S3 repository

    [SQL_extract_1,SQL_extract_2] >> Transform >> Load