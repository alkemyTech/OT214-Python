from airflow import DAG
from datetime import timedelta, datetime 
from airflow.operators.python_operator import PythonOperator


# Postgresql server connection info
source_db_properties = {
                'user':'alkymer',
                'passwd':'alkymer123',
                'host':'training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com',
                'port': '5432',
                'db':'training'
}

# DAG default arguments, running hourly with 3 retries
default_args = {
    'owner': 'alkimer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval':'@hourly',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


# Class to contain the functions required for the ETL process.
class ETL():

    # Extracts data from source DataBase
    def extract_data():
        pass
        
    # Cleans and shapes the data extracted. 
    def transform_data():    
        pass

    # Uploads cleaned data to S3
    def upload_data():
        pass
    
        
# DAG to execute the ETL on schedule
with DAG(
    'university_C',
    start_date=datetime.today(),
    description='ETL for universities: Palermo, Jujuy',
    default_args=default_args,    
) as dag:

    etl = ETL()

    opr_extract_data = PythonOperator(
            task_id='extract_data',
            python_callable = etl.extract_data,
    )

    opr_transform_data = PythonOperator(
            task_id='transform_data',
            python_callable=etl.transform_data,
    )

    opr_upload_data = PythonOperator(
            task_id='upload_data',
            python_callable=etl.upload_data,
    )

    opr_extract_data >> opr_transform_data >> opr_upload_data