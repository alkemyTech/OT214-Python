from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import timedelta, datetime


# Arguments to be used by the dag by default
default_args = {
    "owner": "alkymer",
    "depends_on_past": False,
    "email": ["example@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(hours=1)
}


# It will contain the functions of the dag
class ETL():
    # will initialize configurations for the ETL
    def __init__(self):
        self.config = {}

    # public function that will be in charge of the Moron university's
    # extraction through data queries hosted in AWS
    def extract_moron(self):
        pass

    # public function that will be in charge of the Rio Cuarto university's
    # extraction through data queries hosted in AWS
    def extract_rc(self):
        pass

    # public function that will be in charge of data analysis
    # using pandas of raw data extracted from the database
    def transform(self):
        pass

    # public function that will be in charge of loading the information
    # later to be analyzed, cleaned and processed to a database for later use
    def load(self):
        pass


# The DAG that will be in charge of managing the ETL functions
with DAG(
    "university_f",
    default_args=default_args,
    description="""Dag to extract, process and load data
                  from Moron and Rio Cuarto's Universities""",
    schedule_interval=timedelta(hours=1),
    start_date=datetime.now()
) as dag:
    # initialize the ETL configuration
    etl = ETL()

    # declare the operators
    sql_task_moron = PythonOperator(task_id="extract_moron",
                                    python_callable=etl.extract_moron)

    sql_task_rc = PythonOperator(task_id="extract_rc",
                                 python_callable=etl.extract_rc)

    pandas_task = PythonOperator(task_id="transform",
                                 python_callable=etl.transform)

    save_task = PythonOperator(task_id="load", python_callable=etl.load)

    [sql_task_moron, sql_task_rc] >> pandas_task >> save_task
