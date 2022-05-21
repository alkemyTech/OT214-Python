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

    # public function that will be in charge of the
    # extraction through data queries hosted in AWS
    def extract(self):
        pass

    # public function that will be in charge of data analysis
    # using pandas of raw data extracted from the database
    def transform(self):
        pass

    # public function that will be in charge of loading the information
    # later to be analyzed, cleaned and processed to a database for later use
    def load(self):
        pass



