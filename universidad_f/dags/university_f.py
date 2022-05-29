import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from decouple import config
from sqlalchemy import create_engine

from config_logger import get_logger


default_args = {
    "owner": "alkymer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": '@hourly'
}


class ETL():
    def load(self):
        self.logger.info("Stating load process.")

    def transform(self):
        self.logger.info("Starting transform process")

    def _delete_exists(self, r):
        self.logger.info("Deleting data used.")

        for k, v in r.items():
            os.remove(self.data_path + k + ".csv")

    def _create_engine(self):
        self.logger.info("Getting URI from .env")
        try:
            self.engine = create_engine("postgresql://" + config("_PG_USER") +
                                        ":" + config("_PG_PASSWD") +
                                        "@" + config("_PG_HOST") +
                                        ":" + config("_PG_PORT") +
                                        "/" + config("_PG_DB"))
        except Exception as e:
            self.logger.warning("Error connecting to database, check .env file"
                                + "\n" + str(e))

    def _save_csv(self, r):
        self.logger.info("Saving data.csv files.")

        try:
            if not os.path.exists(self.data_path):
                os.mkdir(self.data_path)
        except Exception as e:
            self.logger.warning("Error creating ./data/ path \n" + str(e))

        try:
            for k, v in r.items():
                v.to_csv(self.data_path + k + ".csv")
        except Exception as e:
            self.logger.warning("Error saving data.csv \n" + str(e))

    def _database_status(self, engine):
        self.logger.info("Checking database status.")

        try:
            if not bool(engine):
                raise ValueError("Database doesn't exists.")
            return
        except Exception as e:
            self.logger.warning("Error connecting to database, check .env file"
                                + "\n" + str(e))

    def _make_query(self):
        self.logger.info("Executing query.")

        result = {}

        try:
            with self.connection as conn:
                for i in os.listdir(self.query_path):
                    with open(self.query_path + i) as sql_file:
                        query = sql_file.read()
                        result[i] = pd.read_sql(query, conn)
        except Exception as e:
            self.logger.warning("Error executing queries \n" + str(e))
        return result

    def extract(self):
        self.logger.info("Extracting data.")
        try:
            self._create_engine()

            self.logger.info("Connecting database.")
            self.connection = self.engine.connect()

            self._database_status(self.connection)

            r = self._make_query()

            if len(r) == 0:
                self.logger.warning("Queries not found.")
                return

            self._delete_exists(r)

            self._save_csv(r)
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error("Error founded, stopping...")

    def __init__(self, sql_paths="../sql/",
                 csv_paths="../data/", logger_config="dev"):
        self.logger = get_logger(logger_config)

        self.logger.info("Starting ETL process.")
        self.query_path = sql_paths
        self.data_path = csv_paths


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

    # all operators will be compatible with all the universities found in
    # the queries to be reusable in case queries are added or simply changed
    sql_task = PythonOperator(task_id="extract",
                              python_callable=etl.extract)

    pandas_task = PythonOperator(task_id="transform",
                                 python_callable=etl.transform)

    save_task = PythonOperator(task_id="load", python_callable=etl.load)

    # the dag will allow the ETL process to be done for all
    # the universities that have queries, are in /university_f/sql
    # and the configuration files of their columns/rows are in ./config/
    sql_task >> pandas_task >> save_task
