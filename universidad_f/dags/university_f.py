import os
from datetime import datetime, timedelta
import sys
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.insert(0,"./dags/OT214-Python/universidad_f")
from dags.config_loader import get_logger
from decouple import config
from sqlalchemy import create_engine

# Arguments to be used by the dag by default
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

    # Delete last .csv file created if exists
    def _delete_exists(self, queries_file):
        self.logger.info("Deleting data used.")

        try:
            for name_queries, _ in queries_file.items():
                name_file = name_queries.split(".")[0]
                os.remove(self.data_path + name_file + ".csv")
        except Exception as e:
            self.logger.warning("Data already used not found" + str(e))

    def _create_engine(self):
        self.logger.info("Getting URI from .env")
        try:
            # Create engine to make query
            self.engine = create_engine("postgresql://" + config("_PG_USER") +
                                        ":" + config("_PG_PASSWD") +
                                        "@" + config("_PG_HOST") +
                                        ":" + config("_PG_PORT") +
                                        "/" + config("_PG_DB"))
        except Exception as e:
            # if not found .env with configuration
            self.logger.warning("Error connecting to database, check .env file"
                                + "\n" + str(e))

    # Save .csv and make .data columns
    def _save_csv(self, queries_file):
        self.logger.info("Saving data.csv files.")

        try:
            # create data path
            if not os.path.exists(self.data_path):
                os.mkdir(self.data_path)
        except Exception as e:
            # permission error with mkdir
            self.logger.warning("Error creating ./data/ path \n" + str(e))

        try:
            # create .csv with name and df
            for name_query, df in queries_file.items():
                name_file = name_query.split(".")[0]

                df.to_csv(self.data_path + name_file + ".csv")
        except Exception as e:
            self.logger.warning("Error saving data.csv \n" + str(e))

    # Check database status
    def _database_status(self):
        self.logger.info("Checking database status.")

        try:
            if not bool(self.engine):
                raise ValueError("Database doesn't exists.")
            return
        except Exception as e:
            self.logger.warning("Error connecting to database, check .env file"
                                + "\n" + str(e))

    def _make_query(self):
        self.logger.info("Executing query.")

        # result get {name_file:query_result}
        result = {}

        try:
            with self.connection as conn:
                for file_name in os.listdir(self.query_path):
                    with open(self.query_path + file_name) as sql_file:
                        query = sql_file.read()
                        result[file_name] = pd.read_sql(query, conn)
        except Exception as e:
            self.logger.warning("Error executing queries \n" + str(e))

        return result

    def extract(self):
        self.logger.info("Extracting data.")
        try:
            self._create_engine()

            self.logger.info("Connecting database.")
            self.connection = self.engine.connect()

            self._database_status()

            # get result queries from engine
            result_queries = self._make_query()

            # check if queries exists or return []
            if len(result_queries) == 0:
                self.logger.warning("Queries not found.")
                return

            # delete data files if exists
            self._delete_exists(result_queries)

            # save .csv
            self._save_csv(result_queries)
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error("Error founded, stopping...")

    def __init__(self, sql_paths="/sql/",
                 csv_paths="/files/", logger_config="dev"):
        self.logger = get_logger(logger_name=logger_config)

        # get path
        path_university_f = os.path.dirname(__file__)
        path_university_f = str(Path(path_university_f).parents[0])
        self.logger.info("Starting ETL process.")
        self.query_path = path_university_f + sql_paths
        self.data_path = path_university_f + csv_paths


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
