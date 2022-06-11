import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from decouple import config
from sqlalchemy import create_engine

from dags.config_loader import get_dataframe_config, get_logger

sys.path.insert(0, "./dags/OT214-Python/universidad_f")

default_args = {
    "owner": "alkymer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": '@hourly'
}

path_university_f = os.path.dirname(__file__)
path_university_f = str(Path(path_university_f).parents[0])


class ETL:

    def __init__(self, sql_paths="/sql/",
                 csv_paths=path_university_f + "/files/",
                 logger_config="dev",
                 utils_paths=path_university_f + "/utils/"):
        self.logger = get_logger(logger_file="logger.yaml",
                                 logger_name=logger_config)
        self.logger.info("Starting ETL process.")
        self.query_path = path_university_f + sql_paths
        self.data_path = csv_paths
        self.utils_path = utils_paths

    def _save_txt(self, df_r):
        self.logger.info("Saving data to txt.")

        # new data was saved in ./data/
        d_path = path_university_f + "/data/"

        if not os.path.exists(d_path):
            os.mkdir(d_path)

        for file_name, df in df_r.items():
            df.to_csv(d_path + file_name + ".txt", sep=';')

    # filter columns
    def _get_columns(self, df, columns):
        return df[columns]

    # get location dataframe
    def _get_location(self):
        df_pc = pd.read_csv(self.utils_path + "codigos_postales.csv")

        return df_pc

    # get location and postal code
    def _postal_code_location(self, df, location_column="location",
                              pc_column="postal_code"):
        df_pc = self._get_location()

        # new columns to merge
        columns = {"codigo_postal": "postal_code", "localidad": "location"}

        # get location
        df_pc = self._make_columns(df_pc, columns)
        df_pc = self._clear_rows(df_pc, ["location"], {"postal_code": "str"})

        if "location" in df.columns:
            df_tp = pd.merge(right=df,
                             left=df_pc[[location_column, pc_column]],
                             on=location_column)
            return df_tp

        # get postal code
        df_tp = pd.merge(right=df,
                         left=df_pc[[location_column, pc_column]],
                         on=pc_column)

        return df_tp

    # get age with (days_now - days_birthday) / days_year
    def _get_age(self, df, age_column="age", birthday_column="birthday"):
        self.logger.info("Getting age.")
        df[age_column] = ((datetime.now() - df[birthday_column]) / 365).dt.days

        df = df.fillna(0)

        df[age_column] = df[age_column].astype("int")

        return df

    # this function get gender
    def _get_gender(self, df, gender_column="gender"):
        self.logger.info("Getting gender.")
        df[gender_column] = df[gender_column].apply(lambda x:
                                                    "male" if x == "M" else
                                                    "female")

        return df

    # this function split name in firts_name and last_name
    def _make_name(self, df, name_column="name", ):
        self.logger.info("Getting names.")
        names_columns = ["first_name",
                         "last_name"]
        df[names_columns[0]] = df[name_column].apply(lambda x:
                                                     (x.split(" "))[0])

        df[names_columns[1]] = df[name_column].apply(lambda x:
                                                     (x.split(" "))[1])

        return df

    def _clean_rows(self, x, s_letters, g_letters):
        s_dictionary = x.maketrans(s_letters, g_letters)
        x = x.translate(s_dictionary)

        return x

    def _clear_rows(self, df, clear_columns, change_type, clear_email="email"):
        self.logger.info("Cleaning rows.")

        # are the columns to be subjected to lower,
        # strip, and removed from underscore and overscore
        for column in clear_columns:
            try:
                if column not in df.columns:
                    continue
                df[column] = df[column].str.lower()

                # the email column is not removed from the
                # underscore and overscore so as not to generate errors
                if column != clear_email:
                    df[column] = df[column].apply(lambda x:
                                                  self._clean_rows(x,
                                                                   "áéíóú-_",
                                                                   "aeiou  "))
                df[column] = df[column].str.strip()

            except Exception as e:
                self.logger.warning("Error cleaning rows. \n" + str(e))

        # change type handles changing columns of type from a dictionary
        # {column:new_type}
        for column, new_type in change_type.items():
            try:
                if column not in df.columns:
                    continue

                if new_type == "datetime":
                    df[column] = df[column].apply(lambda x: pd.to_datetime(x,
                                                  errors="coerce"))
                    continue

                df[column] = df[column].astype(new_type)

            except Exception as e:
                self.logger.warning("Error changing type of columns \n" +
                                    str(e))

        return df

    # Change columns names with config.yaml file
    def _make_columns(self, df, columns):
        self.logger.info("Changing name of columns")
        df = df.rename(columns=columns)

        return df

    def _get_files_csv(self):
        self.logger.info("Getting csv files to transform")

        self.file_names = []

        # list of dataframes
        df_r = []

        for i in [i for i in os.listdir(self.data_path) if
                  os.path.isfile(self.data_path + i) and i.endswith(".csv")]:

            df = pd.read_csv(self.data_path + i)

            # save file name
            self.file_names.append(i.split(".")[0])

            df_r.append(df)

        return df_r

    def load(self):
        self.logger.info("Stating load process.")

    # public function that will be in charge of data analysis
    # using pandas of raw data extracted from the database
    def transform(self):

        # df_r list of dataframes get from ./files
        df_r = self._get_files_csv()

        # get dataframe after clean
        df_new = []

        # get columns's config from ./config/columns.yaml
        df_config = get_dataframe_config()

        for df in df_r:
            df = self._make_columns(df, df_config["columns"])
            df = self._clear_rows(df, df_config["clear_columns"],
                                  change_type=df_config["change_type"])
            df = self._make_name(df)
            df = self._get_gender(df)
            df = self._get_age(df)
            df = self._postal_code_location(df)
            df = self._get_columns(df, df_config["final_columns"])
            df_new.append(df)

        # dataframe dictionary {file_name:df}
        df_f = dict(zip(self.file_names, df_new))

        self._save_txt(df_f)

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
            self.engine.connect()

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
