
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from config_logger import get_logger
import pandas as pd
import os

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
    def __init__(self, sql_paths="../sql/",
                 csv_paths="../files/",
                 logger_config="dev",
                 utils_paths="../utils/"):
        self.logger = get_logger(logger_config)

        self.logger.info("Starting ETL process.")
        self.query_path = sql_paths
        self.data_path = csv_paths
        self.utils_path = utils_paths

    # public function that will be in charge of the
    # extraction through data queries hosted in AWS
    def extract(self):
        pass

    def _save_txt(self, df):
        df.to_csv("../data/universities_f.txt", sep=';')

    def _append_dataframe(self, df_r):
        df = pd.DataFrame()

        for i in df_r:
            df = df.append(i, ignore_index = True)

        return df

    def _get_columns(self, df, columns):
        return df[columns]

    def _get_location(self):
        df_pc = pd.read_csv(self.utils_path + "codigos_postales.csv")

        return df_pc

    def _postal_code_location(self, df, location_column="location",
                              pc_column="postal_code"):
        df_pc = self._get_location()

        columns = {"codigo_postal": "postal_code", "localidad": "location"}

        df_pc = self._make_columns(df_pc, columns)
        df_pc = self._clear_rows(df_pc, ["location"], {"postal_code": "str"})

        if "location" in df.columns:
            df_tp = pd.merge(right=df,
                             left=df_pc[[location_column, pc_column]],
                             on=location_column)
            return df_tp

        df_tp = pd.merge(right=df,
                         left=df_pc[[location_column, pc_column]],
                         on=pc_column)

        return df_tp

    def _get_age(self, df, age_column="age", birthday_column="birthday"):
        self.logger.info("Getting age.")
        df[age_column] = ((datetime.now() - df[birthday_column]) / 365).dt.days

        return df

    def _get_gender(self, df, gender_column="gender"):
        self.logger.info("Getting gender.")
        df[gender_column] = df[gender_column].apply(lambda x:
                                                    "male" if x == "M" else
                                                    "female")

        return df

    def _make_name(self, df, name_column="name", names_columns=["first_name",
                                                                "last_name"]):
        self.logger.info("Getting names.")
        df[names_columns[0]] = df[name_column].apply(lambda x:
                                                     (x.split(" "))[0])

        df[names_columns[1]] = df[name_column].apply(lambda x:
                                                     (x.split(" "))[1])

        return df

    def _clean_rows(self, x, s_letters, g_letters):
        #self.logger.info("Translatings letters.")
        s_dictionary = x.maketrans(s_letters, g_letters)
        x = x.translate(s_dictionary)

        return x

    def _clear_rows(self, df, clear_columns, change_type, clear_email="email"):
        self.logger.info("Cleaning rows.")
        for i in clear_columns:
            try:
                if i not in df.columns:
                    continue
                df[i] = df[i].str.lower()

                if i != clear_email:
                    df[i] = df[i].apply(lambda x: self._clean_rows(x,
                                                                   "áéíóú-_",
                                                                   "aeiou  "))
                df[i] = df[i].str.strip()

            except Exception as e:
                self.logger.warning("Error cleaning rows. \n" + str(e))

        for k, v in change_type.items():
            try:
                if k not in df.columns:
                    continue

                c = "coerce"
                if v == "datetime":
                    df[k] = df[k].apply(lambda x: pd.to_datetime(x, errors=c))
                    continue

                df[k] = df[k].astype(v)

            except Exception as e:
                self.logger.warning("Error changing type of columns \n" +
                                    str(e))

        return df

    def _make_columns(self, df, columns):
        self.logger.info("Changing name of columns")
        df = df.rename(columns=columns)

        return df

    def _get_files_csv(self):
        self.logger.info("Getting csv files to transform")

        df_r = []

        for i in [i for i in os.listdir(self.data_path) if
                  os.path.isfile(self.data_path + i) and i.endswith(".csv")]:
            df = pd.read_csv(self.data_path + i)

            df_r.append(df)

        return df_r

    # public function that will be in charge of data analysis
    # using pandas of raw data extracted from the database
    def transform(self):
        df_r = self._get_files_csv()

        df_new = []

        columns = {"names": "name",
                   "sexo": "gender",
                   "eemail": "email",
                   "nombrre": "name",
                   "carrera": "carrer",
                   "carrerra": "carrer",
                   "direccion": "address",
                   "localidad": "location",
                   "nacimiento": "birthday",
                   "direcciones": "address",
                   "universidad": "university",
                   "codgoposstal": "postal_code",
                   "univiersities": "university",
                   "fechas_nacimiento": "birthday",
                   "fechaiscripccion": "inscription_date",
                   "inscription_dates": "inscription_date"}

        change_type = {"age": "int",
                       "postal_code": "str",
                       "birthday": "datetime",
                       "inscription_date": "datetime"}

        for df in df_r:
            df = self._make_columns(df, columns)
            df = self._clear_rows(df, clear_columns=["name",
                                                     "email",
                                                     "carrer",
                                                     "location",
                                                     "university"],
                                  change_type=change_type)
            df = self._make_name(df)
            df = self._get_gender(df)
            df = self._get_age(df)
            df = self._postal_code_location(df)
            df = self._get_columns(df, columns=["age",
                                                "email",
                                                'gender',
                                                "carrer",
                                                'location',
                                                "last_name",
                                                "first_name",
                                                "university",
                                                "postal_code",
                                                "inscription_date"])
            df_new.append(df)

        df = self._append_dataframe(df_new)

        self._save_txt(df)

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
