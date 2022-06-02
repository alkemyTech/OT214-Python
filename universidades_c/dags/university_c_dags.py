from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from decouple import config
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from logging_config import logging_configuration
from pandas_processing_dicts import jujuy_columns, palermo_columns
from S3operator import S3Operator

# DAG default arguments dictionary, running hourly with 5 retries.
default_args = {
    "owner": "alkymer",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "tags": "[aceleracion]",
}

# Source and destination DataBase properties dictionaries to be
# injected as **kwargs on extract_data and upload_data functions.
db_args = {
    "user": config("_PG_USER"),
    "passwd": config("_PG_PASSWD"),
    "host": config("_PG_HOST"),
    "port": config("_PG_PORT"),
    "db": config("_PG_DB"),
}

s3_kwargs = {
    'bucket': config('_BUCKET_NAME'),
    'p_key': config('_PUBLIC_KEY'),
    's_key': config('_SECRET_KEY')
}

_paths = {
    "query_palermo": [
        Path("./OT214-Python/universidades_c/sql/palermo.sql"),
        Path("./OT214-Python/universidades_c/files/universidad_palermo.csv"),
        Path("./OT214-Python/universidades_c/files/universidad_palermo.txt"),
        "Palermo",
    ],
    "query_jujuy": [
        Path("./OT214-Python/universidades_c/sql/jujuy.sql"),
        Path("./OT214-Python/universidades_c/files/universidad_jujuy.csv"),
        Path("./OT214-Python/universidades_c/files/universidad_jujuy.txt"),
        "Jujuy",
    ],
    "postal_codes": Path(
        "./OT214-Python/universidades_c/files/codigos_postales.csv"
    ),
}

s3_dict = {
    'Universidad_Palermo.txt': "./OT214-Python/universidades_c/files/universidad_palermo.txt",
    'Universidad_Jujuy.txt': "./OT214-Python/universidades_c/files/universidad_jujuy.txt"
    }

# Function injected to the transforming functions to calculate age.
def calc_age(dob):
    today = date.today()
    age = (
        today.year
        - dob.year
        - ((today.month, today.day) < (dob.month, dob.day))
    )
    return age


# Function to Extract data from source DataBase.
def extract_data(loggerfunc, paths_dict, **kwargs):
    loggr = loggerfunc()
    loggr.info("\nStarting ETL for universities Palermo and Jujuy.")
    engine = create_engine(
        "postgresql://{user}:{passwd}@{host}:{port}/{db}".format(**kwargs)
    )
    conn = engine.connect()
    status = bool(conn)
    if status is True:
        loggr.info("Conected to DataBase PostgreSQL")
    else:
        loggr.info("Retrying conection.")
        return status
    for key, item in paths_dict.items():
        if key != "postal_codes":
            sql_file = open(item[0])
            sql_as_string = sql_file.read()
            df = pd.read_sql(sql_as_string, conn)
            df.to_csv(item[1], index=False, encoding="utf-8")
            loggr.info(f"File for university {item[3]} created and written.")

    return status


# Function to transform data for university Palermo.
def palermo_transform_data(loggerfunc, paths_dict, columns_dict, age_func):
    loggr = loggerfunc()
    loggr.info("Starting data transformation for Universidad Palermo.")
    try:
        df_palermo = pd.read_csv(
            paths_dict["query_palermo"][1], index_col=False, encoding="utf-8"
        )
        loggr.info("universidad_palermo.csv file read successfuly.")
    except Exception:
        loggr.info(".csv file could not be read. Check .csv file path.")
        return False
    # Names column cleaning and splitting
    df_palermo["names"] = df_palermo["names"].replace(
        r"(\w+[.]_)", "", regex=True
    )
    df_palermo[["first_name", "last_name", "title"]] = df_palermo[
        "names"
    ].str.split("_", expand=True)
    df_palermo = df_palermo.drop(columns=["names", "title"])
    # Columns renaming
    df_palermo = df_palermo.rename(columns=columns_dict)
    # Column: "postal code" formatting.
    df_palermo["postal_code"] = df_palermo["postal_code"].astype("str")
    # Columns: "university", "career", "location" and "email" formatting.
    career_location_lst = ["university", "career", "location", "email"]
    for item in career_location_lst:
        if item != "email":
            df_palermo[item] = df_palermo[item].str.replace("_", " ")
        df_palermo[item] = df_palermo[item].str.replace(
            r"(\ $)", "", regex=True
        )
        df_palermo[item] = df_palermo[item].str.lower()
    # Column: inscription_date formatting
    df_palermo["inscription_date"] = pd.to_datetime(
        df_palermo["inscription_date"], format="%d/%b/%y"
    )
    df_palermo["inscription_date"] = df_palermo[
        "inscription_date"
    ].dt.strftime("%Y-%m-%d")
    # Column: "gender" formatting
    df_palermo["gender"] = df_palermo["gender"].map(
        {"m": "male", "f": "female"}
    )
    # Column: "age" formatting and calculation.
    df_palermo["age"] = pd.to_datetime(df_palermo["age"], format="%d/%b/%y")
    df_palermo["year"] = df_palermo["age"].dt.strftime("%y")
    # Considering minimum age for university inscription as 16 y/o
    # People with 16 y/o would have a DoB (2022 - 16) = 6
    df_palermo["year"] = pd.to_numeric(df_palermo["year"]).apply(
        lambda x: x + 1900 if x > 6 else x + 2000
    )
    df_palermo["year"] = df_palermo["year"].astype("str")
    df_palermo["age"] = df_palermo["age"].dt.strftime("%m/%d")
    df_palermo["age"] = df_palermo["year"] + "/" + df_palermo["age"]
    df_palermo["age"] = pd.to_datetime(df_palermo["age"], format="%Y/%m/%d")
    df_palermo["age"] = df_palermo["age"].apply(age_func)
    df_palermo = df_palermo.drop(columns="year")
    df_palermo = df_palermo[
        [
            "university",
            "career",
            "inscription_date",
            "first_name",
            "last_name",
            "gender",
            "age",
            "postal_code",
            "location",
            "email",
        ]
    ]
    #  Getting np array from dataframe
    array_palermo = df_palermo.to_numpy()
    # writing .txt file
    try:
        np.savetxt(
            paths_dict["query_palermo"][2],
            array_palermo,
            fmt="%s",
            header="university, career, inscription_date,\
                    first_name, last_name, gender, age, postal_code, location,\
                    email",
            delimiter=",",
        )
        loggr.info("Data for Universidad Palermo written to .txt file.")
    except Exception:
        loggr.info(".txt file for Universidad Palermo writting failed.")
        return False

    return True


# Function to transform data for university Jujuy.
def jujuy_transform_data(loggerfunc, paths_dict, columns_dict, age_func):
    loggr = loggerfunc()
    loggr.info("Starting data transformation for Universidad Jujuy.")
    try:
        df_jujuy_raw = pd.read_csv(
            paths_dict["query_jujuy"][1], index_col=False, encoding="utf-8"
        )
        df_jujuy_postal = pd.read_csv(
            paths_dict["postal_codes"], index_col=False, encoding="utf-8"
        )
        loggr.info("universidad_jujuy.csv file read successfuly.")
    except Exception:
        loggr.info(".csv file could not be read. Check .csv file path.")
        return False
    df_jujuy_postal["localidad"] = df_jujuy_postal["localidad"].str.lower()
    df_jujuy_postal = df_jujuy_postal.rename(columns={"localidad": "location"})
    df_jujuy = df_jujuy_raw.merge(df_jujuy_postal, how="left", on="location")
    df_jujuy = df_jujuy.drop(columns=["location"])
    # Nombre column cleaning and splitting
    df_jujuy["nombre"] = df_jujuy["nombre"].replace(
        r"(\w+[.] )", "", regex=True
    )
    df_jujuy[["first_name", "last_name", "title"]] = df_jujuy[
        "nombre"
    ].str.split(" ", expand=True)
    df_jujuy = df_jujuy.drop(columns=["nombre", "title"])
    # Columns renaming
    df_jujuy = df_jujuy.rename(columns=columns_dict)
    df_jujuy["postal_code"] = df_jujuy["postal_code"].astype("str")
    # Columns: "university", "career", "location" and "email" formatting.
    career_location_lst = ["university", "career", "location", "email"]
    for item in career_location_lst:
        if item != "email":
            df_jujuy[item] = df_jujuy[item].str.replace("_", " ")
            df_jujuy[item] = df_jujuy[item].str.replace(
                r"(\ $)", "", regex=True
            )
        df_jujuy[item] = df_jujuy[item].str.lower()
    # Column: inscription_date formatting
    df_jujuy["inscription_date"] = pd.to_datetime(
        df_jujuy["inscription_date"], format="%Y/%m/%d"
    )
    df_jujuy["inscription_date"] = df_jujuy["inscription_date"].dt.strftime(
        "%Y-%m-%d"
    )
    # Column: "gender" formatting
    df_jujuy["gender"] = df_jujuy["gender"].map({"m": "male", "f": "female"})
    # Column: "age" formatting and calculation.
    df_jujuy["age"] = pd.to_datetime(df_jujuy["age"], format="%Y-%m-%d")
    df_jujuy["year"] = df_jujuy["age"].dt.strftime("%y")
    # Considering minimum age for university inscription as 16 y/o
    # People with 16 y/o would have a DoB (2022 - 16) = 6
    df_jujuy["year"] = pd.to_numeric(df_jujuy["year"]).apply(
        lambda x: x + 1900 if x > 6 else x + 2000
    )
    df_jujuy["year"] = df_jujuy["year"].astype("str")
    df_jujuy["age"] = df_jujuy["age"].dt.strftime("%m/%d")
    df_jujuy["age"] = df_jujuy["year"] + "/" + df_jujuy["age"]
    df_jujuy["age"] = pd.to_datetime(df_jujuy["age"], format="%Y/%m/%d")
    df_jujuy["age"] = df_jujuy["age"].apply(age_func)
    df_jujuy = df_jujuy.drop(columns="year")
    df_jujuy = df_jujuy[
        [
            "university",
            "career",
            "inscription_date",
            "first_name",
            "last_name",
            "gender",
            "age",
            "postal_code",
            "location",
            "email",
        ]
    ]
    #  Getting np array from dataframe
    array_jujuy = df_jujuy.to_numpy()
    # writing .txt file
    try:
        np.savetxt(
            paths_dict["query_jujuy"][2],
            array_jujuy,
            fmt="%s",
            header="university, career, inscription_date,\
                    first_name, last_name, gender, age, postal_code, location,\
                    email",
            delimiter=",",
        )
        loggr.info("Data for Universidad Jujuy written to .txt file.")
    except Exception:
        loggr.info(".txt file for Universidad Jujuy writting failed.")
        return False

    return True


# DAG to execute the ETL on schedule. Dict with default_args injected.
with DAG(
    "university_C_3",
    start_date=datetime(2022, 6, 2),
    description="ETL for universities: Palermo, Jujuy",
    default_args=default_args,
    schedule_interval="@hourly",
) as dag:

    # PythonOperator to execute the extract_data function.
    opr_extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        op_args=[logging_configuration, _paths],
        op_kwargs=db_args,
    )

    opr_transform_palermo_data = PythonOperator(
        task_id="transform_palermo_data",
        python_callable=palermo_transform_data,
        op_args=[logging_configuration, _paths, palermo_columns, calc_age],
    )

    opr_transform_jujuy_data = PythonOperator(
        task_id="transform_jujuy_data",
        python_callable=jujuy_transform_data,
        op_args=[logging_configuration, _paths, jujuy_columns, calc_age],
    )

    join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_SKIPPED,
    )

    opr_upload_data = S3Operator(
        task_id='upload_data',
        names=s3_dict,
        loggerfunc=logging_configuration,
        op_kwargs=s3_kwargs
    )

    opr_extract_data >> [
        opr_transform_palermo_data,
        opr_transform_jujuy_data] >> join >> opr_upload_data
