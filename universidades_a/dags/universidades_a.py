import logging
from pathlib import Path

import pandas as pd
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def extract_sql():
    '''
    Universities Data is extracted from the database with the saved queries.
    This Data is saved in a csv file corresponding to each universities
    '''

    try:
        engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}"
            .format(
                config('_PG_USER'),
                config('_PG_PASSWD'),
                config('_PG_HOST'),
                config('_PG_PORT'),
                config('_PG_DB')))
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        if "port" in error:
            logging.critical("Comunication Error, verify HOST:PORT")
        else:
            logging.critical("Autentication error, verify User / password")
    else:
        logging.info("Database connection success")

    try:
        filepath_flores = Path(
            'airflow/universidades_a/sql/flores.sql')
        filepath_villamaria = Path(
            'airflow/universidades_a/sql/villaMaria.sql')
        with open(filepath_flores, 'r', encoding="utf-8") as file:
            query_flores = file.read()
        with open(filepath_villamaria, 'r', encoding="utf-8") as file:
            query_villamaria = file.read()
        df_flores = pd.read_sql(query_flores, engine)
        df_villamaria = pd.read_sql(query_villamaria, engine)
    except IOError:
        logging.error("SQL file not appear or exist")
    else:
        logging.info("SQL query reading success")

    try:
        filepath_flores_csv = Path(
            'airflow/universidades_a/dags/files/universidad_flores.csv')
        filepath_flores_csv.parent.mkdir(parents=True, exist_ok=True)
        filepath_villamaria_csv = Path(
            'airflow/universidades_a/dags/files/universidad_villamaria.csv')
        filepath_villamaria_csv.parent.mkdir(parents=True, exist_ok=True)
        df_flores.to_csv(filepath_flores_csv, index=False)
        df_villamaria.to_csv(filepath_villamaria_csv, index=False)
    except Exception as exc:
        logging.error(exc)
    else:
        logging.info("csv files were generated successfully")