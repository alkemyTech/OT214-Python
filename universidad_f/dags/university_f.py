from email.policy import default
import os
from datetime import timedelta

from psycopg2 import connect

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists
import pandas as pd
import logging
import logging.config
import yaml



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
    def __init__(self, sql_paths="../sql/", csv_paths="../data/"):
        logger.info("Starting ETL process.")
        self.query_path = sql_paths
        self.data_path = csv_paths

    def _create_engine(self):
        logger.info("Getting URI from .env")
        try:
            self.engine = create_engine("postgresql://" + config("_PG_USER") +
                                        ":" + config("_PG_PASSWD") +
                                        "@" + config("_PG_HOST") +
                                        ":" + config("_PG_PORT") +
                                        "/" + config("_PG_DB"))
        except Exception as e:
            logger.warning("""Error connecting to database,
                            check .env file \n""" + str(e))

    def _save_csv(self, r):
        try:
            if not os.path.exists(self.data_path):
                os.mkdir(self.data_path)
        except Exception as e:
            logger.warning("Error creating ./data/ path \n" + str(e))

        try:
            for k, v in r.items():
                v.to_csv(self.data_path + k + ".csv")
        except Exception as e:
            logger.warning("Error saving data.csv \n" + str(e))

    def _database_status(self, engine):
        logger.info("Checking database status")

        try:
            if not bool(engine):
                raise ValueError("Database doesn't exists")
            return
        except Exception as e:
            logger.warning("""Error connecting to database,
                        check .env file \n""" + str(e))

    def _make_query(self):
        result = {}

        with self.connection as conn:
            for i in os.listdir(self.query_path):
                with open(self.query_path + i) as sql_file:
                    query = sql_file.read()
                    result[i] = pd.read_sql(query, conn)

        return result

    def extract(self):
        logger.info("Extracting data.")

        self._create_engine()

        logger.info("Connecting database")
        self.connection = self.engine.connect()

        self._database_status(self.connection)

        r = self._make_query()

        self._save_csv(r)


if __name__ == "__main__":
    with open("./config/logger.yaml") as logger_config:
        try:
            log_config = yaml.safe_load(logger_config.read())
            logging.config.dictConfig(log_config)
        except Exception as e:
            logging.info("Error loading logger configuration \n" + str(e))
            print(str(e))
    
        finally:
            logger_config.close()

    logger = logging.getLogger("dev")
    logger.handlers

    etl = ETL()

    etl.extract()

    
