from datetime import timedelta

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

from decouple import config

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
    def _database_status(self, engine):
        if not database_exists(engine) or not bool(engine):
            return False
        return True

    def extract(self):
        engine = create_engine("postgresql://" + config("USER") +
                               ":" + config("PASSWORD") +
                               "@" + config("HOST") +
                               ":" + config("PORT") +
                               "/" + config("DATABASE"))

        if not self._database_status(engine):
            raise ValueError("Database doesn't exists")
