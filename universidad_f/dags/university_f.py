from datetime import timedelta

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

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
        engine = create_engine("postgresql://" + config("_PG_USER") +
                               ":" + config("_PG_PASSWD") +
                               "@" + config("_PG_HOST") +
                               ":" + config("_PG_PORT") +
                               "/" + config("_PG_DB"))

        if not self._database_status(engine):
            raise ValueError("Database doesn't exists")
