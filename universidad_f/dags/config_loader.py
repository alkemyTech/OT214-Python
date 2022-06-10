import logging
import logging.config
import os

import yaml

configuration_path = os.path.dirname(__file__) + "/config/"


def get_dataframe_config(columns_file="columns.yaml"):
    try:
        with open(configuration_path + columns_file) as column_config:
            df_config = yaml.safe_load(column_config.read())

    except Exception as e:
        logging.exception("Error loading dataframe's configuration" + str(e))

    finally:
        column_config.close()
        return df_config["dataframes_config"]


def _getting_default_logger():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    return logger


def get_logger(logger_file="logger.yaml",
               logger_name="dev"):
    try:
        with open(configuration_path + logger_file) as logger_config:
            try:
                log_config = yaml.safe_load(logger_config.read())
                logging.config.dictConfig(log_config)
            except Exception:
                logging.exception("Error loading logger configuration")
                return _getting_default_logger()
            finally:
                logger_config.close()
    except Exception:
        logging.exception("Error, logger's file configuration not found")
        return _getting_default_logger()

    logger = logging.getLogger(logger_name)

    return logger
