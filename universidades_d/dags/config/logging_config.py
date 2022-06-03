import logging
import logging.config
import os

import yaml


def logging_configuration(_path='/logging_config.yml'):
    _path_yml = os.path.dirname(__file__)+_path
    try:
        with open(_path_yml, 'rt') as file:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config)
            logger = logging.getLogger('ETL_DAG')
            return logger
    except EnvironmentError:
        logger = get_default_log()
        return logger


def get_default_log():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    return logger
