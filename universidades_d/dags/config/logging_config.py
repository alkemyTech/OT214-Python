import logging
import logging.config

import yaml


def logging_configuration(_path='./logging_config.yaml'):

    with open(_path, 'rt') as file:
        config = yaml.safe_load(file.read())
        logging.config.dictConfig(config)
        logger = logging.getLogger('ETL_DAG')
        return logger
