import logging
import logging.config

import yaml


def logging_configuration(_path='./logging_config.yaml'):

    with open(_path, 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
        logger = logging.getLogger('__name__')

    return logger
