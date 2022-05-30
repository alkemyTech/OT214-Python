import logging
import os

import yaml


def config():
    root_path = os.path.dirname(__file__)
    config_path = os.path.join(root_path, 'logging_config.yaml')
    with open(config_path) as file:
        config = yaml.safe_load(file.read())
        logging.config.dictConfig(config)
        logger = logging.getLogger(__name__)

    return logger
