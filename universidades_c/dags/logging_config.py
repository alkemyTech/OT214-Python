import logging
import logging.config
from pathlib import Path

import yaml


def logging_configuration():
    path = Path('./OT214-Python/universidades_c/dags/config/logging_config.yaml')
    with open(testpath, 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
        logger = logging.getLogger('__name__')

    return logger
