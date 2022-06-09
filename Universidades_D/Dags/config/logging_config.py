import logging
import logging.config
import os

import yaml

# main configure for log from .yml file: set directories in path rute for .log outcome file


def logging_configuration(_path='/logging_config.yml'):
    _path_yml = os.path.dirname(__file__)+_path
    _path_log = os.path.dirname(__file__)+'/logging.log'
    try:
        with open(_path_yml, 'rt') as file:
            config = yaml.safe_load(file.read())
        if config['handlers']['log_handler']['filename'] != _path_log:
            config['handlers']['log_handler']['filename'] = _path_log
            with open(_path_yml, 'w') as file_w:
                file_w.write(str(config))
        logging.config.dictConfig(config)
        logger = logging.getLogger('ETL_DAG')
        return logger
    except EnvironmentError:
        logger = get_default_log()
        return logger

# change to default logging config if any case or error setting on configuration


def get_default_log():
    logger = logging.getLogger('ETL_DAG')
    logger.setLevel(logging.INFO)
    form = logging.Formatter('%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    path = os.path.dirname(os.path.abspath(__file__))+'/logging.log'
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(form)
    logger.addHandler(file_handler)
    return logger
