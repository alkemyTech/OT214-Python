import logging
import logging.config
import os

import yaml

path = "./dags/OT214-Python/universidades_e/dags"
# configuring logging with yaml file
with open(f'{path}/config/logging_config.yaml', 'r') as archive:
    config = yaml.safe_load(archive.read())
    logging.config.dictConfig(config)

# Create logger for each task
lg_connect = logging.getLogger('task_connect')
lg_process = logging.getLogger('task_process')
lg_send = logging.getLogger('task_send')
