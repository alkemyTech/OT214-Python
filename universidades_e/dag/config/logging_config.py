import logging
import logging.config

import yaml

# configuring logging with yaml file
with open('logging_config.yaml', 'r') as archive:
    config = yaml.safe_load(archive.read())
    logging.config.dictConfig(config)

# Create logger for each task
lg_connect = logging.getLogger('task_connect')
lg_process = logging.getLogger('task_process')
lg_send = logging.getLogger('task_send')
