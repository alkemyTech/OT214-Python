import logging.config
import yaml

with open("./config/logger.yaml") as logger_config:
    config = yaml.load(logger_config, Loader=yaml.FullLoader)
    logging.config.dictConfig(config)

    logger_config.close()