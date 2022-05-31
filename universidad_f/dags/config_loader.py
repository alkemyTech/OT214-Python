import logging
import logging.config
import yaml


def get_logger(logger="dev"):
    try:
        with open("./config/logger.yaml") as logger_config:
            try:
                log_config = yaml.safe_load(logger_config.read())
                logging.config.dictConfig(log_config)
            except Exception as e:
                logging.info("Error loading logger configuration \n" + str(e))
                logging.basicConfig(level=logging.INFO)
            finally:
                logger_config.close()
    except Exception as e:
        logging.info("Error, logger's file configuration not found \n" +
                     str(e))
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(logger)

    return logger
