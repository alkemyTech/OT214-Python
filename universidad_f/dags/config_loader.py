import logging
import logging.config
import yaml

configuration_path = "./config/"


def _getting_default_logger():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    return logger


def get_logger(logger_file="logger.yaml", logger_name="dev"):
    try:
        with open(configuration_path + logger_file) as logger_config:
            try:
                log_config = yaml.safe_load(logger_config.read())
                logging.config.dictConfig(log_config)
            except Exception:
                logging.exception("Error loading logger configuration")
                return _getting_default_logger()
            finally:
                logger_config.close()
    except Exception:
        logging.exception("Error, logger's file configuration not found")
        return _getting_default_logger()

    logger = logging.getLogger(logger_name)

    return logger
