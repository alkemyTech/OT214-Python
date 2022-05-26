import logging
import logging.config

import yaml


# Logging setup , the configuration parameters are taken
# from the yaml file that works as a dictionary
def setup_logging(default_level=logging.INFO):

    try:
        with open("./config/logging_config.yaml", 'rt') as file:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config)
            return ('Configuracion de Logging exitosa')
    except Exception as error:
        print(error)
        logging.basicConfig(level=default_level)
        return ('Error en configuracion de Logging, '
                'se utilizara configuración por defecto')
