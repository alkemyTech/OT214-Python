import logging
import logging.config

import yaml

configuration_path = "./config/"


def get_dataframe_config(columns_file="columns.yaml"):
    try:
        with open(configuration_path + columns_file) as column_config:
            df_config = yaml.safe_load(column_config.read())

    except Exception as e:
        logging.exception("Error loading dataframe's configuration" + str(e))

    finally:
        column_config.close()
        return df_config["dataframes_config"]
