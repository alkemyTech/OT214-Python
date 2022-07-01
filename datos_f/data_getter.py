from xml.etree import ElementTree as ET
import os
import logging
import logging.config
from pathlib import Path

file_path = Path(__file__).parents[0]
logger_config = Path(
        file_path,
        'config/logger.cfg')

logging.config.fileConfig(logger_config)
logger = logging.getLogger("dev")


class DataGetter():
    def chunkify(self, generator, batch_size):
        while True:
            try:
                data = []
                for i in range(batch_size):
                    data.append(next(generator)[1])
                yield data
            except RuntimeError:
                logger.info("Generator raise StopIteration")


    def _get_iterator(self):
        generator = ET.iterparse(self.data_path)

        return generator

    def get_files(self, files: list) -> list:
        path_files = []

        for file in files:
            path_file = self.data_path + file

            try:
                if os.path.isfile(path_file):
                    path_files.append(self.data_path + file)
                    continue
                raise FileNotFoundError("File not found: " + file)
            except Exception as e:
                self.logger.warning("Error getting files" + str(e))
        return path_files

    def __init__(self, data_path="./data/"):
        self.logger = logger

        self.logger.info("Getting Data.")
        self.data_path = data_path
