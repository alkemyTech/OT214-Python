import logging
import logging.config
from functools import reduce
from pathlib import Path

file_path = Path(__file__).parents[0]
logger_config = Path(
        file_path,
        'config/logger.cfg')

logging.config.fileConfig(logger_config)
logger = logging.getLogger("dev")


class Reducer():
    def _make_r(self, a, b):
        try:
            return a + b.total_seconds()
        except AttributeError:
            return a

    def make_diff(self, data):
        return (reduce(lambda a, b: self._make_r(a, b), data, 0)) / len(data)

    def _reduce_list(self, a, b):
        a[list(b.keys())[0]] = b[list(b.keys())[0]]

        return a

    def list_parse(self, data, types="dict"):
        if types == "list":
            return reduce(lambda a, b: a + b, data, [])
        return reduce(lambda a, b: self._reduce_list(a, b), data, {})

    def _make_sum(self, values, x):
        if x in values.keys():
            values[x] += 1
            return values
        values[x] = 1

        return values

    def make_count(self, data):
        return reduce(lambda a, b: self._make_sum(a, b), data, {})

    def __init__(self):
        self.logger = logger
