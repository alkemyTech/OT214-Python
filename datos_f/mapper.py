from datetime import datetime
import logging
import logging.config
from pathlib import Path

file_path = Path(__file__).parents[0]
logger_config = Path(
        file_path,
        'config/logger.cfg')

logging.config.fileConfig(logger_config)
logger = logging.getLogger("dev")


class Mapper():
    def _result_diff(self, x, answer_data):
        try:
            return answer_data[x.attrib["AcceptedAnswerId"]
                               ] - datetime.strptime(x.attrib["CreationDate"],
                                                     "%Y-%m-%dT%H:%M:%S.%f")
        except KeyError:
            self.logger.warning("Data is not clean, irrelevant")

    def get_diff(self, p_data, answer_data):
        return list(map(lambda x: self._result_diff(x, answer_data), p_data))

    def get_score(self, data):
        return list(map(lambda x: {x.attrib["Id"]: x.attrib["Score"]}, data))

    def get_p(self, f):
        return map(lambda x: f(x))

    def parse_words(self, x):
        x = x.replace(",", "").replace("\n", "  ")

        return x.split(" ")

    def get_words(self, data):
        return list(map(lambda x: self.parse_words(x), data))

    def get_body(self, data):
        return list(map(lambda x: x.attrib["Body"], data))

    def data_sort(self, data, type=dict):
        return {k: v for k, v in sorted(data.items(), key=lambda item: item[1])}

    def _make_datetime(self, data, attrib: str,
                       format: str, datetime_split=""):
        if datetime_split != "":
            parse_datetime = data.attrib[attrib].split(datetime_split)[0]
            return datetime.strptime(parse_datetime, format)
        return datetime.strptime(data, format)

    def get_datetime(self, data: list, format="%Y-%m-%d"):
        day = map(lambda x: self._make_datetime(x, "CreationDate",
                                                format, "T"), data)

        return day

    def __init__(self) -> None:
        self.logger = logger
