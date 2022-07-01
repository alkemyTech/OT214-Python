import logging
import logging.config
from datetime import datetime
from pathlib import Path

from click import types

from data_getter import DataGetter
from mapper import Mapper
from reducer import Reducer

file_path = Path(__file__).parents[0]
logger_config = Path(
        file_path,
        'config/logger.cfg')

logging.config.fileConfig(logger_config)
logger = logging.getLogger("dev")
logger.propagate = False

logger.info("Logger config")


def main():
    data_path = Path(
                file_path,
                '112010 Meta Stack Overflow/posts.xml')

    data_getter = DataGetter(data_path)
    data_mapper = Mapper()
    data_reducer = Reducer()

    generator = data_getter._get_iterator()
    data = data_getter.chunkify(generator, 5000)

    # results
    results = [{}, {}, {}]

    # all data
    data_c = []

    while True:
        try:
            # Getting data
            data_chunk = list(next(data))

            # first problem

            data_dt = data_mapper.get_datetime(data_chunk)

            data_result = data_reducer.make_count(data_dt)

            results[0].update(data_result)

            # second problem

            data_body = data_mapper.get_body(data_chunk)

            data_words = data_mapper.get_words(data_body)

            data_word = data_reducer.list_parse(data_words, types="list")

            data_count = data_reducer.make_count(data_word)

            results[1].update(data_count)

            # third problem

            data_c.extend(data_chunk)

            data_q = list(filter(lambda x: int(x.attrib["PostTypeId"]) == 1
                                 and "AcceptedAnswerId" in x.attrib.keys(),
                                 data_chunk))

            data_score = data_mapper.get_score(data_q)

            data_r = data_reducer.list_parse(data_score, types="dict")

            results[2].update(data_r)

        except RuntimeError:
            # print first problem
            for i in sorted(results[0], key=results[0].get)[0:10]:
                logger.info("the dates with the fewest publications made: "
                            + str(i).split(" ")[0])
            # print second problem
            for i in sorted(results[1], key=results[1].get)[(len(results[1])
                                                             - 10):len(
                                                                results[1])]:
                logger.info("The most said words in the posts are:"
                            + str(i))

            # print third problem

            data_top = sorted(results[2], key=results[2].get)[len(results[2])
                                                              - 200:
                                                              len(results[2])]

            results[2] = list(filter(lambda x: x.attrib["Id"] in data_top,
                                     data_c))

            answer_id = list(map(lambda x: x.attrib["AcceptedAnswerId"],
                                 results[2]))

            answer_get = list(filter(lambda x: x.attrib["Id"] in answer_id,
                                     data_c))

            answer_get = list(filter(lambda x: "Id" in x.attrib.keys(),
                                     answer_get))

            answer_time = list(map(lambda x: {x.attrib["Id"]:
                                              datetime.strptime(
                                                x.attrib["CreationDate"],
                                                "%Y-%m-%dT%H:%M:%S.%f")},
                                   answer_get))

            data_pt = data_reducer.list_parse(answer_time, types="dict")

            r_diff = data_mapper.get_diff(results[2], data_pt)

            time_m = data_reducer.make_diff(r_diff)

            r = "The average time it takes to answer a question correctly is:"

            logger.info(r + str(time_m / 60 / 60 / 24) + " days")

            logger.info("StopIteration")
            return


if __name__ == "__main__":
    main()
