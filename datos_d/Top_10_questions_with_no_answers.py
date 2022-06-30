import os
from functools import reduce
from logging.config import logging

from defusedxml.ElementTree import parse

# initialize instance of log for further logging notices

path_logging_file = os.path.dirname(__file__) + '/config/logging.cfg'
logging.config.fileConfig(path_logging_file)
logger = logging.getLogger('Stack Overflow')

# proceed to parse source file to analyze

xml_data_filename = 'posts.xml'
my_tree = ''
try:
    my_tree = parse(xml_data_filename)
except Exception as error:
    logger.critical(f'could not parse {xml_data_filename}, file not found or missing: {error}')
    raise FileNotFoundError

# generate main root for parsing all row entries

all_xml_roots = my_tree.getroot()


# defined function to partition metadata loaded in fixed chunk
# size of chunk could be set has an argument, declare division quantity
# if divided chunks partition do not outcome in round portions, data restless will be assigned to the last chunk

def data_chunker(all_data, number_of_chunks):
    chunk = []
    total_length = len(all_data)
    chunk_portion = int(total_length / number_of_chunks)
    near_integer = chunk_portion * number_of_chunks
    for divider in range(number_of_chunks):
        chunk.append(all_data[chunk_portion*divider:(chunk_portion*divider) + chunk_portion])
    if total_length % number_of_chunks != 0:
        add_rest = all_data[near_integer:total_length-1]
        last_chunk = chunk[number_of_chunks-1] + add_rest
        chunk[number_of_chunks-1] = last_chunk
    for row_entry in chunk:
        yield row_entry

# a generator is returned for each chunk to get it dynamic


# function to map all questions entries with no accepted answers, and its scores

def get_question_no_answers(objets):
    try:
        is_question = objets.attrib.get('PostTypeId')
        if is_question == '1':
            if 'AcceptedAnswerId' not in objets.attrib:
                id_question = int(objets.attrib.get('Id'))
                id_score = int(objets.attrib.get('Score'))
                return id_question, id_score
    except Exception as fail:
        logger.error(f'Fails to get attributes from xml posts: {fail}')
        raise AttributeError


# function to reduce and return 10 highest values of a chunk

def get_chunk_top_10(accumulate, iterable):
    if len(accumulate) < 10:
        accumulate.append(iterable)
        return accumulate
    minimum = min(accumulate, key=lambda item: item[1])
    if iterable[1] > minimum[1]:
        accumulate[accumulate.index(min(accumulate, key=lambda item: item[1]))] = iterable
    return accumulate


# compares the 10 highest values between two set of values (synthesized chunk and present one)

def get_top_10(accumulate, iterable):
    concat = sorted(accumulate + iterable, key=lambda item: item[1], reverse=True)
    return concat[:10]


# get the top 10 question with no accepted answers, defined by highest score obtained

def chunk_to_map_top_10_no_answers(chunk):
    map_questions = map(get_question_no_answers, chunk)
    map_questions = filter(None, map_questions)
    chunk_reduced = reduce(get_chunk_top_10, map_questions, [])
    return chunk_reduced


if __name__ == '__main__':

    # set partition on chunk by order of 16 fixed portions, could be other range

    xml_chunked_no_answers = data_chunker(all_xml_roots, 16)
    xml_mapped_questions = map(chunk_to_map_top_10_no_answers, xml_chunked_no_answers)

    # final reduce to synthesize the top 10 highest questions with no answers of all data set rows

    top_10_no_answers = reduce(get_top_10, xml_mapped_questions)
    logger.info('TOP 10 QUESTIONS WITH NO ANSWER:')
    for value in top_10_no_answers:
        print(f'Highest Score Nº{top_10_no_answers.index(value) + 1}-> Id: {value[0]}: {value[1]}')
