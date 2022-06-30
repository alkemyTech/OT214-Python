import datetime
import os
import xml.etree.ElementTree as Et
from collections import Counter
from functools import reduce
from logging.config import logging

from dateutil import parser

# initialize instance of log for further logging notices

path_logging_file = os.path.dirname(__file__) + '/config/logging.cfg'
logging.config.fileConfig(path_logging_file)
logger = logging.getLogger('Stack Overflow')

# proceed to parse source file to analyze

xml_data_filename = 'posts.xml'
my_tree = ''
try:
    my_tree = Et.parse(xml_data_filename)
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


# function to parse all question entries and calculate its activity lifetime

def get_question_date(objets):
    try:
        is_question = objets.attrib.get('PostTypeId')
        if is_question == '1':
            creation_date = parser.parse(objets.attrib['CreationDate'])
            last_activity_date = parser.parse(objets.attrib['LastActivityDate'])
            id_time_delta = last_activity_date - creation_date
            id_question = objets.attrib.get('Id')
            return id_question, id_time_delta
    except Exception as fail:
        logger.error(f'Fails to get attributes from xml posts: {fail}')
        raise AttributeError


# function to reduce and return 10 longest sessions time of a chunk

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


# defines the 10 questions with the longest session activity of a chunk

def chunk_to_map_top_10(chunk):
    map_date = map(get_question_date, chunk)
    map_date = filter(None, map_date)
    chunk_reduced = reduce(get_chunk_top_10, map_date, [])
    return chunk_reduced


if __name__ == '__main__':

    # set partition on chunk by order of 16 fixed portions, could be other range

    xml_chunked_dates = data_chunker(all_xml_roots, 16)
    xml_mapped_dates = map(chunk_to_map_top_10, xml_chunked_dates)

    # final reduce to synthesize the top 10 longest session of all data set rows

    top_10_longest_sessions = reduce(get_top_10, xml_mapped_dates)
    logger.info('TOP 10 ACTIVITY TIME QUESTIONS:')
    for value in top_10_longest_sessions:
        print(f'Longest Session Nº{top_10_longest_sessions.index(value) + 1}-> Id: {value[0]}: {value[1]}')
