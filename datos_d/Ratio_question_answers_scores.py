import os
from collections import Counter
from functools import reduce
from logging.config import logging

import numpy
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


# find all answers per each question through 'ParentId' attribute (every answer has a question Id reference)

def get_answers(objets):
    try:
        is_question = objets.attrib.get('PostTypeId')
        if is_question == '2':
            parent_id = int(objets.attrib.get('ParentId'))
            return parent_id
    except Exception as fail:
        logger.error(f'Fails to get attributes from xml posts: {fail}')
        raise AttributeError


# find all answers entries, beside its has answers or not

def get_questions_and_scores(objets):
    try:
        is_question = objets.attrib.get('PostTypeId')
        if is_question == '1':
            question_id = int(objets.attrib.get('Id'))
            question_score = int(objets.attrib.get('Score'))
            return question_id, question_score
    except Exception as fault:
        logger.error(f'Could not find question Id or Score: {fault}')
        raise AttributeError


# simple iterable function to reach an element summary and return the outcome

def get_total_of(accumulate, score):
    accumulate += score
    return accumulate


# calculates the relation between scores and the number of answers per questions
# criteria: function returns '0' has relation calculated if a particular question has no answers
# must be implemented to solve Zero's Division Error and get a homogeneous result

def get_relation(accumulate, iterable):
    if iterable[2] != 0:
        relation = iterable[1] / iterable[2]
    else:
        relation = 0
    accumulate.append(relation)
    return accumulate


# map every chunk to get the relation ratio of a chunk, this means: get all questions with its answers,
# whatever exists or not, and each question score to calculate every particular relation ratio resulted
# then reduce the outcome by estimate the mean relation of the entire chunk and return this value
# criteria: questions with no answers has been set to 0 value on the argument make it possible to calculate
# skipping 'None' values errors

def chunk_to_map_relation(chunk):
    index = 0
    tuples = []
    mapped_chunk = map(get_answers, chunk)
    mapped_chunk = filter(None, mapped_chunk)
    mapped_questions_answers = Counter(mapped_chunk).most_common()
    mapped_questions_answers = sorted(mapped_questions_answers, key=lambda item: item[0])
    question_ids = [item for item, item2 in mapped_questions_answers]
    answers_per_question = [item2 for item, item2 in mapped_questions_answers]
    mapped_question_and_scores = map(get_questions_and_scores, chunk)
    mapped_question_and_scores = filter(None, mapped_question_and_scores)
    for question in mapped_question_and_scores:
        if question[0] in question_ids:
            b = (question[0], question[1], answers_per_question[index])
            tuples.append(b)
            index += 1
        else:
            c = (question[0], question[1], 0)
            tuples.append(c)
    reduced_relations = reduce(get_relation, tuples, [])
    chunk_relations_ratio = numpy.mean(reduced_relations)
    return chunk_relations_ratio


if __name__ == '__main__':

    # set partition on chunk by order of 16 fixed portions, could be other range

    xml_chunked_relation = data_chunker(all_xml_roots, 16)
    xml_mapped = map(chunk_to_map_relation, xml_chunked_relation)

    # get the 'n' finally relations ratios of all chunk and calculate the mean relation resulted

    relation_ratio = (reduce(get_total_of, xml_mapped)) / 16
    logger.info(f'Relation Ratio Answers - Scores: {relation_ratio}')
