import logging
import logging.config
import math
import re
import xml.etree.ElementTree as Elt
# from datetime import datetime
from functools import reduce
from typing import List, Tuple

logging.config.fileConfig('./config/logging.cfg')
file_logger = logging.getLogger('fileRotating')
console_logger = logging.getLogger('console')


xml_path1 = './Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
csv_name1 = './xlmtocsv.csv'


# Defining parser function.
def parser_func(xml_path: str):
    tree = Elt.parse(xml_path)
    return tree


def chunker_func(tree: Elt.ElementTree, chunk_numbers):
    tree_list = list(tree.iter('row'))
    chunk_size = len(tree_list) // chunk_numbers
    chunk_residue = len(tree_list) % chunk_numbers
    init = 0
    list_of_chunks = []
    for i in range(chunk_numbers - 1):
        if i < chunk_numbers:
            list_of_chunks = list(tree_list[init:(init + chunk_size)])
            init += chunk_size
            yield list_of_chunks
    list_of_chunks = list(tree_list[init:(init
                                          + chunk_size
                                          + chunk_residue
                                          + 1)])
    yield list_of_chunks


def mapper_func(row_obj: Elt.Element):
    if row_obj.attrib['PostTypeId'] == '1':
        clean_string = re.sub(r'(&lt;)', '', row_obj.attrib['Body'])
        mapped_body_splitted = clean_string.split()
        if 'AnswerCount' in row_obj.attrib.keys():
            answer_count = int(row_obj.attrib['AnswerCount'])
        else:
            answer_count = 0
        return (len(mapped_body_splitted), answer_count)
    else:
        pass


def sqrd_func(mapped_tuple: Tuple):
    return (mapped_tuple[0]**2, mapped_tuple[1]**2)


def tuples_summer(mapped_tuple1: Tuple, mapped_tuple2: Tuple):
    return (mapped_tuple1[0] + mapped_tuple2[0],
            mapped_tuple1[1] + mapped_tuple2[1])


def multiplier_func(mapped_tuple: Tuple):
    return (mapped_tuple[0] * mapped_tuple[1])


def int_summer(int_a, int_b):
    return int_a + int_b


# MAPPING
def main_mapper_func(
    list_test: List,
):
    word_count_mapped = list(map(mapper_func, list_test))
    word_count_mapped_clean = list(filter(None, word_count_mapped))
    return word_count_mapped_clean


def mult_mapper_func(
    list_test: List,
):
    words_times_answers_mapped = list(map(multiplier_func, list_test))
    return words_times_answers_mapped


def sqrd_mapper_func(
    list_test: List,
):
    sqrd_count_answers_mapped = list(map(sqrd_func, list_test))
    return sqrd_count_answers_mapped


def returning_tuples(tuple1: Tuple):
    return tuple1


# REDUCING
def red_cleaning_func(list1: List, list2: List):
    list3 = list1 + list2
    return list3


def master_reducer_func(
    word_count_mapped: List,
    sqrd_count_answers_mapped: List,
    words_times_answers_mapped: List
):
    number_of_registers = len(word_count_mapped)
    reduced_word_count_answers = reduce(tuples_summer, word_count_mapped)
    reduced_words_times_answrs_mapped = int(reduce(int_summer,
                                                   words_times_answers_mapped))
    reduced_sqrds = reduce(tuples_summer, sqrd_count_answers_mapped)
    coef_args = {
        'n_reg': number_of_registers,
        'sum_x': reduced_word_count_answers[0],
        'sum_y': reduced_word_count_answers[1],
        'sum_xy': reduced_words_times_answrs_mapped,
        'sum_x_sqr': reduced_sqrds[0],
        'sum_y_sqr': reduced_sqrds[1]
    }
    return coef_args


"""
    For Pearsons Coefficient (r) calculation we use the following formula:

                      n * sum(xy) - (sum(x) * sum(y))
    r = --------------------------------------------------------
        sqrt{[n * (sum x**2) - sum(x)**2]*[n * (sum y**2) - sum(y)**2]}

    Where:
    n = number_of_registers
    sum(x) = reduced_word_count_answers[0]
    sum(y) = reduced_word_count_answers[1]
    sum(xy) = reduced_words_times_answers
    sum(x**2) = reduced_sqrds[0]
    sum(y**2) = reduced_sqrds[1]
"""


def pearson_coef_calculator(
    n_reg: int,
    sum_x: int,
    sum_y: int,
    sum_xy: int,
    sum_x_sqr: int,
    sum_y_sqr: int
):
    upper_opperand = (n_reg * sum_xy) - (sum_x * sum_y)
    lower_opperand_a = (n_reg * sum_x_sqr) - (sum_x ** 2)
    lower_opperand_b = (n_reg * sum_y_sqr) - (sum_y ** 2)
    lower_opperand = math.sqrt(lower_opperand_a * lower_opperand_b)
    r_coef = upper_opperand / lower_opperand
    return round(r_coef, 5)


# Defining logger response according to coeefficient result.
def coef_message(coef: float):
    if (coef >= -1) and (coef <= -0.75):
        return 'Perfect negative correlation'
    elif (coef > -0.75) and (coef <= -0.5):
        return 'Strong negative correlation'
    elif (coef > -0.5) and (coef <= -0.25):
        return 'Moderate negative correlation'
    elif (coef > -0.25) and (coef < 0.25):
        return 'Mild to none correlation'
    elif (coef >= 0.25) and (coef < 0.5):
        return 'Moderate positive correlation'
    elif (coef >= 0.5) and (coef < 0.75):
        return 'Strong positive correlation'
    elif (coef >= 0.75) and (coef <= 1):
        return 'Perfect positive correlation'


# Defining master function that works with the yielded iterator,
# maps, reduces and returns the desired result.
def yielding_iterator():
    # timer = datetime.now()
    parsed = parser_func(xml_path1)
    # parse_time = (datetime.now() - timer)
    # print(f'parsing takes {parse_time}')
    list_of_chunks = chunker_func(parsed, 6)
    # chunk_time = (datetime.now() - timer) - parse_time
    # print(f'chunking takes {chunk_time}')
    main_mapped_clean = list(map(main_mapper_func, list_of_chunks))
    main_sqrd_word_answr = list(map(sqrd_mapper_func, main_mapped_clean))
    main_multi_word_answr = list(map(mult_mapper_func, main_mapped_clean))
    list_of_chunks.close()
    # map_time = (datetime.now() - timer)-parse_time - chunk_time
    # print(f'mapping takes {map_time}')
    main_mapped_to_reduce = reduce(red_cleaning_func, main_mapped_clean)
    main_sqr_to_reduce = reduce(red_cleaning_func, main_sqrd_word_answr)
    main_mult_to_reduce = reduce(red_cleaning_func, main_multi_word_answr)
    reducer_args = [main_mapped_to_reduce,
                    main_sqr_to_reduce,
                    main_mult_to_reduce]
    # red_time = (datetime.now() - timer) - map_time - map_time - chunk_time
    # print(f'reducing takes {red_time}')
    coef_args = master_reducer_func(*reducer_args)
    coefficient = pearson_coef_calculator(**coef_args)
    message = coef_message(coefficient)
    file_logger.info(f' {message} with coefficient {coefficient}')
    console_logger.info(f' {message} with coefficient {coefficient}')
    return


yielding_iterator()
