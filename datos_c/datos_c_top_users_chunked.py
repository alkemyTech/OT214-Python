import logging
import logging.config
import xml.etree.ElementTree as Elt
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


# Defining the chunk divider function.
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
    list_of_chunks = list(tree_list[
                          init:(init + chunk_size + chunk_residue + 1)])
    yield list_of_chunks


# Defining the main mapping function that returns the Id, AnswerCount Tuples.
def mapper_func(row_obj: Elt.Element):
    if row_obj.attrib['PostTypeId'] == '1':
        if 'AnswerCount' in row_obj.attrib.keys():
            answer_count = int(row_obj.attrib['AnswerCount'])
        else:
            answer_count = 0
        if 'OwnerUserId' in row_obj.attrib.keys():
            owner_id = int(row_obj.attrib['OwnerUserId'])
        else:
            owner_id = 0
        return (owner_id, answer_count)
    else:
        pass


#  Defining function to later sort the data by its amount of AnswerCount.
def sorter_func(tuple1: Tuple, tuple2: Tuple):
    if tuple2[1] > tuple1[1]:
        return tuple2
    else:
        return tuple1


# Defining the function to take the top 10 most answered entries.
def top_10_func(list_of_tuples: List):
    top_10_list = []
    for i in range(10):
        id_max_answer = reduce(sorter_func, list_of_tuples)
        top_10_list.append(id_max_answer)
        max_id = id_max_answer[0]

        def max_value_popper(tuple1: Tuple):
            if tuple1[0] == max_id:
                return False
            else:
                return True
        list_of_tuples = list(filter(max_value_popper, list_of_tuples))
    return top_10_list


# Defining function to take dictionary key:values and return them as tuples.
def dict_to_tuple(key_value_pair):
    return (key_value_pair[0], key_value_pair[1])


# MAPPING
def main_mapper_func(
    list_test: List,
):
    id_answers_mapped = list(map(mapper_func, list_test))
    id_answers_clean = dict(filter(None, id_answers_mapped))
    id_answers_tuples = list(map(dict_to_tuple, id_answers_clean.items()))
    max_list = top_10_func(id_answers_tuples)
    return max_list


# REDUCING
def red_cleaning_func(list1: List, list2: List):
    list3 = list1 + list2
    return list3


# Defining master function that works with the yielded iterator,
# maps, reduces and returns the desired result.
def yielding_iterator():
    list_of_chunks = chunker_func(parser_func(xml_path1), 6)
    id_list_max = list(map(main_mapper_func, list_of_chunks))
    list_of_chunks.close()
    max_list_clean = reduce(red_cleaning_func, id_list_max)
    top_10_max = top_10_func(max_list_clean)
    num = 1
    message = ''
    for item in top_10_max:
        message += (f'{num}) UserId: {item[0]} Answers: {item[1]}\n')
        num += 1
    file_logger.info(f' The top users with most answers are:\n{message}')
    console_logger.info(f' The top users with most answers are:\n{message}')
    return


# Execution
yielding_iterator()
