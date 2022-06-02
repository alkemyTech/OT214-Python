import logging


def transform_flores():
    '''
    Function that will normalize the data of the Universidad de Flores.
    Will return the final state of normalization
    '''
    pass


def transform_villamaria():
    '''
    Function that will normalize the data of the Universidad de Villa Maria.
    Will return the final state of normalization
    '''
    pass


def transform_data():
    '''
    Universities Data is extracted from the csv files generated in extract task
    These data are normalized as required through the transform functions
    '''
    logger = logging.getLogger("Transform")
    res = transform_flores()
    if(res == "success"):
        logger.info(
            "Universidad de Flores normalized txt were generate succesfully")
    else:
        logger.error(res)
    res = transform_villamaria()
    if(res == "success"):
        logger.info(
            "Universidad de Villa María"
            "normalized txt were generate succesfully")
    else:
        logger.error(res)
