from optimus.helpers.constants import SPARK_DTYPES_DICT, SPARK_SHORT_DTYPES, PYTHON_SHORT_TYPES, \
    SPARK_DTYPES_DICT_OBJECTS


def parse_spark_dtypes(value):
    """
    Get a pyspark data type from a string data type representation. for example 'StringType' from 'string'
    :param value:
    :return:
    """

    if not isinstance(value, list):
        value = [value]

    try:
        data_type = [SPARK_DTYPES_DICT[SPARK_SHORT_DTYPES[v]] for v in value]

    except KeyError:
        data_type = value

    if isinstance(data_type, list) and len(data_type) == 1:
        result = data_type[0]
    else:
        result = data_type

    return result


def parse_python_dtypes(value):
    """
    Get a spark data type from a string
    :param value:
    :return:
    """
    return PYTHON_SHORT_TYPES[value.lower()]


def parse_spark_class_dtypes(value):
    """
    Get a pyspark data class from a string data type representation. for example 'StringType()' from 'string'
    :param value:
    :return:
    """
    if not isinstance(value, list):
        value = [value]

    try:
        data_type = [SPARK_DTYPES_DICT_OBJECTS[SPARK_SHORT_DTYPES[v]] for v in value]

    except (KeyError, TypeError):
        data_type = value

    if isinstance(data_type, list) and len(data_type) == 1:
        result = data_type[0]
    else:
        result = data_type

    return result
