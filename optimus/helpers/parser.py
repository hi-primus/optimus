from optimus import val_to_list, SPARK_DTYPES_DICT_OBJECTS, SPARK_SHORT_DTYPES
from optimus.helpers.constants import SPARK_DTYPES_DICT, SPARK_SHORT_DTYPES, PYTHON_SHORT_TYPES
from optimus.helpers.converter import val_to_list, one_list_to_val


def parse_spark_dtypes(value):
    """
    Get a pyspark data type from a string data type representation. for example 'StringType' from 'string'
    :param value:
    :return:
    """

    value = val_to_list(value)

    try:
        data_type = [SPARK_DTYPES_DICT[SPARK_SHORT_DTYPES[v]] for v in value]

    except KeyError:
        data_type = value

    data_type = one_list_to_val(data_type)
    return data_type


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
    value = val_to_list(value)
    try:
        data_type = [SPARK_DTYPES_DICT_OBJECTS[SPARK_SHORT_DTYPES[v]] for v in value]

    except (KeyError, TypeError):
        data_type = value

    data_type = one_list_to_val(data_type)
    return data_type