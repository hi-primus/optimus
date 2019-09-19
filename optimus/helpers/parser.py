from optimus.helpers.constants import SPARK_DTYPES_DICT, SPARK_SHORT_DTYPES, PYTHON_SHORT_TYPES, \
    SPARK_DTYPES_DICT_OBJECTS
from optimus.helpers.logger import logger


def compress_dict(lst, col_name):
    _result = {}
    for l in lst:
        _result.setdefault(col_name, []).append({"value": l[col_name], "count": l["count"]})
    return _result


def compress_list(lst):
    _result = {}
    for l in lst:
        for k, v in l.items():
            _result.setdefault(k, []).append(v)
    return _result


def parse_col_names_funcs_to_keys(data):
    from optimus.helpers.check import is_numeric, is_nan

    """
    Helper function that return a formatted json with function:value inside columns. Transform from
    {'max_antiguedad_anos': 15,
    'max_m2_superficie_construida': 1800000,
    'min_antiguedad_anos': 2,
    'min_m2_superficie_construida': 20}

    to

    {'m2_superficie_construida': {'min': 20, 'max': 1800000}, 'antiguedad_anos': {'min': 2, 'max': 15}}

    :param data: json data
    :return: json
    """
    functions_array = ["range", "count_uniques", "count_na", "min", "max", "stddev", "kurtosis", "mean", "skewness",
                       "sum", "variance",
                       "approx_count_distinct", "countDistinct", "na", "zeros", "percentile", "count", "hist"]

    _result = {}
    for k, v in data[0].items():
        for f in functions_array:

            temp_func_name = f + "_"
            if k.startswith(temp_func_name):
                _col_name = k[len(temp_func_name):]
                if is_nan(v):
                    logger.print(
                        "'{FUNCTION}' function in '{COL_NAME}' column is returning 'nan'. Is that what you expected?. Seems that '{COL_NAME}' has 'nan' values".format(
                            FUNCTION=f,
                            COL_NAME=_col_name))
                # If the value is numeric only get 5 decimals
                elif is_numeric(v):
                    v = round(v, 5)
                _result.setdefault(_col_name, {})[f] = v

                break

    return _result


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
