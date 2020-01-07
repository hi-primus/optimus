# This file need to be send to the cluster via .addPyFile to handle the pickle problem
# This is outside the optimus folder on purpose because it cause problem importing optimus when using de udf.
# This can not import any optimus file unless it's imported via addPyFile
import datetime
import os
import re
from ast import literal_eval

import fastnumbers
import math
from dateutil.parser import parse as dparse
from pyspark.ml.linalg import VectorUDT
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType, DoubleType, BooleanType, StructType, \
    LongType, DateType, ByteType, ShortType, TimestampType, BinaryType, NullType


# This function return True or False if a string can be converted to any datatype.


def str_to_boolean(_value):
    _value = _value.lower()
    if _value == "true" or _value == "false":
        return True
    else:
        return False


def str_to_date(_value):
    try:
        dparse(_value)
        return True
    except (ValueError, OverflowError):
        pass


def str_to_null(_value):
    _value = _value.lower()
    if _value == "null":
        return True
    else:
        return False


def is_null(_value):
    if _value is None:
        return True
    else:
        return False


def str_to_gender(_value):
    _value = _value.lower()
    if _value == "male" or _value == "female":
        return True
    else:
        return False


def str_to_data_type(_value, _dtypes):
    """
    Check if value can be parsed to a tuple or and list.
    Because Spark can handle tuples we will try to transform tuples to arrays
    :param _value:
    :return:
    """
    try:
        if isinstance(literal_eval((_value.encode('ascii', 'ignore')).decode("utf-8")), _dtypes):
            return True
    except (ValueError, SyntaxError):
        pass


def str_to_array(_value):
    return str_to_data_type(_value, (list, tuple))


def str_to_object(_value):
    return str_to_data_type(_value, (dict, set))


def str_to_url(_value):
    regex = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    if regex.match(_value):
        return True


def str_to_ip(_value):
    regex = re.compile('''\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}''')
    if regex.match(_value):
        return True


def str_to_email(_value):
    try:
        regex = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
        if regex.match(_value):
            return True
    except TypeError:
        pass


def str_to_credit_card(_value):
    # Reference https://www.regular-expressions.info/creditcard.html
    # https://codereview.stackexchange.com/questions/74797/credit-card-checking
    regex = re.compile(r'(4(?:\d{12}|\d{15})'  # Visa
                       r'|5[1-5]\d{14}'  # Mastercard
                       r'|6011\d{12}'  # Discover (incomplete?)
                       r'|7\d{15}'  # What's this?
                       r'|3[47]\d{13}'  # American Express
                       r')$')
    return bool(regex.match(_value))


def str_to_zip_code(_value):
    regex = re.compile(r'^(\d{5})([- ])?(\d{4})?$')
    if regex.match(_value):
        return True
    return False


def str_to_missing(_value):
    return True if _value == "" else False


def str_to_int(_value):
    return True if fastnumbers.isint(_value) else False


def str_to_decimal(_value):
    return True if fastnumbers.isfloat(_value) else False


def str_to_str(_value):
    return True if isinstance(_value, str) else False


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


class Infer(object):
    """
    This functions return True or False if match and specific dataType
    """
    DTYPE_FUNC = {"string": str_to_str, "boolean": str_to_boolean, "date": str_to_date,
                  "array": str_to_array, "object": str_to_object, "ip": str_to_ip,
                  "url": str_to_url, "email": str_to_email, "gender": str_to_gender,
                  "credit_card_number": str_to_credit_card, "zip_code": str_to_zip_code, "int": str_to_int,
                  "decimal": str_to_decimal}

    @staticmethod
    def mismatch(value: tuple, dtypes: dict):
        """
        UDF function.
        For example if we have an string column we also need to pass if the column is a credit card or  postal code.
        Count the dataType that match, do not match, nulls and missing.
        :param value: tuple(Column/Row, value)
        :param dtypes: dict {col_name:(dataType, mismatch)}

        :return:
        """
        col_name, value = value

        _data_type = ""
        dtype = dtypes[col_name]

        if Infer.DTYPE_FUNC[dtype](value) is True:
            _data_type = dtype
        else:
            if is_null(value) is True:
                _data_type = "null"
            elif str_to_missing(value) is True:
                _data_type = "missing"
            else:
                _data_type = "mismatch"

        result = (col_name, _data_type), 1
        return result

    @staticmethod
    def to_spark(value):
        """
        Infer a Spark data type from a value
        :param value: value to be inferred
        :return: Spark data type
        """
        result = None
        if value is None:
            result = "null"

        elif is_bool(value):
            result = "bool"

        elif fastnumbers.isint(value):
            result = "int"

        elif fastnumbers.isfloat(value):
            result = "float"

        elif is_list(value):
            result = ArrayType(Infer.to_spark(value[0]))

        elif is_datetime(value):
            result = "datetime"

        elif is_date(value):
            result = "date"

        elif is_binary(value):
            result = "binary"

        elif is_str(value):
            if str_to_boolean(value):
                result = "bool"
            elif str_to_date(value):
                result = "string"  # date
            elif str_to_array(value):
                result = "string"  # array
            else:
                result = "string"

        return parse_spark_class_dtypes(result)

    @staticmethod
    def func(value, data_type, get_type):
        """
        Check if a value can be casted to a specific
        :param value: value to be checked
        :return:
        """
        if isinstance(value, bool):
            _data_type = "bool"
        elif fastnumbers.isint(value):  # Check if value is integer
            _data_type = "int"
        elif fastnumbers.isfloat(value):
            _data_type = "float"
        # if string we try to parse it to int, float or bool
        elif isinstance(value, str):
            if str_to_boolean(value):
                _data_type = "bool"
            elif str_to_date(value):
                _data_type = "date"
            elif str_to_array(value):
                _data_type = "array"
            else:
                _data_type = "string"
        else:
            _data_type = "null"

        if get_type is False:
            if _data_type == data_type:
                return True
            else:
                return False
        else:
            return _data_type

    @staticmethod
    def parse(value, infer: bool = False, dtypes=None, str_funcs=None, int_funcs=None):
        """

        :param value:
        :param infer: If 'True' try to infer in all the dataTypes available. See int_func and str_funcs
        :param dtypes:
        :param str_funcs: Custom string function to infer.
        :param int_funcs: Custom numeric functions to infer.
        {col_name: regular_expression}
        :return:
        """
        col_name, value = value

        # Try to order the functions from less to more computational expensive
        if int_funcs is None:
            int_funcs = [(str_to_credit_card, "credit_card_number"), (str_to_zip_code, "zip_code")]

        if str_funcs is None:
            str_funcs = [
                (str_to_missing, "missing"), (str_to_boolean, "boolean"), (str_to_date, "date"),
                (str_to_array, "array"), (str_to_object, "object"), (str_to_ip, "ip"),
                (str_to_url, "url"),
                (str_to_email, "email"), (str_to_gender, "gender"), (str_to_null, "null")
            ]
        print(dtypes)
        if dtypes[col_name] == "string" and infer is True:

            if isinstance(value, bool):
                _data_type = "boolean"

            elif fastnumbers.isint(value):  # Check if value is integer
                _data_type = "int"
                for func in int_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break

            elif fastnumbers.isfloat(value):
                _data_type = "decimal"

            elif isinstance(value, str):
                _data_type = "string"
                for func in str_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break
            else:
                _data_type = "null"

        else:
            _data_type = dtypes[col_name]
            if is_null(value) is True:
                _data_type = "null"
            elif str_to_missing(value) is True:
                _data_type = "missing"
            else:
                if dtypes[col_name].startswith("array"):
                    _data_type = "array"
                else:
                    _data_type = dtypes[col_name]

        result = (col_name, _data_type), 1

        return result


def is_nan(value):
    """
    Check if a value is nan
    :param value:
    :return:
    """
    result = False
    if is_str(value):
        if value.lower() == "nan":
            result = True
    elif is_numeric(value):
        if math.isnan(value):
            result = True
    return result


def is_none(value):
    """
    Check if a value is none
    :param value:
    :return:
    """
    result = False
    if is_str(value):
        if value.lower() == "none":
            result = True
    elif value is None:
        result = True
    return result


def is_same_class(class1, class2):
    """
    Check if 2 class are the same
    :param class1:
    :param class2:
    :return:
    """
    return class1 == class2


def is_(value, type_):
    """
    Check if a value is instance of a class
    :param value:
    :param type_:
    :return:
    """

    return isinstance(value, type_)


def is_type(type1, type2):
    """
    Check if a value is a specific class
    :param type1:
    :param type2:
    :return:
    """
    return type1 == type2


def is_function(value):
    """
    Check if a param is a function
    :param value: object to check for
    :return:
    """
    return hasattr(value, '__call__')


def is_list(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, list)


def is_list_empty(value):
    """
    Check is a list is empty
    :param value:
    :return:
    """
    return len(value) == 0


def is_dict(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, dict)


def is_tuple(value):
    """
    Check if an object is a tuple
    :param value:
    :return:
    """
    return isinstance(value, tuple)


def is_column(value):
    """
    Check if a object is a column
    :return:
    """
    return isinstance(value, F.Column)


def is_list_of_str(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, str) for elem in value)


def is_list_of_int(value):
    """
    Check if an object is a list of integers
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, int) for elem in value)


def is_list_of_float(value):
    """
    Check if an object is a list of floats
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, float) for elem in value)


def is_list_of_str_or_int(value):
    """
    Check if an object is a string or an integer
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (int, str)) for elem in value)


def is_list_of_str_or_num(value):
    """
    Check if an object is string, integer or float
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (str, int, float)) for elem in value)


def is_list_of_dataframes(value):
    """
    Check if an object is a Spark DataFrame
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, DataFrame) for elem in value)


def is_filepath(file_path):
    """
    Check if a value ia a valid file path
    :param file_path:
    :return:
    """
    # the file is there
    if os.path.exists(file_path):
        return True
    # the file does not exists but write privileges are given
    elif os.access(os.path.dirname(file_path), os.W_OK):
        return True
    # can not write there
    else:
        return False


def is_ip(value):
    """
    Check if a value is valid ip
    :param value:
    :return:
    """
    parts = value.split(".")
    if len(parts) != 4:
        return False
    for item in parts:
        if not 0 <= int(item) <= 255:
            return False
    return True


def is_list_of_strings(value):
    """
    Check if all elements in a list are strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, str) for elem in value)


def is_list_of_numeric(value):
    """
    Check if all elements in a list are int or float
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (int, float)) for elem in value)


def is_list_of_list(value):
    """
    Check if all elements in a list are tuples
    :param value:
    :return:
    """

    return bool(value) and isinstance(value, list) and all(isinstance(elem, list) for elem in value)


def is_list_of_tuples(value):
    """
    Check if all elements in a list are tuples
    :param value:
    :return:
    """

    return bool(value) and isinstance(value, list) and all(isinstance(elem, tuple) for elem in value)


def is_list_of_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    if is_list(value):
        return len(value) == 1


def is_dict_of_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    if is_dict(value):
        return len(value) == 1


def is_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (str, int, float, bool))


def is_num_or_str(value):
    """
    Check if a var is numeric(int, float) or string
    :param value:
    :return:
    """
    return isinstance(value, (int, float, str))


def is_str_or_int(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """

    return isinstance(value, (str, int))


def is_numeric(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (int, float))


def is_str(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    return isinstance(value, str)

def is_list_of_futures(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(
        isinstance(elem, distributed.client.Future) for elem in value)


def is_int(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, int)


def is_url(value):
    regex = re.compile(
        r'^(?:http|ftp|hdfs)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    return re.match(regex, value)


def is_float(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, float)


def is_dataframe(value):
    """
    Check if an object is a Spark DataFrame
    :param value:
    :return:
    """
    return isinstance(value, DataFrame)


def is_bool(value):
    return isinstance(value, bool)


def is_datetime(value):
    """
    Check if an object is a datetime
    :param value:
    :return:
    """

    return isinstance(value, datetime.datetime)


def is_binary(value):
    """
    Check if an object is a bytearray
    :param value:
    :return:
    """
    return isinstance(value, bytearray)


def is_date(value):
    """
    Check if an object is a date
    :param value:
    :return:
    """

    return isinstance(value, datetime.date)


PYTHON_SHORT_TYPES = {"string": "string",
                      "str": "string",
                      "integer": "int",
                      "int": "int",
                      "float": "float",
                      "double": "double",
                      "bool": "boolean",
                      "boolean": "boolean",
                      "array": "array",
                      "null": "null"
                      }
PYTHON_TYPES = {"string": str, "int": int, "float": float, "boolean": bool}
PYSPARK_NUMERIC_TYPES = ["byte", "short", "big", "int", "double", "float"]
PYSPARK_NOT_ARRAY_TYPES = ["byte", "short", "big", "int", "double", "float", "string", "date", "bool"]
PYSPARK_STRING_TYPES = ["str"]
PYSPARK_ARRAY_TYPES = ["array"]
SPARK_SHORT_DTYPES = {"string": "string",
                      "str": "string",
                      "integer": "int",
                      "int": "int",
                      "bigint": "bigint",
                      "big": "bigint",
                      "long": "bigint",
                      "float": "float",
                      "double": "double",
                      "bool": "boolean",
                      "boolean": "boolean",
                      "struct": "struct",
                      "array": "array",
                      "date": "date",
                      "datetime": "datetime",
                      "byte": "byte",
                      "short": "short",
                      "binary": "binary",
                      "null": "null",
                      "vector": "vector",
                      "timestamp": "datetime"
                      }
SPARK_DTYPES_DICT = {"string": StringType, "int": IntegerType, "float": FloatType,
                     "double": DoubleType, "boolean": BooleanType, "struct": StructType, "array": ArrayType,
                     "bigint": LongType, "date": DateType, "byte": ByteType, "short": ShortType,
                     "datetime": TimestampType, "binary": BinaryType, "null": NullType, "vector": VectorUDT
                     }
SPARK_DTYPES_DICT_OBJECTS = \
    {"string": StringType(), "int": IntegerType(), "float": FloatType(),
     "double": DoubleType(), "boolean": BooleanType(), "struct": StructType(), "array": ArrayType(StringType()),
     "bigint": LongType(), "date": DateType(), "byte": ByteType(), "short": ShortType(),
     "datetime": TimestampType(), "binary": BinaryType(), "null": NullType()
     }
PROFILER_COLUMN_TYPES = {"categorical", "numeric", "date", "null", "array", "binary"}
PYTHON_TO_PROFILER = {"string": "categorical", "boolean": "categorical", "int": "numeric", "decimal": "numeric",
                      "date": "date", "array": "array", "binaty": "binary", "null": "null"}
SPARK_DTYPES_TO_PROFILER = {"int": ["smallint", "tinyint", "bigint", "int"], "decimal": ["float", "double"],
                            "string": "string", "date": {"date", "timestamp"}, "boolean": "boolean", "binary": "binary",
                            "array": "array", "object": "object", "null": "null", "missing": "missing"}
