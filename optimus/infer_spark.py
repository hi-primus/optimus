import fastnumbers
from pyspark.ml.linalg import VectorUDT
from pyspark.sql import DataFrame as SparkDataFrame, functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, BooleanType, StructType, ArrayType, \
    LongType, DateType, ByteType, ShortType, TimestampType, BinaryType, NullType

from optimus.infer import is_bool_value, is_list_value, is_datetime, is_date, is_binary, is_str, is_bool_str, str_to_date, \
    is_list_str

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
SPARK_DTYPES_TO_INFERRED = {"int": ["smallint", "tinyint", "bigint", "int"], "float": ["float", "double"],
                            "string": "string", "date": {"date", "timestamp"}, "boolean": "boolean", "binary": "binary",
                            "array": "array", "object": "object", "null": "null", "missing": "missing"}
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


def to_spark(value):
    """
    Infer a Spark data type from a value
    :param value: value to be inferred
    :return: Spark data type
    """
    result = None
    if value is None:
        result = "null"

    elif is_bool_value(value):
        result = "bool"

    elif fastnumbers.isint(value):
        result = "int"

    elif fastnumbers.isfloat(value):
        result = "float"

    elif is_list_value(value):
        result = ArrayType(to_spark(value[0]))

    elif is_datetime(value):
        result = "datetime"

    elif is_date(value):
        result = "date"

    elif is_binary(value):
        result = "binary"

    elif is_str(value):
        if is_bool_str(value):
            result = "bool"
        elif is_datetime(value):
            result = "string"  # date
        elif is_list_str(value):
            result = "string"  # array
        else:
            result = "string"

    return parse_spark_class_dtypes(result)


def is_list_of_spark_dataframes(value):
    """
    Check if an object is a Spark DataFrame
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, SparkDataFrame) for elem in value)


def is_column(value):
    """
    Check if a object is a column
    :return:
    """
    return isinstance(value, F.Column)
