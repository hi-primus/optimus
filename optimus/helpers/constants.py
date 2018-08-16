from pyspark.sql.types import StringType, BooleanType, IntegerType, \
    ArrayType, FloatType, DoubleType, StructType, DateType


import logging

# You can use string, str or String as param

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
PYTHON_TYPES_ = {"string": str, "int": int, "float": float, "boolean": bool}


PROFILER_TYPES = {"int", "float", "string", "bool", "date", "null"}
PROFILER_LEGEND_TYPES = {"string": "ABC", "int": "#", "integer": "#", "float": "##.#", "double": "##.#", "bigint": "#"}
PROFILER_COLUMN_TYPES = {"categorical", "numeric", "date", "null"}

SPARK_SHORT_DTYPES = {"string": "string",
                      "str": "string",
                      "integer": "int",
                      "int": "int",
                      "float": "float",
                      "double": "double",
                      "bool": "boolean",
                      "boolean": "boolean",
                      "struct": "struct",
                      "array": "array",
                      "date": "date"
                      # "vector": "vector"

                      }
SPARK_DTYPES_DICT = {"string": StringType(), "int": IntegerType(), "float": FloatType(),
                "double": DoubleType(), "boolean": BooleanType(), "struct": StructType(), "array": ArrayType,
                "date": DateType()
                     }



# TODO: We should get all the data types from pyspark.sql.types something like tuple(t.__all__)
SPARK_DTYPES = (StringType, IntegerType, FloatType, DoubleType, ArrayType)


# Strings and Function Messages
JUST_CHECKING = "Just checking that all necessary environments vars are present..."
STARTING_SPARK = "Starting or getting SparkSession and SparkContext..."
STARTING_OPTIMUS = "Transform and Roll out..."


def print_check_point_config(filesystem):
    return logging.info(
        "Setting checkpoint folder %s. If you are in a cluster initialize Optimus with master='your_ip' as param",
        filesystem)


SUCCESS = "Optimus successfully imported. Have fun :)."
