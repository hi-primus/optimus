from pyspark.sql import types as t
from pyspark.sql.types import StringType, BooleanType, IntegerType, ArrayType, FloatType, DoubleType

# You can use string, str or String as param
TYPES = {'string': 'string',
         'str': 'string',
         'String': 'string',
         'integer': 'int',
         'int': 'int',
         'float': 'float',
         'double': 'double',
         'Double': 'double'}

TYPES_PROFILER = {'integer', 'float', 'string', 'boolean', 'null'}

DICT_TYPES = {'string': StringType(), 'int': IntegerType(), 'float': FloatType(), 'double': DoubleType()}

# TODO: We should get all the data types from pyspark.sql.types something like tuple(t.__all__)
VAR_TYPES = (StringType, IntegerType, FloatType, DoubleType)

TYPES_LEGEND = {"string": "ABC", "int": "#", "integer": "#", "float": "##.#", "double": "##.#", "bigint": "#"}

# Strings Messages

JUST_CHECKING = "Just checking that all necessary environments vars are present..."
STARTING = "Starting or getting SparkSession and SparkContext..."


def print_check_point_config(filesystem):
    print("Setting checkpoint folder (", filesystem,
          "). If you are in a cluster initialize optimus with master='your_ip' as param")


SUCCESS = "Optimus successfully imported. Have fun :)."
