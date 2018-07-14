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

# Instead StringType() just use string
DICT_TYPES = {'string': StringType(), 'int': IntegerType(), 'float': FloatType(), 'double': DoubleType()}

# FIX: We should get all the datatypes from pyspark.sql.types something like tuple(t.__all__)
VAR_TYPES = (StringType, IntegerType, FloatType, DoubleType)

JUST_CHECKING = "Just checking that all necessary environments vars are present..."
STARTING = "Starting or getting SparkSession and SparkContext..."


def print_check_point_config(filesystem):
    print("Setting checkpoint folder (", filesystem,"). If you are in a cluster initialize optimus with")


SUCCESS = "Optimus successfully imported. Have fun :)."
