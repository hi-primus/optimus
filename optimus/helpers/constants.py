from pyspark.sql.types import *

# You can use string, str or String as param
TYPES = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
         'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

# Instead StringType() just use string
DICT_TYPES = {'string': StringType(), 'int': IntegerType(), 'float': FloatType(), 'double': DoubleType()}

VAR_TYPES = (StringType, IntegerType, FloatType, DoubleType)

JUST_CHECKING = "Just checking that all necessary environments vars are present..."
STARTING = "Starting or getting SparkSession and SparkContext..."
CHECK_POINT_CONFIG = "Setting checkpoint folder (local). If you are in a cluster change it with set_check_point_ " \
                     "folder(path,'hadoop')."
SUCCESS = "Optimus successfully imported. Have fun :)."
