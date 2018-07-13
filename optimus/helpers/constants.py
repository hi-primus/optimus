from pyspark.sql.types import *

# You can use string, str or String as param
TYPES = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
         'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

# Instead StringType() just use string
DICT_TYPES = {'string': StringType(), 'int': IntegerType(), 'float': FloatType(), 'double': DoubleType()}

VAR_TYPES = (StringType, IntegerType, FloatType, DoubleType)