from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import StringType, BooleanType, IntegerType, ArrayType, FloatType, DoubleType, StructType, \
    DateType, LongType, ByteType, ShortType, TimestampType, BinaryType, NullType

from optimus.helpers.logger import logger

# Python to PySpark reference
#
# type(None): NullType,
# bool: BooleanType,
# int: LongType,
# float: DoubleType,
# str: StringType,
# bytearray: BinaryType,
# decimal.Decimal: DecimalType,
# datetime.date: DateType,
# datetime.datetime: TimestampType,
# datetime.time: TimestampType,


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

# Profiler
PROFILER_COLUMN_TYPES = {"categorical", "numeric", "date", "null", "array", "binary"}

SPARK_DTYPES_TO_PROFILER = {"int": ["smallint", "tinyint", "bigint", "int"], "decimal": ["float", "double"],
                            "string": "string", "date": {"date", "timestamp"}, "boolean": "boolean", "binary": "binary",
                            "array": "array", "object": "object", "null": "null", "missing": "missing"}

from enum import Enum


class ProfilerDataTypes(Enum):
    INT = "int"
    DECIMAL = "decimal"
    STRING = "string"
    BOOLEAN = "boolean"
    DATE = "date"
    ARRAY = "array"
    OBJECT = "object"
    GENDER = "gender"
    IP = "ip"
    URL = "url"
    EMAIL = "email"
    CREDIT_CARD_NUMBER = "credit_card_number"
    ZIP_CODE = "zip_code"
    NULL = "null"
    MISSING = "missing"


# Strings and Function Messages
JUST_CHECKING = "Just check that Spark and all necessary environments vars are present..."
STARTING_SPARK = "Starting or getting SparkSession and SparkContext..."
STARTING_OPTIMUS = "Transform and Roll out..."

SUCCESS = "Optimus successfully imported. Have fun :)."

CONFIDENCE_LEVEL_CONSTANT = [50, .67], [68, .99], [90, 1.64], [95, 1.96], [99, 2.57]


def print_check_point_config(filesystem):
    logger.print(
        "Setting checkpoint folder %s. If you are in a cluster initialize Optimus with master='your_ip' as param",
        filesystem)


SPARK_VERSION = "2.4.1"
HADOOP_VERSION = "2.7"

SPARK_FILE = "spark-{SPARK_VERSION}-bin-hadoop{HADOOP_VERSION}.tgz".format(SPARK_VERSION=SPARK_VERSION,
                                                                           HADOOP_VERSION=HADOOP_VERSION)
SPARK_URL = "https://archive.apache.org/dist/spark/spark-{SPARK_VERSION}//{SPARK_FILE}".format(
    SPARK_VERSION=SPARK_VERSION, SPARK_FILE=SPARK_FILE)

# For Google Colab
SPARK_PATH_COLAB = "/content/spark-{SPARK_VERSION}-bin-hadoop{HADOOP_VERSION}".format(SPARK_VERSION=SPARK_VERSION,
                                                                                      HADOOP_VERSION=HADOOP_VERSION)
JAVA_PATH_COLAB = "/usr/lib/jvm/java-8-openjdk-amd64"
RELATIVE_ERROR = 10000
