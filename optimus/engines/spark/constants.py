from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import StringType, BooleanType, IntegerType, ArrayType, FloatType, DoubleType, StructType, \
    DateType, LongType, ByteType, ShortType, TimestampType, BinaryType, NullType

from optimus.engines.base.constants import BaseConstants

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


DTYPES_DICT_OBJECTS = \
    {"string": StringType(), "int": IntegerType(), "float": FloatType(),
     "double": DoubleType(), "boolean": BooleanType(), "struct": StructType(), "array": ArrayType(StringType()),
     "bigint": LongType(), "date": DateType(), "byte": ByteType(), "short": ShortType(),
     "datetime": TimestampType(), "binary": BinaryType(), "null": NullType()
     }

SHORT_DTYPES = {"string": "string",
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

SPARK_VERSION = "2.4.1"
HADOOP_VERSION = "2.7"
SPARK_FILE = "spark-{SPARK_VERSION}-bin-hadoop{HADOOP_VERSION}.tgz".format(SPARK_VERSION=SPARK_VERSION,
                                                                           HADOOP_VERSION=HADOOP_VERSION)
SPARK_URL = "https://archive.apache.org/dist/spark/spark-{SPARK_VERSION}//{SPARK_FILE}".format(
    SPARK_VERSION=SPARK_VERSION, SPARK_FILE=SPARK_FILE)
SPARK_PATH_COLAB = "/content/spark-{SPARK_VERSION}-bin-hadoop{HADOOP_VERSION}".format(SPARK_VERSION=SPARK_VERSION,
                                                                                      HADOOP_VERSION=HADOOP_VERSION)
STARTING_SPARK = "Starting or getting SparkSession and SparkContext..."


class Constants(BaseConstants):
    SHORT_DTYPES = SHORT_DTYPES

    DTYPES_DICT = {"string": StringType, "int": IntegerType, "float": FloatType,
                   "double": DoubleType, "boolean": BooleanType, "struct": StructType, "array": ArrayType,
                   "bigint": LongType, "date": DateType, "byte": ByteType, "short": ShortType,
                   "datetime": TimestampType, "binary": BinaryType, "null": NullType, "vector": VectorUDT
                   }

    DTYPES_DICT_OBJECTS = DTYPES_DICT_OBJECTS

    NUMERIC_TYPES = ["byte", "short", "big", "int", "double", "float"]
    NOT_ARRAY_TYPES = ["byte", "short", "big", "int", "double", "float", "string", "date", "bool"]
    STRING_TYPES = ["str", "object"]
    ARRAY_TYPES = ["array"]

    DTYPES_TO_INFERRED = {"int": ["smallint", "tinyint", "bigint", "int"], "float": ["float", "double"],
                          "string": ["string"], "date": ["date", "timestamp"], "boolean": ["boolean"],
                          "binary": ["binary"],
                          "array": ["array"], "object": ["object"], "null": ["null"], "missing": ["missing"]}
