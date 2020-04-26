from enum import Enum

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
# Profiler


class Actions(Enum):
    """
    Actions that modify a columns/rows.
    """
    # COLUMNS
    LOWER = "lower"
    UPPER = "upper"
    TRIM = "trim"
    REVERSE = "reverse"
    REMOVE_ACCENTS = "remove"
    REMOVE_SPECIAL_CHARS = "remove"
    REMOVE_WHITE_SPACES = "remove"
    REPLACE = "replace"
    REPLACE_REGEX = "replace"
    FILL_NA = "fill_na"
    CAST = "cast"
    IS_NA = "is_na"
    Z_SCORE = "z_score"
    NEST = "nest"
    UNNEST = "unnest"
    VALUES_TO_COLS = "values_to_cols"
    SET = "set"
    STRING_TO_INDEX = "string_to_index"
    INDEX_TO_STRING = "index_to_string"
    MIN_MAX_SCALER = "min_max_scaler"
    MAX_ABS_SCALER = "max_abs_scaler"
    APPLY_COLS = "apply_cols"

    # ROWS
    SELECT_ROW = "select_row"
    DROP_ROW = "drop_row"
    BETWEEN_ROW = "between_drop"
    SORT_ROW = "sort_row"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Actions))


class ProfilerDataTypesQuality(Enum):
    MISMATCH = 0
    MISSING = 1
    MATCH = 2


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
    MISSING = "missing"
    CATEGORICAL = "categorical"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, ProfilerDataTypes))

    # NULL = "null"
    # MISSING = "missing"


# Strings and Function Messages
JUST_CHECKING = "Just check that all necessary environments vars are present..."
STARTING_OPTIMUS = "Transform and Roll out..."

SUCCESS = "Optimus successfully imported. Have fun :)."

CONFIDENCE_LEVEL_CONSTANT = [50, .67], [68, .99], [90, 1.64], [95, 1.96], [99, 2.57]


def print_check_point_config(filesystem):
    logger.print(
        "Setting checkpoint folder %s. If you are in a cluster initialize Optimus with master='your_ip' as param",
        filesystem)


# For Google Colab
JAVA_PATH_COLAB = "/usr/lib/jvm/java-8-openjdk-amd64"
RELATIVE_ERROR = 10000

# Buffer size in rows
BUFFER_SIZE = 500000
