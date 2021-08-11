import numpy as np


class BaseConstants:

    DTYPES_ALIAS = {"str": "string",
                    "vector": "object",
                    "list": "object",
                    "dict": "object",
                    "datetime": "datetime64[ns]",
                    "date": "datetime64[ns]",
                    "time": "datetime64[ns]",
                    "timestamp": "datetime64[ns]"}

    INFERRED_DTYPES_ALIAS = {"string": "str",
                             "float": "decimal",
                             "numeric": "decimal",
                             "vector": "object",
                             "list": "array",
                             "dict": "object",
                             "date": "datetime",
                             "time": "datetime",
                             "timestamp": "datetime"}

    DTYPES_DICT = {"string": np.str, "uint8": np.uint8, "uint16": np.uint16, "uint32": np.uint32,
                   "uint64": np.uint64, "int8": np.int8, "int16": np.int16, "int32": np.int32, "int64": np.int64,
                   "float": np.float, "float64": np.float64, "boolean": np.bool, "array": np.array,
                   "bigint": np.int64, "object": np.object_}

    SHORT_DTYPES = {"str": "string",
                    "integer": "int",
                    "big": "bigint",
                    "long": "bigint",
                    "bool": "boolean", # TO DO: bool: (True, False), boolean: (True, False, Null)
                    "timestamp": "datetime"
                    }

    NUMERIC_TYPES = ["int8", "int16", "int32", "int64",
                     "uint8", "uint16", "uint32", "uint64", "float64"]
    DTYPES_TO_INFERRED = {"int": ["int64", "int32"], "float": [
        "float64", "float"], "object": ["object"]}

    STRING_TYPES = ["string", "object"]
    OBJECT_TYPES = ["object"]


LIMIT = 1000
LIMIT_TABLE = 10
NUM_PARTITIONS = 10
SAMPLE_NUMBER = 10000
