import numpy as np
from dask.dataframe.core import DataFrame


def constants(self):
    class Constants:
        DTYPES_DICT = {"string": np.str, "int32": np.int32, "int64": np.int64, "float": np.float, "float64": np.float64,
                       "boolean": np.bool, "array": np.array,
                       "bigint": np.int64, "object": np.object}

        SHORT_DTYPES = {"string": "string",
                        "str": "string",
                        "integer": "int",
                        "int": "int",
                        "int64": "int64",
                        "int32": "int32",
                        "float64": "float64",
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
                        "timestamp": "datetime",
                        "object": "object"
                        }

        STRING_TYPES = ["object"]
        NUMERIC_TYPES = ["int32", "int64", "float64"]
        DTYPES_TO_PROFILER = {"int": ["int64", "int32"], "float": ["float64"]}

    return Constants()


DataFrame.constants = property(constants)
