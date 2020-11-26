import numpy as np

def constants(self):
    class Constants:
        DTYPES_DICT = {"string": np.str, "uint8": np.uint8, "uint16": np.uint16, "uint32": np.uint32,
                       "uint64": np.uint64, "int8": np.int8, "int16": np.int16, "int32": np.int32, "int64": np.int64,
                       "float": np.float, "float64": np.float64, "boolean": np.bool, "array": np.array,
                       "bigint": np.int64, "object": np.object_}

        SHORT_DTYPES = {"string": "string",
                        "str": "string",
                        "integer": "int",
                        "int": "int",

                        "uint8": "uint8",
                        "uint16": "uint16",
                        "uint32": "uint32",
                        "uint64": "uint64",
                        "int8": "int8",
                        "int16": "int16",
                        "int32": "int32",
                        "int64": "int64",

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

        NUMERIC_TYPES = ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float64"]
        DTYPES_TO_PROFILER = {"int": ["int64", "int32"], "float": ["float64", "float"], "object": ["object"]}

        STRING_TYPES = ["string", "object"]
        OBJECT_TYPES = ["object"]

    return Constants()


STARTING_DASK = "Starting or setting Dask Client..."
