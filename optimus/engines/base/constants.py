from optimus.helpers.constants import ProfilerDataTypes
import numpy as np


class BaseConstants:

    # inferred/input to internal
    OPTIMUS_TO_INTERNAL = {ProfilerDataTypes.INT.value: "int",
                           ProfilerDataTypes.FLOAT.value: "float",
                           ProfilerDataTypes.STRING.value: "str",
                           ProfilerDataTypes.BOOL.value: "bool",
                           ProfilerDataTypes.BOOLEAN.value: "bool",
                           ProfilerDataTypes.DATETIME.value: "datetime64[ns]",
                           ProfilerDataTypes.ARRAY.value: "object",
                           ProfilerDataTypes.OBJECT.value: "object",
                           ProfilerDataTypes.GENDER.value: "gender",
                           ProfilerDataTypes.IP.value: "str",
                           ProfilerDataTypes.URL.value: "str",
                           ProfilerDataTypes.EMAIL.value: "str",
                           ProfilerDataTypes.CREDIT_CARD_NUMBER.value: "str",
                           ProfilerDataTypes.ZIP_CODE.value: "object",
                           ProfilerDataTypes.MISSING.value: "object",
                           ProfilerDataTypes.PHONE_NUMBER.value: "str",
                           ProfilerDataTypes.SOCIAL_SECURITY_NUMBER.value: "str",
                           ProfilerDataTypes.HTTP_CODE.value: "str",
                           ProfilerDataTypes.US_STATE.value: "category",
                           ProfilerDataTypes.CATEGORICAL.value: "category",
                           ProfilerDataTypes.NULL.value: "object",
                           "double": "float",
                           "vector": "object",
                           "list": "object",
                           "dict": "object",
                           "date": "datetime64[ns]",
                           "time": "datetime64[ns]",
                           "timestamp": "datetime64[ns]"}

    # internal to inferred
    INTERNAL_TO_OPTIMUS = {"string_": ProfilerDataTypes.STRING.value,
                           "large_string": ProfilerDataTypes.STRING.value,
                           "unicode_": ProfilerDataTypes.STRING.value,
                           "string": ProfilerDataTypes.STRING.value,
                           "int_": ProfilerDataTypes.INT.value,
                           "bigint": ProfilerDataTypes.INT.value,
                           "int8": ProfilerDataTypes.INT.value,
                           "int16": ProfilerDataTypes.INT.value,
                           "int32": ProfilerDataTypes.INT.value,
                           "int64": ProfilerDataTypes.INT.value,
                           "uint": ProfilerDataTypes.INT.value,
                           "uint8": ProfilerDataTypes.INT.value,
                           "uint16": ProfilerDataTypes.INT.value,
                           "uint32": ProfilerDataTypes.INT.value,
                           "uint64": ProfilerDataTypes.INT.value,
                           "binary": ProfilerDataTypes.INT.value,
                           "large_binary": ProfilerDataTypes.INT.value,
                           "numeric": ProfilerDataTypes.FLOAT.value,
                           "float": ProfilerDataTypes.FLOAT.value,
                           "float16": ProfilerDataTypes.FLOAT.value,
                           "float32": ProfilerDataTypes.FLOAT.value,
                           "float64": ProfilerDataTypes.FLOAT.value,
                           "float_": ProfilerDataTypes.FLOAT.value,
                           "double": ProfilerDataTypes.FLOAT.value,
                           "bool_": ProfilerDataTypes.BOOL.value,
                           "date": ProfilerDataTypes.DATETIME.value,
                           "date32": ProfilerDataTypes.DATETIME.value,
                           "date64": ProfilerDataTypes.DATETIME.value,
                           "time": ProfilerDataTypes.DATETIME.value,
                           "time32": ProfilerDataTypes.DATETIME.value,
                           "time64": ProfilerDataTypes.DATETIME.value,
                           "timestamp": ProfilerDataTypes.DATETIME.value,
                           "datetime64": ProfilerDataTypes.DATETIME.value,
                           "datetime64[ns]": ProfilerDataTypes.DATETIME.value,
                           "datetime[64]": ProfilerDataTypes.DATETIME.value,
                           "timedelta[ns]": ProfilerDataTypes.DATETIME.value,
                           "category": ProfilerDataTypes.CATEGORICAL.value,
                           "list": ProfilerDataTypes.ARRAY.value,
                           "vector": ProfilerDataTypes.OBJECT.value,
                           "dict": ProfilerDataTypes.OBJECT.value}

    # short to internal 
    SHORT_DTYPES = {"str": "string",
                    "integer": "int",
                    "big": "bigint",
                    "long": "bigint",
                    "bool": "boolean", # TO DO: bool: (True, False), boolean: (True, False, Null)
                    "timestamp": "datetime"
                    }
                     
    @property
    def OPTIMUS_TO_INTERNALS(self):
        values = set(list(self.INTERNAL_TO_OPTIMUS.values()) + ProfilerDataTypes.list())
        
        _dict = {}
        for v in values:
            _list = [self.OPTIMUS_TO_INTERNAL[v]]
            _list += [t[0] for t in self.INTERNAL_TO_OPTIMUS.items() if t[1] == v]
            _dict.update({v: list(set(_list))})
            
        return _dict

    ANY_TYPES = ["object"]

    @property
    def INT_INTERNAL_TYPES(self):
        return [ProfilerDataTypes.INT.value] + [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] == ProfilerDataTypes.INT.value]

    @property
    def NUMERIC_INTERNAL_TYPES(self):
        types = [ProfilerDataTypes.INT.value, ProfilerDataTypes.FLOAT.value]
        return types + [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] in types]

    @property
    def STRING_INTERNAL_TYPES(self):
        return [ProfilerDataTypes.STRING.value] + [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] == ProfilerDataTypes.STRING.value]
    
    @property
    def DATETIME_INTERNAL_TYPES(self):
        return [ProfilerDataTypes.DATETIME.value] +\
               [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] == ProfilerDataTypes.DATETIME.value]

    @property
    def INT_TYPES(self):
        return self.ANY_TYPES + [ProfilerDataTypes.INT.value] +\
               [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] == ProfilerDataTypes.INT.value]

    @property
    def NUMERIC_TYPES(self):
        types = [ProfilerDataTypes.INT.value, ProfilerDataTypes.FLOAT.value]
        return self.ANY_TYPES + types +\
               [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] in types]

    @property
    def STRING_TYPES(self):
        return self.ANY_TYPES + [ProfilerDataTypes.STRING.value] +\
               [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] == ProfilerDataTypes.STRING.value]
    
    @property
    def DATETIME_TYPES(self):
        return self.ANY_TYPES + [ProfilerDataTypes.DATETIME.value] +\
               [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] == ProfilerDataTypes.DATETIME.value]

    @property
    def OBJECT_TYPES(self):
        types = [ProfilerDataTypes.OBJECT.value, ProfilerDataTypes.ARRAY.value]
        return self.ANY_TYPES + types +\
               [item[0] for item in self.INTERNAL_TO_OPTIMUS.items() if item[1] in types]

    COMPATIBLE_DTYPES = {}


LIMIT = 1000
LIMIT_TABLE = 10
NUM_PARTITIONS = 10
SAMPLE_NUMBER = 10000
