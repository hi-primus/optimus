from pyspark.sql.types import StructField, StructType

# Helpers
from optimus.helpers.constants import *


class Create:
    def __init__(self, spark=None):
        self.spark = spark

    def dataframe(self, rows_data, col_specs):
        """
        Helper to create a Spark dataframe
        :param rows_data:
        :param col_specs:
        :return:
        """
        specs = []
        for c in col_specs:
            value = c[1]
            # Try to find if the type var is a Spark datatype
            if isinstance(value, (VAR_TYPES)):
                var_type = value
            # else, try to parse a str, int, float ......
            else:
                var_type = DICT_TYPES[TYPES[c[1]]]

            specs.append([c[0], var_type, c[2]])

        struct_fields = list(map(lambda x: StructField(*x), specs))

        return self.spark.createDataFrame(rows_data, StructType(struct_fields))

    # Alias
    df = dataframe
