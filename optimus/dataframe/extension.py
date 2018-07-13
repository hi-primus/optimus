from pyspark.sql.types import StructField, StructType

# Helpers
from optimus.helpers.constants import *
from optimus.spark import get_spark


class Create:

    @staticmethod
    def data_frame(col_specs, rows_data):
        """
        Helper to create a Spark dataframe
        :param col_specs:
        :param rows_data:
        :return:
        """
        specs = []
        for c in col_specs:
            value = c[1]
            # Try to find if the type var is a Spark datatype
            if isinstance(value, VAR_TYPES):
                var_type = value
            # else, try to parse a str, int, float ......
            else:
                var_type = DICT_TYPES[TYPES[c[1]]]

            specs.append([c[0], var_type, c[2]])

        struct_fields = list(map(lambda x: StructField(*x), specs))

        return get_spark().createDataFrame(rows_data, StructType(struct_fields))

    # Alias
    df = data_frame
