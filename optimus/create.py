from functools import reduce

from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Helpers
# from optimus.helpers.constants import *
from optimus.helpers.functions import *
from optimus.spark import Spark



class Create:

    @staticmethod
    def data_frame(cols, rows):
        """
        Helper to create a Spark dataframe
        :param cols:
        :param rows:
        :return:
        """
        specs = []
        for c in cols:
            value = c[1]
            # Try to find if the type var is a Spark datatype
            if isinstance(value, VAR_TYPES):
                var_type = value
            # else, try to parse a str, int, float ......
            else:
                var_type = parse_spark_dtypes(c[1])

            specs.append([c[0], var_type, c[2]])

        struct_fields = list(map(lambda x: StructField(*x), specs))

        return Spark.instance.get_ss().createDataFrame(rows, StructType(struct_fields))

    @staticmethod
    def concat(dfs, axis=0):
        """
        Concat columns or rows to a dataframe
        :param dfs:
        :param axis;
        :return:
        """
        # Add increasing Ids, and they should be the same.
        if axis == 1:
            df_result = []
            col_temp_name = "id_" + random_name()
            for df in dfs:
                df_result.append(df.withColumn(col_temp_name, F.monotonically_increasing_id()))

            def _append_df(df1, df2):
                return df2.join(df1, col_temp_name, "outer").drop(col_temp_name)

            result = reduce(_append_df, df_result)
        else:
            result = reduce(DataFrame.union, dfs)
        return result

        # Alias

    df = data_frame
