from pyspark.sql.types import StructField, StructType


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
        struct_fields = list(map(lambda x: StructField(*x), col_specs))
        return self.spark.createDataFrame(rows_data, StructType(struct_fields))

    # Alias
    df = dataframe
