import pandas as pd
from pyspark.sql.types import StructField, StructType, StringType

# Helpers
from optimus.helpers.checkit import is_tuple, is_, is_list, is_one_element, is_list_of_tuples
from optimus.helpers.functions import get_spark_dtypes_object
from optimus.spark import Spark


class Create:

    @staticmethod
    def data_frame(cols=None, rows=None, pdf=None):
        """
        Helper to create a Spark dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples if vals with the same number and types that cols
        :param pdf: a pandas dataframe
        :return: Dataframe
        """
        if is_(pdf, pd.DataFrame):
            result = Spark.instance.spark.createDataFrame(pdf)
        else:

            specs = []
            # Process the rows
            if not is_list_of_tuples(rows):
                rows = [(i,) for i in rows]

            # Process the columns
            for c in cols:
                # Get columns name

                if is_one_element(c):
                    col_name = c
                    var_type = StringType()
                    nullable = True

                elif is_tuple(c):

                    # Get columns data type
                    col_name = c[0]
                    var_type = get_spark_dtypes_object(c[1])

                    count = len(c)
                    if count == 2:
                        nullable = True
                    elif count == 3:
                        nullable = c[2]

                # If tuple has not the third param with put it to true to accepts Null in columns
                specs.append([col_name, var_type, nullable])

            struct_fields = list(map(lambda x: StructField(*x), specs))

            result = Spark.instance.spark.createDataFrame(rows, StructType(struct_fields))

        return result

    df = data_frame
