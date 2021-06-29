import pandas as pd

from optimus.engines.spark.dataframe import SparkDataFrame
from optimus.engines.spark.spark import Spark


class Create:
    def __init__(self, root):
        self.root = root

    def dataframe(self, dict=None, cols=None, rows=None, infer_schema=True, pdf=None, *args, **kwargs):
        """
        Helper to create a Spark dataframe:
        :param dict:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param infer_schema: Try to infer the schema data type.
        :param pdf: a pandas dataframe
        :return: Dataframe
        """
        # if is_(pdf, pd.DataFrame):
        #     df = Spark.instance.spark.createDataFrame(pdf)
        # else:
        #
        #     specs = []
        #     # Process the rows
        #     if not is_list_of_tuples(rows):
        #         rows = [(i,) for i in rows]
        #
        #     # Process the columns
        #     for c, r in zip(cols, rows[0]):
        #         # Get columns name
        #
        #         if is_one_element(c):
        #             col_name = c
        #
        #             if infer_schema is True:
        #                 var_type = Infer.to_spark(r)
        #                 # print(var_type)
        #             else:
        #                 var_type = StringType()
        #             nullable = True
        #
        #         elif is_tuple(c):
        #
        #             # Get columns data type
        #             col_name = c[0]
        #             var_type = parse_spark_class_dtypes(c[1])
        #
        #             count = len(c)
        #             if count == 2:
        #                 nullable = True
        #             elif count == 3:
        #                 nullable = c[2]
        #
        #         # If tuple has not the third param with put it to true to accepts Null in columns
        #         specs.append([col_name, var_type, nullable])
        #
        #     struct_fields = list(map(lambda x: StructField(*x), specs))
        if dict:
            pdf = pd.DataFrame(dict)
        elif pdf is None:
            pdf = pd.DataFrame(kwargs)
            
        df = Spark.instance.spark.createDataFrame(pdf)

        df = SparkDataFrame(df)

        return df
