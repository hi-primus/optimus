# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

import dask.dataframe as dd
from dask.dataframe.core import DataFrame


def functions(self):
    class Functions:
        @staticmethod
        def run():
            print("run")

        @staticmethod
        def count_uniques_agg(col_name: DataFrame, estimate=True):
            """

            :param col_name:
            :param estimate:
            :return:
            """

            def chunk(s):
                """
                The function applied to the
                individual partition (map)
                """
                return s.apply(lambda x: list(set(x)))

            def agg(s):
                """
                The function whic will aggrgate
                the result from all the partitions(reduce)
                """
                s = s._selected_obj
                return s.groupby(level=list(range(s.index.nlevels))).sum()

            def finalize(s):
                """
                The optional functional that will be
                applied to the result of the agg_tu functions
                """
                return s.apply(lambda x: len(set(x)))

            tunique = dd.Aggregation('tunique', chunk, agg, finalize)

    return Functions()


DataFrame.functions = property(functions)
# print(DataFrame.functions)
