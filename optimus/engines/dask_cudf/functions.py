# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

from dask_cudf.core import DataFrame as DaskCUDFDataFrame


def functions(self):
    class Functions:

        @staticmethod
        def kurtosis(col_name, args):
            raise NotImplementedError("Not implemented yet")

        @staticmethod
        def skewness(col_name, args):
            raise NotImplementedError("Not implemented yet")

    return Functions()


DaskCUDFDataFrame.functions = property(functions)
