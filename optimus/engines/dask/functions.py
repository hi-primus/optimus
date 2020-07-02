# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import dask
from dask import delayed
from dask.array import stats
from dask.dataframe.core import DataFrame
import numpy as np
from optimus.helpers.check import is_column_a
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import format_dict
from optimus.helpers.core import val_to_list
from optimus.helpers.raiseit import RaiseIt
from fast_histogram import histogram1d


def functions(self):
    class Functions:

        @staticmethod
        def kurtosis(df, columns, args):
            # Maybe we could contribute with this
            # `nan_policy` other than 'propagate' have not been implemented.

            f = {col_name: stats.kurtosis(df[col_name]) for col_name in columns}

            @delayed
            def _kurtosis(_f):
                return {"kurtosis": _f}

            return _kurtosis(f)

        @staticmethod
        def skewness(df, columns, args):

            f = {col_name: float(stats.skew(df[col_name])) for col_name in columns}

            @delayed
            def _skewness(_f):
                return {"skewness": _f}

            return _skewness(f)



    return Functions()


DataFrame.functions = property(functions)
