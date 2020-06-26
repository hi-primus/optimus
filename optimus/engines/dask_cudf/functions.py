# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

import cupy as cp
import dask
from dask.array import stats
from dask_cudf.core import DataFrame as DaskCUDFDataFrame

from optimus.helpers.check import is_column_a
from optimus.helpers.raiseit import RaiseIt


def functions(self):
    class Functions:

        # def hist_agg(col_name, df, buckets, min_max=None, dtype=None):
        # @staticmethod
        # def hist_agg(columns, args):
        #     # {'OFFENSE_CODE': {'hist': [{'count': 169.0, 'lower': 111.0, 'upper': 297.0},
        #     #                            {'count': 20809.0, 'lower': 3645.0, 'upper': 3831.0}]}}
        #     def str_len(serie):
        #         return serie.str.len()
        #
        #     def value_counts(serie):
        #         return serie.value_counts().sort_index().reset_index()
        #
        #     def min_max_string_func(serie):
        #         return [serie.min()[0], serie.max()[0]]
        #
        #     def map_delayed(df, func, *args, **kwargs):
        #         # if isinstance(df, dask.dataframe.core.Series):
        #         #     print(type(df))
        #         #     partitions = df.to_delayed()
        #         #     delayed_values = [dask.delayed(func)(part, *args, **kwargs)
        #         #                       for part in partitions][0]
        #         # else:
        #         delayed_values = dask.delayed(func)(df, *args, **kwargs)
        #
        #         return delayed_values
        #
        #     def hist_serie(serie, buckets):
        #
        #         arr = cp.fromDlpack(serie.to_dlpack())
        #         i, j = cp.histogram(arr, buckets)
        #         # We need to convert from array to numeric
        #         return {"count": list([float(x) for x in i]), "bins": list([float(x) for x in j])}
        #
        #     def hist_agg_(df):
        #         # df = args[0]
        #         buckets = args[1]
        #         min_max = args[2]
        #         result_min_max = {}
        #         delayed_results = []
        #
        #         for col_name in columns:
        #             calc = None
        #             if min_max is not None:
        #                 calc = min_max.get(col_name)
        #
        #             if calc is None or min_max is None:
        #                 if is_column_a(df, col_name, df.constants.STRING_TYPES):
        #                     a = map_delayed(df[col_name], str_len)
        #                     b = map_delayed(a, value_counts)
        #                     c = dask.delayed(min_max_string_func)(b)
        #                     f = map_delayed(b["index"], hist_serie, buckets)
        #                 #
        #                 elif is_column_a(df, col_name, df.constants.NUMERIC_TYPES):
        #                     f = map_delayed(df[col_name], hist_serie, buckets)
        #                 else:
        #                     RaiseIt.type_error("column", ["numeric", "string"])
        #                 delayed_results.append({col_name: f})
        #
        #         r = {}
        #         # Convert list to dict
        #         for i in dask.compute(*delayed_results):
        #             r.update(i)
        #
        #         return {"hist": r}
        #
        #     return hist_agg_

        @staticmethod
        kurtosis(col_name, args):
            def _kurtoris(serie):
                result = {"kurtosis": float(stats.kurtosis(serie[col_name]))}
                return result

            return _kurtoris

        @staticmethod
        def skewness(col_name, args):
            def _skewness(serie):
                result = {"skewness": float(stats.skew(serie[col_name]))}
                return result

            return _skewness

    return Functions()


DaskCUDFDataFrame.functions = property(functions)
