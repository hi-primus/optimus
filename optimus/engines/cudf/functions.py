# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import cupy as cp
import numpy as np
from cudf.core import DataFrame

from optimus.helpers.core import val_to_list


def functions(self):
    class Functions:

        @staticmethod
        def sum(columns, args):

            def dataframe_sum_(df):
                return {"sum": df[columns].sum()}

            return dataframe_sum_





        # def hist_agg(col_name, df, buckets, min_max=None, dtype=None):
        @staticmethod
        def hist_agg(columns, args, df):
            # {'OFFENSE_CODE': {'hist': [{'count': 169.0, 'lower': 111.0, 'upper': 297.0},
            #                            {'count': 20809.0, 'lower': 3645.0, 'upper': 3831.0}]}}

            df = args[0]
            buckets = args[1]
            min_max = args[2]

            # print("aaaaa",df[columns],buckets, min_max)

            # https://iscinumpy.gitlab.io/post/histogram-speeds-in-python/
            # Fast histograms library https://github.com/astrofrog/fast-histogram

            # @numba.njit
            # def histogram1d(v, bins, range):
            #     return np.histogram(v, bins, range)

            # _serie = df[columns].to_numpy()
            result = {}
            for col_name in columns:
                _serie = df[col_name]
                if df[col_name].dtype == np.float64 or df[col_name].dtype == np.int64:
                    arr = cp.fromDlpack(_serie.to_dlpack())
                    i, j = cp.histogram(arr, buckets)
                    # We need to convert from array to numeric
                    result[col_name]= {"count": list([float(x) for x in i]), "bins": list([float(x) for x in j])}
            return result

            # result_hist = {}
            # for col in col_name:
            #     if is_column_a(df, col, df.constants.STRING_TYPES):
            #         if min_max is None:
            #             def func(val):
            #                 return val.str.len()
            #
            #             partitions = df[col].to_delayed()
            #             delayed_values = [dask.delayed(func)(part)
            #                               for part in partitions]
            #             df_len = from_delayed(delayed_values)
            #             df_len = df_len.value_counts()
            #             min, max = dd.compute(df_len.min(), df_len.max())
            #             min_max = {"min": min, "max": max}
            #         df_hist = df_len
            #
            #     elif is_column_a(df, col, df.constants.NUMERIC_TYPES):
            #         if min_max is None:
            #             min_max = df.cols.range(col_name)[col]
            #         df_hist = serie[col]
            #     else:
            #         RaiseIt.type_error("column", ["numeric", "string"])
            #
            #     i, j = (da.histogram(df_hist, bins=buckets, range=[min_max["min"], min_max["max"]]))
            #     result_hist.update({col: {"count": list(i), "bins": list(j)}})

            return hist_agg_



        @staticmethod
        def count_uniques_agg(col_name, args, df):
            print("ARGS", args)
            estimate = args[0]

            if estimate is True:
                ps = df.cols.nunique_approx(col_name)
                # ps = pd.Series({col: df[col].nunique_approx() for col in df.cols.names()})
            else:
                ps = df.cols.nunique(col_name)
            result = [{"count_uniques": ps}]
            return result

    return Functions()


DataFrame.functions = property(functions)
