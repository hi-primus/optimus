# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import cupy as cp
import numpy as np
from cudf.core import DataFrame

from optimus.helpers.core import val_to_list


def functions(self):
    class Functions:

        # def hist_agg(col_name, df, buckets, min_max=None, dtype=None):
        @staticmethod
        def hist_agg(columns, args, df):
            # {'OFFENSE_CODE': {'hist': [{'count': 169.0, 'lower': 111.0, 'upper': 297.0},
            #                            {'count': 20809.0, 'lower': 3645.0, 'upper': 3831.0}]}}

            df = args[0]
            buckets = args[1]
            min_max = args[2]

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
                    result[col_name] = {"count": list([float(x) for x in i]), "bins": list([float(x) for x in j])}
            return result

    return Functions()


DataFrame.functions = property(functions)
