# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import numpy as np
import pandas as pd

# from fast_histogram import histogram1d
from optimus.engines.jit import numba_histogram

DataFrame = pd.DataFrame


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

            result = {}
            result_hist = {}
            _min_max = df.cols.min_max(columns)

            for col_name in columns:
                _serie = df[col_name].to_numpy()
                if df[col_name].dtype == np.float64 or df[col_name].dtype == np.int64:

                    i, j = numba_histogram(_serie, bins=buckets)

                    result_hist.update({col_name: {"count": list(i), "bins": list(j)}})

                    r = []
                    for idx, v in enumerate(j):
                        if idx < len(j) - 1:
                            r.append({"count": float(i[idx]), "lower": float(j[idx]), "upper": float(j[idx + 1])})

                    f = {col_name: {"hist": r}}
                    result.update(f)

            return result

    return Functions()


DataFrame.functions = property(functions)
