# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import numpy as np
import pandas as pd

# from fast_histogram import histogram1d
from optimus.engines.jit import numba_histogram
from optimus.helpers.core import val_to_list

DataFrame = pd.DataFrame


def functions(self):
    class Functions:

        @staticmethod
        def min(columns, args, df):
            # columns = args
            return [{"min": df[columns].min()}]

        @staticmethod
        def max(columns, args, df):
            # columns = args
            return [{"max": df[columns].max()}]

        @staticmethod
        def mean(columns, args):
            def dataframe_mean_(df):
                return {"mean": df[columns].mean()}

            return dataframe_mean_

        @staticmethod
        def variance(columns, args):
            def dataframe_var_(df):
                return {"variance": df[columns].var()}

            return dataframe_var_

        @staticmethod
        def sum(columns, args):

            def dataframe_sum_(df):
                return {"sum": df[columns].sum()}

            return dataframe_sum_

        @staticmethod
        def percentile_agg(columns, args):
            values = args[1]

            def _percentile(df):
                return {"percentile": df[columns].quantile(values)}

            return _percentile

        @staticmethod
        def stddev(col_name, args):
            def _stddev(serie):
                return {"stddev": {col: serie[col].std() for col in col_name}}

            return _stddev

        @staticmethod
        def zeros_agg(col_name, args, df):
            col_name = val_to_list(col_name)

            def zeros_(df):
                result = {"zeros": {col: (df[col].values == 0).sum() for col in col_name}}
                # result = {"zeros": (df[col_name].values == 0).sum()}
                return result

            return zeros_

        @staticmethod
        def count_na_agg(columns, args, df):
            return [{"count_na": df.cols.count_na(columns)}]

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

                    # _min, _max = _min_max[col_name]["min"], _min_max[col_name]["min"]
                    # i, j = numba_histogram(_serie, bins=buckets, range=(_min, _max), )
                    i, j = numba_histogram(_serie, bins=buckets)

                    result_hist.update({col_name: {"count": list(i), "bins": list(j)}})

                    r = []
                    for idx, v in enumerate(j):
                        if idx < len(j) - 1:
                            r.append({"count": i[idx], "lower": j[idx], "upper": j[idx + 1]})

                    f = {col_name: {"hist": r}}
                    result.update(f)

            return result

        @staticmethod
        def kurtosis(columns, args):
            # Maybe we could contribute with this
            # `nan_policy` other than 'propagate' have not been implemented.

            def _kurtosis(serie):
                result = {"kurtosis": {col: float(stats.kurtosis(serie[col])) for col in columns}}
                # result = {"kurtosis": float(stats.kurtosis(serie[col_name], nan_policy="propagate"))}
                return result

            return _kurtosis

        @staticmethod
        def skewness(columns, args):
            def _skewness(serie):
                result = {"skewness": {col: float(stats.skew(serie[col])) for col in columns}}
                # result = {"skewness": float(stats.skew(serie[col_name], nan_policy="propagate"))}
                return result

            return _skewness

        @staticmethod
        def count_uniques_agg(col_name, args, df):
            print("ARGS", args)
            estimate = args[0]

            if estimate is True:
                ps = df.cols.nunique(col_name)
                # ps = pd.Series({col: df[col].nunique_approx() for col in df.cols.names()})
            else:
                ps = df.cols.nunique(col_name)
            result = [{"count_uniques": ps}]

            return result

        @staticmethod
        def range_agg(columns, args):
            def _dataframe_range_agg_(df):
                return {"min": df[columns].min(), "max": df[columns].max()}

            return _dataframe_range_agg_

        @staticmethod
        def mad_agg(col_name, args):
            more = args[0]

            def _mad_agg(serie):
                median_value = serie[col_name].quantile(0.5)
                mad_value = (serie[col_name] - median_value).abs().quantile(0.5)

                _mad = {}
                if more:
                    result = {"mad": mad_value, "median": median_value}
                else:
                    result = {"mad": mad_value}

                return result

            return _mad_agg

    return Functions()


DataFrame.functions = property(functions)
