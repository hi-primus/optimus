# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

import cudf
import dask
import numpy as np
from dask import dataframe as dd
from dask.array import stats
from dask.dataframe import from_delayed
from dask_cudf.core import DataFrame as DaskCUDFDataFrame

from optimus.helpers.check import is_column_a
from optimus.helpers.raiseit import RaiseIt


def functions(self):
    class Functions:

        @staticmethod
        def min(columns, args):
            def dataframe_min_(df):
                return {"min": df[columns].min()}

            return dataframe_min_

        @staticmethod
        def max(columns, args):
            def dataframe_max_(df):
                return {"max": df[columns].max()}

            return dataframe_max_

        @staticmethod
        def mean(columns, args):
            def dataframe_mean_(df):
                return {"mean": df[columns].mean()}

            return dataframe_mean_

        @staticmethod
        def variance(columns, args):
            def dataframe_var_(df):
                return {"var": df[columns].var()}

            return dataframe_var_

        @staticmethod
        def sum(columns, args):

            def dataframe_sum_(df):
                return {"sum": df[columns].sum()}

            return dataframe_sum_

        @staticmethod
        def percentile_agg(columns, args):
            print("PERCENTILE")
            values = args[1]

            def _percentile(df):
                return {"percentile": df[columns].quantile(values)}

            return _percentile

        @staticmethod
        def stddev(col_name, args):
            def std_(serie):
                return {"stddev": serie[col_name].std()}

            return std_

        # @staticmethod
        # def stddev(columns, args):
        #     def dataframe_stddev_(df):
        #         return {"stddev": df[columns].std()}
        #
        #     return dataframe_stddev_

        # @staticmethod
        # def mean(col_name, args):
        #     def _mean(serie):
        #         return {"mean": serie[col_name].mean()}
        #
        #     return _mean
        #
        # @staticmethod
        # def sum(col_name, args):
        #     def std_(serie):
        #         return {"sum": serie[col_name].sum()}
        #
        #     return std_
        #
        # @staticmethod
        # def variance(col_name, args):
        #     def var_(serie):
        #         return {"variance": serie[col_name].var()}
        #
        #     return var_

        @staticmethod
        def zeros_agg(col_name, args):

            def zeros_(serie):
                result = {"zeros": (serie[col_name].values == 0).sum()}
                return result

            return zeros_

        @staticmethod
        def count_na_agg(col_name, args):
            # estimate = args[0]

            def count_na_(serie):
                result = {"count_na": serie[col_name].isnull().sum()}
                return result

            return count_na_

        # def hist_agg(col_name, df, buckets, min_max=None, dtype=None):
        @staticmethod
        def hist_agg(col_name, args):
            # {'OFFENSE_CODE': {'hist': [{'count': 169.0, 'lower': 111.0, 'upper': 297.0},
            #                            {'count': 20809.0, 'lower': 3645.0, 'upper': 3831.0}]}}
            def hist_agg_(serie):
                df = args[0]
                buckets = args[1]
                min_max = args[2]

                result_hist = {}
                for col in col_name:
                    print("COLANEM", col)
                    if is_column_a(df, col, df.constants.STRING_TYPES):
                        if min_max is None:
                            def func(val):
                                return val.str.len()

                            partitions = df[col].to_delayed()
                            delayed_values = [dask.delayed(func)(part)
                                              for part in partitions]
                            df_len = from_delayed(delayed_values)
                            df_len = df_len.value_counts()
                            min, max = dd.compute(df_len.min(), df_len.max())
                            min_max_col = {"min": min, "max": max}
                        df_hist = df_len

                    elif is_column_a(df, col, df.constants.NUMERIC_TYPES):
                        if min_max is None:
                            print("888888888888888888 COL_NAME", col)
                            min_max_col = df.cols.range(col)[col]
                        df_hist = serie[col]
                    else:
                        RaiseIt.type_error("column", ["numeric", "string"])

                    if min_max:
                        min_max_col = min_max

                    def histogram(_df, _col, _bins):

                        binned = _df[_col].digitize(_bins, right=False)
                        vc = binned.value_counts().sort_index()
                        return vc, cudf.Series(_bins)

                    # bins = [-30, 0, 3, 6, 9, 10, 19, 50]
                    bins = np.linspace(min_max_col["min"], min_max_col["max"], buckets - 1, dtype=int)
                    print("BINS", min_max)
                    # r = serie.map_partitions(histogram, col, bins, meta=tuple).compute()

                    partitions = df.to_delayed()
                    delayed_values = [dask.delayed(histogram)(part, col, bins)
                                      for part in partitions]
                    result = from_delayed(delayed_values)
                    print(result)
                    # i, j = (da.histogram(df_hist, bins=buckets, range=[min_max["min"], min_max["max"]]))
                    result_hist.update({col: {"count": [i for i in r[0][0]], "bins": [i for i in r[0][1]]}})

                result = {}
                result['hist'] = result_hist

                print("RESULT", result)
                return result

            return hist_agg_

        @staticmethod
        def kurtosis(col_name, args):
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

        @staticmethod
        def count_uniques_agg(col_name, args):
            estimate = args[0]

            def _count_uniques_agg(serie):
                if estimate is True:
                    result = {"count_uniques": serie[col_name].nunique_approx()}
                else:
                    result = {"count_uniques": serie[col_name].nunique()}
                return result

            return _count_uniques_agg

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


DaskCUDFDataFrame.functions = property(functions)
