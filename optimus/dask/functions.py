# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

import dask.array as da
from dask.array import stats
from dask.dataframe.core import DataFrame


def functions(self):
    class Functions:
        @staticmethod
        def run():
            print("run")

        @staticmethod
        def min(col_name, *args):
            def min_(x):
                return {"min": x[col_name].min()}

            return min_

        @staticmethod
        def max(col_name, *args):
            def max_(x):
                return {"max": x[col_name].max()}

            return max_

        @staticmethod
        def stddev(col_name, *args):
            def std_(x):
                return {"stddev": x[col_name].max()}

            return std_

        @staticmethod
        def sum(col_name, *args):
            def std_(x):
                return {"sum": x[col_name].sum()}

            return std_

        @staticmethod
        def variance(col_name, *args):
            def var_(x):
                return {"variance": x[col_name].var()}

            return var_

        @staticmethod
        def zeros_agg(col_name, *args):
            estimate = args[0]

            def zeros_(x):
                result = {"zeros": (x[col_name].values == 0).sum()}
                return result

            return zeros_

        @staticmethod
        def count_na_agg(col_name, *args):
            # estimate = args[0]

            def count_na_(x):
                result = {"count_na": x[col_name].isnull().sum()}
                return result

            return count_na_

        @staticmethod
        def hist_agg(col_name, args):
            df = args[0]
            bins = args[1]
            range = args[2]

            def hist_agg_(x):
                # return {"hist_agg": da.histogram(x[col_name], bins=bins)}
                return {"hist_agg": da.histogram(x[col_name], bins=bins, range=range)}

            return hist_agg_


        @staticmethod
        def kurtosis(col_name, *args):
            def _kurtoris(x):
                result = {"kurtosis": float(stats.kurtosis(x[col_name]))}
                return result

            return _kurtoris

        @staticmethod
        def mean(col_name, *args):
            def _mean(x):
                return {"percentile_agg": x[col_name].mean()}
                return result

            return _mean

        @staticmethod
        def skewness(col_name, *args):
            def _skewness(x):
                result = {"skewness": float(stats.skew(x[col_name]))}
                return result

            return _skewness

        @staticmethod
        def percentile_agg(col_name, args):
            values = args[1]
            print(values)

            def _percentile(x):
                return {"percentile_agg": x[col_name].quantile(values)}

            return _percentile

        @staticmethod
        def count_uniques_agg(col_name, args):
            estimate = args[0]

            def _count_uniques_agg(x):
                if estimate is True:
                    result = {"count_uniques_agg": x[col_name].nunique_approx()}
                else:
                    result = {"count_uniques_agg": x[col_name].nunique()}
                return result

            return _count_uniques_agg

    return Functions()


DataFrame.functions = property(functions)
# print(DataFrame.functions)
