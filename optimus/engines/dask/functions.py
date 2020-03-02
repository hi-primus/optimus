# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import dask
import numpy
from dask.array import stats
from dask.dataframe.core import DataFrame

from optimus.helpers.check import is_column_a
from optimus.helpers.core import val_to_list
from optimus.helpers.raiseit import RaiseIt


def functions(self):
    class Functions:

        @staticmethod
        def min(columns, args):
            def _dataframe_min(df):
                return {"min": df[columns].min()}

            return _dataframe_min

        @staticmethod
        def max(columns, args):
            def _dataframe_max(df):
                return {"max": df[columns].max()}

            return _dataframe_max

        @staticmethod
        def mean(columns, args):
            def _dataframe_mean(df):
                return {"mean": df[columns].mean()}

            return _dataframe_mean

        @staticmethod
        def variance(columns, args):
            def _dataframe_var(df):
                return {"variance": df[columns].var()}

            return _dataframe_var

        @staticmethod
        def sum(columns, args):

            def _dataframe_sum(df):
                return {"sum": df[columns].sum()}

            return _dataframe_sum

        @staticmethod
        def percentile_agg(columns, args):
            values = args[1]

            def _percentile(df):
                return {"percentile": df[columns].quantile(values)}

            return _percentile

        @staticmethod
        def stddev(columns, args):
            def _stddev(df):
                return {"stddev": df[columns].std()}

            return _stddev

        @staticmethod
        def zeros_agg(columns, args):
            columns = val_to_list(columns)

            def zeros_(df):
                result = {"zeros": {col_name: (df[col_name].values == 0).sum() for col_name in columns}}
                # result = {"zeros": (df[col_name].values == 0).sum()}
                return result

            return zeros_

        @staticmethod
        def count_na_agg(columns, args):

            def _count_na_agg(df):
                return {"count_na": df[columns].isnull().sum()}

            return _count_na_agg

        @staticmethod
        def hist_agg(columns, args):
            # {'OFFENSE_CODE': {'hist': [{'count': 169.0, 'lower': 111.0, 'upper': 297.0},
            #                            {'count': 20809.0, 'lower': 3645.0, 'upper': 3831.0}]}}
            def str_len(series):
                return series.str.len()

            def value_counts(series):
                return series.value_counts().sort_index().reset_index(level=0)

            def min_max_string_func(series):
                return [series.min()[0], series.max()[0]]

            def min_max_func(series):
                return [series.min(), series.max()]

            def hist_serie(series, buckets, min_max):
                i, j = numpy.histogram(series, bins=buckets, range=min_max)
                return {"count": list(i), "bins": list(j)}

            # TODO: Calculate mina max in one pass.
            def hist_agg_(df):
                # df = args[0]
                buckets = args[1]
                min_max = args[2]
                result_min_max = {}

                delayed_results = []

                for col_name in columns:
                    calc = None
                    if min_max is not None:
                        calc = min_max.get(col_name)

                    if calc is None or min_max is None:
                        if is_column_a(df, col_name, df.constants.STRING_TYPES):
                            a = Functions.map_delayed(df[col_name], str_len)
                            b = Functions.map_delayed(a, value_counts)
                            c = dask.delayed(min_max_string_func)(b)
                            f = Functions.map_delayed(b["index"], hist_serie, buckets, c)
                        #
                        elif is_column_a(df, col_name, df.constants.NUMERIC_TYPES):
                            a = Functions.map_delayed(df[col_name], min_max_func)
                            f = Functions.map_delayed(df[col_name], hist_serie, buckets, a)
                        else:
                            RaiseIt.type_error("column", ["numeric", "string"])
                        delayed_results.append({col_name: f})
                results_compute = dask.compute(*delayed_results)

                r = {}
                # Convert list to dict
                for i in results_compute:
                    r.update(i)
                result = {"hist": r}

                return result

            return hist_agg_

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
        def count_uniques_agg(columns, args):
            estimate = args[0]

            def _count_uniques_agg(df):

                if estimate is True:
                    ps = {col_name: df[col_name].nunique_approx() for col_name in columns}
                    # ps = pd.Series({col: df[col].nunique_approx() for col in df.cols.names()})
                else:
                    ps = {col_name: df[col_name].nunique() for col_name in columns}
                result = {"count_uniques": ps}

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

        @staticmethod
        def map_delayed(df, func, *args, **kwargs):
            if isinstance(df, dask.dataframe.core.Series):

                partitions = df.to_delayed()
                delayed_values = [dask.delayed(func)(part, *args, **kwargs)
                                  for part in partitions][0]
            else:
                delayed_values = dask.delayed(func)(df, *args, **kwargs)

            return delayed_values

    return Functions()


DataFrame.functions = property(functions)
