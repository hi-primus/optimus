# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import dask
from dask.array import stats
from dask.dataframe import from_delayed
from dask.dataframe.core import DataFrame

from optimus.helpers.check import is_column_a
from optimus.helpers.core import val_to_list
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

        # def hist_agg(col_name, df, buckets, min_max=None, dtype=None):
        @staticmethod
        def hist_agg(columns, args):
            # {'OFFENSE_CODE': {'hist': [{'count': 169.0, 'lower': 111.0, 'upper': 297.0},
            #                            {'count': 20809.0, 'lower': 3645.0, 'upper': 3831.0}]}}

            # TODO: Calculate mina max in one pass.
            def hist_agg_(serie):
                df = args[0]
                buckets = args[1]
                min_max = args[2]
                result_hist = {}
                result_min_max = {}
                for col_name in columns:
                    calc= None
                    if min_max is not None:
                        calc = min_max.get(col_name)

                    if calc is None or min_max is None:
                        if is_column_a(df, col_name, df.constants.STRING_TYPES):
                            print("1")
                            def func(val):
                                return val.str.len()

                            partitions = df[col_name].to_delayed()
                            delayed_values = [dask.delayed(func)(part)
                                              for part in partitions]
                            print(delayed_values)
                            df_len = from_delayed(delayed_values)
                            # df_len = df_len.value_counts()
                            # min_max = {"min": df_len.min(), "max": df_len.max()}
                            # df_hist = df_len

                        elif is_column_a(df, col_name, df.constants.NUMERIC_TYPES):

                            print("2")
                            min_max = {"min": df[col_name].min(), "max": df[col_name].max()}
                            # df_hist = serie[col_name]
                            print("3")
                        else:
                            RaiseIt.type_error("column", ["numeric", "string"])
                        # print(min_max)
                    result_min_max[col_name] = min_max

                import dask.dataframe as dd
                print(dd.compute(result_min_max))

                # import dask.array as da
                # for col_name in columns:
                #     min_1, max_1 = result_min_max[col_name]["min"], result_min_max[col_name]["max"]
                #     i, j = (da.histogram(df_hist, bins=buckets, range=[min_1, max_1]))
                #     print(col_name)
                #     # r = i.compute()
                #     # result_hist.update({col: {"count": list(r), "bins": list(j)}})

                result = {}
                result['hist'] = result_hist
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

    return Functions()


DataFrame.functions = property(functions)
