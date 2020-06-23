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
        def sum(df, columns, args):

            f = {col_name: df[col_name].sum() for col_name in columns}

            @delayed
            def _sum(_f):
                return {"sum": _f}

            return _sum(f)

        @staticmethod
        def percentile_agg(df, columns, args):

            values = args[0]

            f = [df[col_name].quantile(values) for col_name in columns]

            @delayed
            def _percentile(_f):
                return {"percentile": {c: _f[i].to_dict() for i, c in enumerate(columns)}}

            return _percentile(f)

        @staticmethod
        def zeros_agg(df, columns, args):
            columns = val_to_list(columns)

            @delayed
            def _zeros(_df):
                r = {}
                for col_name in columns:
                    s = (_df[col_name].values == 0)
                    if isinstance(s, np.ndarray):
                        r[col_name] = s.sum()
                return {"zeros": r}

            return _zeros(df)

        @staticmethod
        def count_na_agg(df, columns, args):

            @delayed
            def _count_na_agg(_df):
                return {"count_na": _df[columns].isnull().sum().to_dict()}

            return _count_na_agg(df)

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
        def range_agg(df, columns, args):
            columns = parse_columns(df, columns)

            @delayed
            def _range_agg(_min, _max):
                return {col_name: {"min": __min, "max": __max} for (col_name, __min), __max in
                        zip(_min["min"].items(), _max["max"].values())}

            return _range_agg(df.cols.min(columns), df.cols.max(columns))

        #
        # @staticmethod
        # def range_agg(columns, args):
        #     def _dataframe_range_agg_(df):
        #         return {"min": df[columns].min(), "max": df[columns].max()}
        #
        #     return _dataframe_range_agg_

        @staticmethod
        def mad_agg(df, col_name, args):
            more = args[0]
            median_value = format_dict(df.cols.median(col_name))
            print("median_value",median_value)
            mad_value = (df[col_name] - median_value).abs().quantile(0.5)

            @delayed
            def _mad_agg(_mad_value, _median_value):
                _mad_value = {"mad": _mad_value.to_dict()}

                if more:
                    _mad_value.update({"median": _median_value})

                return _mad_value

            return _mad_agg(mad_value, median_value)

    return Functions()


DataFrame.functions = property(functions)
