from abc import abstractmethod, ABC

import dask
import numpy as np

# import cudf
from optimus.helpers.check import is_dask_series, is_dask_dataframe
from optimus.helpers.core import val_to_list


def op_delayed(df):
    def inner(func):
        def wrapper(*args, **kwargs):
            if is_dask_dataframe(df) or is_dask_series(df):  # or is_dask_cudf_dataframe(df):
                return dask.delayed(func)(*args, **kwargs)
            return func(*args, **kwargs)

        return wrapper

    return inner


class Functions(ABC):
    def __init__(self, series):
        self.series = series

    # @staticmethod
    # @op_delayed
    # def _flat_dict(key_name, ele):
    #     return {key_name: {x: y for x, y in ele.items()}}

    # Aggregation
    @staticmethod
    def min(series):
        return series.ext.to_float().min()

    @staticmethod
    def max(series):
        return series.ext.to_float().max()

    @staticmethod
    def mean(series):
        return series.ext.to_float().max()

    @staticmethod
    def mode(series):
        return series.mode().ext.to_dict(index=False)

    @staticmethod
    def std(series):
        return series.ext.to_float().std()

    @staticmethod
    def sum(series):
        return series.ext.to_float().sum()

    @staticmethod
    def var(series):
        return series.ext.to_float().var()

    @staticmethod
    def count_uniques(series, estimate: bool = True, compute: bool = True):
        return series.astype(str).nunique()

    @staticmethod
    def unique(series):
        # Cudf can not handle null so we fill it with non zero values.
        return series.astype(str).unique().ext.to_dict(index=False)

    @staticmethod
    def count_na(series):
        return series.isnull().sum()

        # return {"count_na": {col_name:  for col_name in columns}}
        # return np.count_nonzero(_df[_serie].isnull().values.ravel())
        # return cp.count_nonzero(_df[_serie].isnull().values.ravel())

    @staticmethod
    @abstractmethod
    def count_zeros(series, *args):
        pass

    @staticmethod
    @abstractmethod
    def kurtosis(series):
        pass

    @staticmethod
    @abstractmethod
    def skew(series):
        pass

    @staticmethod
    def mad(df, columns, args):
        more = args[0]
        mad_value = {}

        for col_name in columns:
            casted_col = df.cols.select(col_name).cols.to_float()
            median_value = casted_col.cols.median(col_name)
            # print(median_value)
            # In all case all the values from the column
            # are nan because can not be converted to number
            if not np.isnan(median_value):
                mad_value = (casted_col - median_value).abs().quantile(0.5)
            else:
                mad_value[col_name] = np.nan

        @op_delayed(df)
        def to_dict(_mad_value, _median_value):
            _mad_value = {"mad": _mad_value}

            if more:
                _mad_value.update({"median": _median_value})

            return _mad_value

        return to_dict(mad_value, median_value)

    # TODO: dask seems more efficient triggering multiple .min() task, one for every column
    # cudf seems to be calculate faster in on pass using df.min()
    # method_to_call = getattr(foo, 'bar')
    # result = method_to_call()

    def range(df, columns, *args):
        return {
            "range": {col_name: {"min": df.cols.min(col_name), "max": df.cols.max(col_name)}
                      for col_name in columns}}
        # @staticmethod
        #     def range_agg(df, columns, args):
        #         columns = parse_columns(df, columns)
        #
        #         @delayed
        #         def _range_agg(_min, _max):
        #             return {col_name: {"min": __min, "max": __max} for (col_name, __min), __max in
        #                     zip(_min["min"].items(), _max["max"].values())}
        #
        #         return _range_agg(df.cols.min(columns), df.cols.max(columns))

    # return value.astype(str).unique().ext.to_dict()

    def percentile_agg(df, columns, args):
        values = val_to_list(args[0])
        # result = [df.cols.select(col_name).cols.to_float().quantile(values) for col_name in columns]
        result = [df[col_name].ext.to_float() for col_name in columns]

        @op_delayed(df)
        def to_dict(_result):
            ## In pandas if all values are non it return {} on dict
            _r = {}
            for col_name, r in zip(columns, _result):
                # Dask raise an exception is all values in the series are np.nan
                if r.isnull().all():
                    _r[col_name] = np.nan
                else:
                    _r[col_name] = r.quantile(values).ext.to_dict()
            return _r

        return to_dict(result)

    # def radians(series):
    #     return series.ext.to_float().radians()
    #
    # def degrees(series, *args):
    #     return call(series, method_name="degrees")

    ###########################

    @staticmethod
    @abstractmethod
    def clip(series, lower_bound, upper_bound):
        pass

    @staticmethod
    @abstractmethod
    def cut(series, bins):
        pass

    @staticmethod
    def abs(series):
        return series.ext.to_float().abs()

    @staticmethod
    @abstractmethod
    def exp(series):
        pass

    @staticmethod
    @abstractmethod
    def sqrt(series):
        pass

    def mod(series, other):
        return series.ext.to_float().mod(other)

    def pow(self, exponent):
        return self.ext.to_float().pow(exponent)

    def floor(self):
        series = self.series
        return series.ext.to_float().floor()

    # def trunc(self):
    #     series = self.series
    #     return series.ext.to_float().truncate()

    @staticmethod
    @abstractmethod
    def radians(series):
        pass

    @staticmethod
    @abstractmethod
    def degrees(series):
        pass

    @staticmethod
    @abstractmethod
    def ln(series):
        pass

    @staticmethod
    @abstractmethod
    def log(series):
        pass

    @staticmethod
    @abstractmethod
    def ceil(series):
        pass

    @staticmethod
    @abstractmethod
    def sin(series):
        pass

    @staticmethod
    @abstractmethod
    def cos(series):
        pass

    @staticmethod
    @abstractmethod
    def tan(series):
        pass

    @staticmethod
    @abstractmethod
    def asin(series):
        pass

    @staticmethod
    @abstractmethod
    def acos(series):
        pass

    @staticmethod
    @abstractmethod
    def atan(series):
        pass

    @staticmethod
    @abstractmethod
    def sinh(series):
        pass

    @staticmethod
    @abstractmethod
    def cosh(series):
        pass

    @staticmethod
    @abstractmethod
    def tanh(series):
        pass

    @staticmethod
    @abstractmethod
    def asinh(series):
        pass

    @staticmethod
    @abstractmethod
    def acosh(series):
        pass

    @staticmethod
    @abstractmethod
    def atanh(series):
        pass

    # Strings
    @staticmethod
    def lower(series):
        return series.astype(str).str.lower()

    @staticmethod
    def upper(series):
        return series.astype(str).str.upper()

    @staticmethod
    def extract(series, regex):
        return series.astype(str).str.extract(regex)

    @staticmethod
    def slice(series, start, stop, step):
        return series.astype(str).str.slice(start, stop, step)

    @staticmethod
    def proper(series):
        return series.astype(str).str.title()

    @staticmethod
    def trim(series):
        return series.astype(str).str.strip()

    @staticmethod
    def remove_white_spaces(series):
        return series.str.replace(" ", "")

    @staticmethod
    def len(series):
        return series.str.len()

    @staticmethod
    def remove_accents(self):
        pass

    @staticmethod
    def find(self, sub, start=0, end=None):
        series = self.series
        return series.astype(str).str.find(sub, start, end)

    @staticmethod
    def rfind(series, sub, start=0, end=None):
        return series.astype(str).str.rfind(sub, start, end)

    @staticmethod
    def left(series, position):
        return series.str[:position]

    @staticmethod
    def right(series, position):
        return series.str[-1 * position:]

    @staticmethod
    def starts_with(series, pat):
        return series.str.startswith(pat)

    @staticmethod
    def ends_with(series, pat):
        return series.str.endswith(pat)

    @staticmethod
    def char(series):
        pass

    @staticmethod
    def unicode(series):
        pass

    @staticmethod
    def exact(series, pat):
        return series == pat

    # dates
    @staticmethod
    def year(series, format):
        # return self.ext.to_datetime(format=format).strftime('%Y').to_self().reset_index(drop=True)
        return series.ext.to_datetime(format=format).dt.year

    @staticmethod
    def month(series, format):
        return series.ext.to_datetime(format=format).dt.month

    @staticmethod
    def day(series, format):
        return series.ext.to_datetime(format=format).dt.day

    @staticmethod
    def hour(series):
        return series.ext.to_datetime(format=format).dt.hour

    @staticmethod
    def minute(series):
        return series.ext.to_datetime(format=format).dt.minute

    @staticmethod
    def second(series):
        return series.ext.to_datetime(format=format).dt.second
