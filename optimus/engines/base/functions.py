from abc import abstractmethod, ABC

import dask
import numpy as np

# import cudf
from optimus.helpers.check import is_pandas_series, is_dask_series, is_cudf_series, is_dask_dataframe
from optimus.helpers.converter import format_dict
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

    @staticmethod
    def _base(ds, func_name, columns=None, tidy=True, args=None):
        # if is_any_series(ds):
        
        result = [getattr(ds.ext.to_float(), func_name)()]
        columns = val_to_list(ds.name)

        # else:
        #     result = [getattr(ds[col_name].ext.to_float(), func_name)() for col_name in columns]

        @op_delayed(ds)
        def to_dict(_result):
            return format_dict({func_name: {col_name: r for col_name, r in zip(columns, _result)}}, tidy=tidy)

        return to_dict(result)

    @staticmethod
    @op_delayed
    def _flat_dict(key_name, ele):
        return {key_name: {x: y for x, y in ele.items()}}

    # Aggregation
    @staticmethod
    def min(ds, columns=None, tidy=True, *args):
        return Functions._base(ds, "min", columns, tidy, args)

    @staticmethod
    def max(ds, columns=None, args=None):
        return Functions._base(ds, "max", columns, args)

    @staticmethod
    def mean(ds, columns=None, args=None):
        return Functions._base(ds, "mean", columns, args)

    @staticmethod
    def mode(ds, columns=None, args=None):
        return Functions._base(ds, "mode", columns, args)

    @staticmethod
    def std(ds, columns=None, args=None):
        return Functions._base(ds, "std", columns, args)

    @staticmethod
    def sum(ds, columns=None, args=None):
        return Functions._base(ds, "sum", columns, args)

    @staticmethod
    def var(ds, columns=None, args=None):
        return Functions._base(ds, "var", columns, args)

    @staticmethod
    def count_uniques(df, columns, estimate: bool = True, compute: bool = True):
        return Functions._flat_dict("count_uniques",
                                    {col_name: df[col_name].astype(str).nunique() for col_name in columns})(df)

    @staticmethod
    def unique(df, columns):
        # Cudf can not handle null so we fill it with non zero values.
        return Functions._flat_dict("unique",
                                    {col_name: list(df.cols.select(col_name)[col_name].astype(str).unique()) for
                                     col_name in
                                     columns})(
            df)

    @staticmethod
    def count_na(df, columns, args):
        return Functions._flat_dict({col_name: df[col_name].isnull().sum() for col_name in columns})(df)

        # return {"count_na": {col_name:  for col_name in columns}}
        # return np.count_nonzero(_df[_serie].isnull().values.ravel())
        # return cp.count_nonzero(_df[_serie].isnull().values.ravel())

    def count_zeros(df, columns, *args):
        # Cudf can not handle null so we fill it with non zero values.
        non_zero_value = 1
        return {
            "zeros": {col_name: int((df.cols.select(col_name).cols.to_float().fillna(non_zero_value).values == 0).sum())
                      for
                      col_name in columns}}

    @staticmethod
    @abstractmethod
    def kurtosis(series):
        pass

    @staticmethod
    @abstractmethod
    def skew(series):
        pass

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

    def is_any_series(series):
        if is_pandas_series(series):
            return True

        elif is_dask_series(series):
            return True
        # elif is_cudf_series(series):
        #     return True

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
    def abs(series):
        return series.ext.to_float().abs()

    @staticmethod
    def clip(series, lower_bound, upper_bound):
        return series.functions.clip(lower_bound, upper_bound)

    @staticmethod
    def cut(series, bins):
        # if is_cudf_series(series):
        #     raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/pull/5222")
        # else:
        if is_pandas_series(series):
            return series.ext.to_float(series).cut(bins, include_lowest=True, labels=list(range(bins)))
        elif is_cudf_series(series):
            raise NotImplementedError("Not implemented yet")

    @staticmethod
    @abstractmethod
    def exp(series):
        pass

    @staticmethod
    @abstractmethod
    def sqrt(series):
        pass

    def mod(self, other):
        series = self.series
        return series.ext.to_float().mod(other)

    def pow(self, exponent):
        series = self.series
        return series.ext.to_float().pow(exponent)

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
    def upper(self):
        series = self.series
        return series.astype(str).str.upper()

    def lower(self):
        series = self.series
        return series.astype(str).str.lower()

    def extract(self, regex):
        series = self.series
        return series.astype(str).str.extract(regex)

    def slice(self, start, stop, step):
        series = self.series
        return series.astype(str).str.slice(start, stop, step)

    def proper(self):
        series = self.series
        return series.astype(str).str.title()

    def trim(self):
        series = self.series
        return series.astype(str).str.strip()

    def remove_white_spaces(self):
        series = self.series
        return series.str.replace(" ", "")

    def len(self):
        series = self.series
        return series.str.len()

    def remove_accents(self):
        pass

    def find(self, sub, start=0, end=None):
        series = self.series
        return series.astype(str).str.find(sub, start, end)

    def rfind(self, sub, start=0, end=None):
        series = self.series
        return series.astype(str).str.rfind(sub, start, end)

    def left(self, position):
        series = self.series
        return series.str[:position]

    def right(self, position):
        series = self.series
        return series.str[-1 * position:]

    def starts_with(self, pat):
        series = self.series
        return series.str.startswith(pat)

    def ends_with(self, pat):
        series = self.series
        return series.str.endswith(pat)

    def char(self):
        pass

    def unicode(self):
        pass

    def exact(self, pat):
        return self == pat

    # dates
    def year(self, format):
        series = self.series
        # return self.ext.to_datetime(format=format).strftime('%Y').to_self().reset_index(drop=True)
        return series.ext.to_datetime(format=format).dt.year

    def month(self, format):
        series = self.series
        return series.ext.to_datetime(format=format).dt.month

    def day(self, format):
        series = self.series
        return series.ext.to_datetime(format=format).dt.day

    def hour(self):
        series = self.series
        return series.ext.to_datetime(format=format).dt.hour

    def minute(self):
        series = self.series
        return series.ext.to_datetime(format=format).dt.minute

    def second(self):
        series = self.series
        return series.ext.to_datetime(format=format).dt.second
