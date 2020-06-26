from datetime import datetime, timedelta

import dask.array as da
import numpy as np
import pandas as pd
from dask import dataframe as dd

from optimus.helpers.check import is_pandas_series, is_dask_series, is_cudf_series, is_dask_cudf_dataframe, \
    is_cudf_dataframe, is_pandas_dataframe, is_dask_dataframe
from optimus.helpers.converter import format_dict

op_to_series_func = {
    "abs": {
        "cudf": "abs",
        "numpy": "abs",
        "da": "abs"
    },
    "exp": {
        "cudf": "exp",
        "numpy": "exp",
    },
    "sqrt": {
        "cudf": "sqrt",
        "numpy": "sqrt",
    },
    "mod": {
        "cudf": "mod",
        "numpy": "mod"

    },
    "pow": {
        "cudf": "pow",
        "numpy": "power"
    },
    "ceil": {
        "cudf": "ceil",
        "numpy": "ceil"
    },
    "floor": {
        "cudf": "floor",
        "numpy": "floor"
    },
    "trunc": {
        "cudf": "trunc",
        "numpy": "trunc"
    },
    "radians": {
        "cudf": "radians",
        "numpy": "radians"
    },
    "degrees": {
        "cudf": "degrees",
        "numpy": "degrees"
    },
    "ln": {
        "cudf": "log",
        "numpy": "log"
    },
    "log": {
        "cudf": "log10",
        "numpy": "log10"
    },
    "sin": {
        "cudf": "sin",
        "numpy": "sin"
    },
    "cos": {
        "cudf": "cos",
        "numpy": "cos"
    },
    "tan": {
        "cudf": "tan",
        "numpy": "tan"
    },
    "asin": {
        "cudf": "asin",
        "numpy": "arcsin"
    },
    "acos": {
        "cudf": "acos",
        "numpy": "arccos"
    },
    "atan": {
        "cudf": "atan",
        "numpy": "arctan"
    },
    "sinh": {
        "cudf": None,
        "numpy": "sinh"
    },
    "asinh": {
        "cudf": None,
        "numpy": "arcsinh"
    },
    "cosh": {
        "cudf": None,
        "numpy": "cosh"
    }
    ,
    "acosh": {
        "cudf": None,
        "numpy": "arccosh"
    },
    "tanh": {
        "cudf": None,
        "numpy": "tanh"
    },
    "atanh": {
        "cudf": None,
        "numpy": "arctanh"
    }

}


def call(series, *args, method_name=None):
    """
    Process a series or number with a function
    :param series:
    :param args:
    :param method_name:
    :return:
    """
    # print("op_to_series_func[method_name]", op_to_series_func[method_name]["cudf"])
    # print("series", dir(series), series)
    print("series", type(series), series)
    if is_pandas_series(series):
        method = getattr(np, op_to_series_func[method_name]["numpy"])
        result = method(series, *args)

    elif is_dask_series(series):
        def func(_series, _method, args):
            return _method(_series, *args)

        method = getattr(da, op_to_series_func[method_name]["da"])
        # result = dd.map_partitions(func, series, method, args, meta=float)
        result = method(series, *args)

    elif is_cudf_series(series):
        method = getattr(series, op_to_series_func[method_name]["cudf"])
        result = method(series, *args)

    elif is_dask_cudf_dataframe(series):
        def func(series, _method, args):
            return _method(series, *args)

        method = getattr(series, op_to_series_func[method_name]["cudf"])
        result = dd.map_partitions(func, series, method, args, meta=float)

    return result


def abs(series):
    return to_numeric(series).abs()


def variance(df, columns, *args):
    return {"var": {col_name: to_numeric(df[col_name]).var() for col_name in columns}}


def to_numeric(df_series):
    def _to_numeric_cudf(_df_series, _col_name=None):
        import cudf
        if is_cudf_series(df_series):
            series = _df_series
        elif is_cudf_dataframe(df_series):
            series = _df_series[_col_name]

        series_string = series.astype(str)
        s = cudf.Series(series_string.str.stof()).fillna(False)
        s[~cudf.Series(cudf.core.column.string.cpp_is_float(series_string._column)).fillna(False)] = None
        return s

    # if not is_column_a(df_series, col, df.constants.NUMERIC_TYPES):

    if is_pandas_series(df_series) or is_pandas_dataframe(df_series):
        return pd.to_numeric(df_series, errors="coerce")

    elif is_dask_dataframe(df_series):
        def func(_df_series):
            return pd.to_numeric(_df_series, errors="coerce")

        return df_series.map_partitions(func)

    elif is_cudf_series(df_series):
        return _to_numeric_cudf(df_series)

    elif is_cudf_dataframe(df_series):
        kw_columns = {}
        for col_name in df_series.cols.names():
            kw_columns[col_name] = _to_numeric_cudf(df_series, col_name)
        return df_series.assign(**kw_columns)


def mad(df, columns, args):
    more = args[0]
    mad_value = {}
    for col_name in columns:
        casted_col = to_numeric(df[col_name])
        median_value = casted_col.quantile(0.5)

        # In all case all the values from the column are nan because can not be converted to number
        if not np.isnan(median_value):
            mad_value[col_name] = (casted_col - median_value).abs().quantile(0.5)
        else:
            mad_value[col_name] = np.nan

    def to_dict(_mad_value, _median_value):
        _mad_value = {"mad": _mad_value}

        if more:
            _mad_value.update({"median": _median_value})

        return _mad_value

    _mad_agg = delayed(df, to_dict)
    return _mad_agg(mad_value, median_value)


def min(df, columns, *args):
    return {"min": {col_name: to_numeric(df[col_name]).min() for col_name in columns}}


def clip(series, lower_bound, upper_bound):
    if is_cudf_series(series):
        raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/pull/5222")
    else:
        return to_numeric(series).clip(lower_bound, upper_bound)


def max(df, columns, *args):
    return {"max": {col_name: to_numeric(df[col_name]).max() for col_name in columns}}


def mode(df, columns, *args):
    return {"mode": {col_name: df[col_name].mode() for col_name in columns}}


def std(df, columns, *args):
    return {"std": {col_name: to_numeric(df[col_name]).std() for col_name in columns}}


def sum(df, columns, *args):
    return {"sum": {col_name: to_numeric(df[col_name]).sum() for col_name in columns}}


# @staticmethod
# def sum(df, columns, args):
#     f = {col_name: df[col_name].sum() for col_name in columns}
#
#     @delayed
#     def _sum(_f):
#         return {"sum": _f}
#
#     return _sum(f)


def range(df, columns, *args):
    return {
        "range": {col_name: {"min": to_numeric(df[col_name]).min(), "max": to_numeric(df[col_name]).max()} for col_name
                  in columns}}
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

def unique(df, columns, *args):
    # Cudf can not handle null so we fill it with non zero values.
    return {"unique": {col_name: df[col_name].astype(str).unique().ext.to_dict(index=False) for col_name in columns}}


def count_zeros(df, columns, *args):
    # Cudf can not handle null so we fill it with non zero values.
    non_zero_value = 1
    return {"zeros": {col_name: int((to_numeric(df[col_name]).fillna(non_zero_value).values == 0).sum()) for col_name in
                      columns}}


def count_uniques(df, columns, *args):
    # Cudf can not handle null so we fill it with non zero values.
    return {"unique": {col_name: df[col_name].astype(str).nunique() for col_name in columns}}


def mean(df, columns, *args):
    return {"mean": {col_name: to_numeric(df[col_name]).mean() for col_name in columns}}


def delayed(df, func):
    import dask
    if is_dask_dataframe(df) or is_dask_cudf_dataframe(df):
        return dask.delayed(func)
    return func


def percentile_agg(df, columns, args):
    values = args[0]
    result = [to_numeric(df[col_name]).quantile(values) for col_name in columns]

    def to_dict(_result):
        return {"percentile": {col_name: r.ext.to_dict() for col_name, r in zip(columns, _result)}}

    _percentile = delayed(df, to_dict)
    return format_dict(_percentile(result))


def count_na(df, columns, args):
    # estimate = args[0]
    return {"count_na": {col_name: df[col_name].isnull().sum() for col_name in columns}}
    # return np.count_nonzero(_df[_serie].isnull().values.ravel())
    # return cp.count_nonzero(_df[_serie].isnull().values.ravel())


def date_format(series, current_format=None, output_format=None):
    if is_pandas_series(series):
        return pd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)
    elif is_cudf_series(series):
        import cudf
        return cudf.to_datetime(series).astype('str', format=output_format)


def years_between(series, date_format=None):
    if is_pandas_series(series):
        return (pd.to_datetime(series, format=date_format,
                               errors="coerce").dt.date - datetime.now().date()) / timedelta(days=365)
    elif is_cudf_series(series):
        import cudf
        raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
        return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()


def exp(series):
    return call(series, method_name="exp")


def mod(series, *args):
    return call(series, *args, method_name="mod")


def pow(series, *args):
    return call(series, *args, method_name="pow")


def ceil(series):
    return call(series, method_name="ceil")


def sqrt(series):
    return call(series, method_name="sqrt")


def floor(series):
    return call(series, method_name="floor")


def trunc(series):
    return call(series, method_name="trunc")


def radians(series):
    return call(series, method_name="radians")


def degrees(series):
    return call(series, method_name="degrees")


def ln(series):
    return call(series, method_name="ln")


def log(series):
    return call(series, method_name="log")


# Trigonometrics
def sin(series):
    return call(series, method_name="sin")


def cos(series):
    return call(series, method_name="cos")


def tan(series):
    return call(series, method_name="tan")


def asin(series):
    return call(series, method_name="asin")


def acos(series):
    return call(series, method_name="acos")


def atan(series):
    return call(series, method_name="atan")


def sinh(series):
    return call(series, method_name="sinh")


def asinh(series):
    return call(series, method_name="asinh")


def cosh(series):
    return call(series, method_name="cosh")


def tanh(series):
    return call(series, method_name="tanh")


def acosh(series):
    return call(series, method_name="acosh")


def atanh(series):
    return call(series, method_name="atanh")


# Strings
def upper(series):
    return series.astype(str).str.upper()


def lower(series):
    return series.astype(str).str.lower()


def extract(series, regex):
    return series.astype(str).str.extract(regex)


def slice(series, start, stop, step):
    return series.astype(str).str.slice(start, stop, step)


def proper(series):
    return series.astype(str).str.title()


def trim(series):
    return series.astype(str).str.strip()


def remove_white_spaces(series):
    return series.str.replace(" ", "")


def remove_special_chars(series):
    pass


def len(series):
    return series.str.len()


def remove_accents(series):
    pass


def find(series, sub, start=0, end=None):
    return series.astype(str).str.find(sub, start, end)


def rfind(series, sub, start=0, end=None):
    return series.astype(str).str.rfind(sub, start, end)


def left(series, position):
    return series.str[:position]


def right(series, position):
    return series.str[-1 * position:]


def starts_with(series, pat):
    return series.str.startswith(pat)


def ends_with(series, pat):
    return series.str.endswith(pat)


def char(series):
    pass


def unicode(series):
    pass


def exact(series, pat):
    return series == pat


# dates
def year(series):
    return series.dt.year()


def month(series):
    return series.dt.mont()


def day(series):
    return series.dt.day()


def hour(series):
    return series.dt.hour()


def minute(series):
    return series.dt.minute()


def second(series):
    return series.dt.second()

# pad

# Not available in cudf dask cudf
#
# def sinh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
# def asinh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
# def cosh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
# def tanh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
# def acosh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
# def atanh(self, series):
#     raise NotImplementedError('Not implemented yet')
