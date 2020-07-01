from datetime import datetime, timedelta

import dask.array as da
import numpy as np
import pandas as pd
from dask import dataframe as dd
# import cudf
from optimus.helpers.check import is_pandas_series, is_dask_series, is_cudf_series, is_dask_cudf_dataframe, \
    is_cudf_dataframe, is_pandas_dataframe, is_dask_dataframe
from optimus.helpers.converter import format_dict

op_to_series_func = {
    "abs": {
        # "cudf": cudf.abs,
        "numpy": np.abs,
        # "da": da.abs
    },
    "exp": {
        "cudf": "exp",
        "numpy": np.exp,
        "da": da.exp
    },
    "sqrt": {
        "cudf": "sqrt",
        "numpy": np.sqrt,
        "da": da.sqrt
    },
    "mod": {
        "cudf": "mod",
        "numpy": np.mod,
        "da": da.mod

    },
    "pow": {
        "cudf": "pow",
        "numpy": np.power,
        "da": da.power
    },
    "radians": {
        "cudf": "radians",
        "numpy": np.radians,
        "da": da.radians

    },
    "degrees": {
        "cudf": "degrees",
        # "numpy": np.degress,
        "da": da.degrees
    },
    "ln": {
        "cudf": "log",
        "numpy": np.log,
        "da": da.log
    },
    "log": {
        "cudf": "log10",
        "numpy": np.log10,
        "da": da.log10

    },
    "sin": {
        "cudf": "sin",
        "numpy": np.sin,
        "da": da.sin
    },
    "cos": {
        "cudf": "cos",
        "numpy": np.cos,
        "da": da.cos
    },
    "tan": {
        "cudf": "tan",
        "numpy": np.tan,
        "da": da.tan
    },
    "asin": {
        "cudf": "arcsin",
        "numpy": np.arcsin,
        "da": da.arcsin
    },
    "acos": {
        "cudf": "arccos",
        "numpy": np.arccos,
        "da": da.arccos

    },
    "atan": {
        "cudf": "arctan",
        "numpy": np.arctan,
        "da": da.arctan

    },
    "sinh": {
        "cudf": None,
        "numpy": np.sinh,
        "da": da.sinh
    },
    "asinh": {
        "cudf": None,
        "numpy": np.arcsinh,
        "da": da.arcsinh
    },
    "cosh": {
        "cudf": None,
        "numpy": np.cosh,
        "da": da.cosh
    }
    ,
    "acosh": {
        "cudf": None,
        "numpy": np.arccosh,
        "da": da.arccosh

    },
    "tanh": {
        "cudf": None,
        "numpy": np.tanh,
        "da": da.tanh

    },
    "atanh": {
        "cudf": None,
        "numpy": np.arctanh,
        "da": da.arctanh
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
    # print("series", type(series), series)
    if is_pandas_series(series):
        method = op_to_series_func[method_name]["numpy"]
        result = method(series, *args)

    elif is_dask_series(series):
        method = op_to_series_func[method_name]["da"]
        result = method(series, *args)

    elif is_cudf_series(series):
        import cudf
        method = getattr(cudf, op_to_series_func[method_name]["cudf"])
        result = method(series)

    elif is_dask_cudf_dataframe(series):
        def func(series, _method, args):
            return _method(series, *args)

        method = getattr(series, op_to_series_func[method_name]["cudf"])
        result = dd.map_partitions(func, series, method, args, meta=float)

    return result


def abs(series):
    return to_numeric(series).abs()


# def to_numeric(df_series, args):
#     def _to_numeric_cudf(_df_series, _col_name=None):
#         import cudf
#         if is_cudf_series(df_series):
#             series = _df_series
#         elif is_cudf_dataframe(df_series):
#             series = _df_series[_col_name]
#
#         series_string = series.astype(str)
#         # See https://github.com/rapidsai/cudf/issues/5345
#         series = cudf.Series(series_string.str.stof()).fillna(False)
#         series[~cudf.Series(cudf.core.column.string.cpp_is_float(series_string._column)).fillna(False)] = None
#         return series
#
#     # if not is_column_a(df_series, col, df.constants.NUMERIC_TYPES):
#
#
#     if is_pandas_series(df_series) or is_pandas_dataframe(df_series):
#         return pd.to_numeric(df_series, errors="coerce")
#
#     elif is_dask_dataframe(df_series):
#         def func(_df_series):
#             return pd.to_numeric(_df_series, errors="coerce")
#
#         return df_series.map_partitions(func)
#
#     elif is_cudf_series(df_series):
#         return _to_numeric_cudf(df_series)
#
#     elif is_cudf_dataframe(df_series):
#         kw_columns = {}
#         for col_name in df_series.cols.names():
#             kw_columns[col_name] = _to_numeric_cudf(df_series, col_name)
#         return df_series.assign(**kw_columns)


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


def clip(series, lower_bound, upper_bound):
    if is_cudf_series(series):
        raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/pull/5222")
    else:
        return to_numeric(series).clip(lower_bound, upper_bound)


def cut(series, bins):
    # if is_cudf_series(series):
    #     raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/pull/5222")
    # else:
    if is_pandas_series(series):
        return to_numeric(series).cut(bins, include_lowest=True, labels=list(range(bins)))
    elif is_cudf_series(series):
        raise NotImplementedError("Not implemented yet")


def is_any_series(series):
    if is_cudf_series(series) or is_pandas_series(series) or is_dask_series(series):
        return True


# TODO: dask seems more efficient triggering multiple .min() task, one for every column
# cudf seems to be calculate faster in on pass using df.min()

def min(df_or_series, columns=None):
    if is_any_series(df_or_series):
        return {"min": {df_or_series.name: to_numeric(df_or_series).min()}}

    return {"min": {col_name: to_numeric(df_or_series[col_name]).min() for col_name in columns}}


def max(df_or_series, columns=None):
    if is_any_series(df_or_series):
        return {"max": {df_or_series.name: to_numeric(df_or_series).max()}}

    return {"max": {col_name: to_numeric(df_or_series[col_name]).max() for col_name in columns}}


def mean(df_or_series, columns=None):
    if is_any_series(df_or_series):
        return {"mean": {df_or_series.name: to_numeric(df_or_series).mean()}}

    return {"mean": {col_name: to_numeric(df_or_series[col_name]).mean() for col_name in columns}}


def mode(df_or_series, columns=None):
    if is_any_series(df_or_series):
        return {"mode": {df_or_series.name: to_numeric(df_or_series).mode()}}

    return {"mode": {col_name: to_numeric(df_or_series[col_name]).mode() for col_name in columns}}


def std(df_or_series, columns=None):
    if is_any_series(df_or_series):
        return {"std": {df_or_series.name: to_numeric(df_or_series).std()}}

    return {"std": {col_name: to_numeric(df_or_series[col_name]).std() for col_name in columns}}


def sum(df_or_series, columns=None):
    if is_any_series(df_or_series):
        return {"sum": {df_or_series.name: to_numeric(df_or_series).sum()}}

    return {"sum": {col_name: to_numeric(df_or_series[col_name]).sum() for col_name in columns}}


def variance(df_or_series, columns=None):
    if is_any_series(df_or_series):
        return {"var": {df_or_series.name: to_numeric(df_or_series).var()}}

    return {"var": {col_name: to_numeric(df_or_series[col_name]).var() for col_name in columns}}


# def variance(df, columns, *args):
#     return {"var": {col_name: to_numeric(df[col_name]).var() for col_name in columns}}


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
        print("strftime will be available in https://github.com/rapidsai/cudf/issues/5583")
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
    return to_numeric(series).mod(*args)


def pow(series, *args):
    return call(series, *args, method_name="pow")


def ceil(series):
    return to_numeric(series).ceil()


def sqrt(series):
    return call(series, method_name="sqrt")


def floor(series):
    return to_numeric(series).floor()


def trunc(series):
    return to_numeric(series).truncate()


def radians(series):
    return to_numeric(series).radians()


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


def adsin(series):
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


#
# def sinh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
#
# def asinh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
#
# def cosh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
#
# def tanh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
#
# def acosh(self, series):
#     raise NotImplementedError('Not implemented yet')
#
#
# def arctanh(self, series):
#     raise NotImplementedError('Not implemented yet')


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
