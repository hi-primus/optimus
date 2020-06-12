import numpy as np
from dask import dataframe as dd

from optimus.helpers.check import is_pandas_series, is_dask_series, is_cudf_series, is_dask_cudf_dataframe

op_to_series_func = {
    "abs": {
        "cudf": "abs",
        "numpy": "abs",
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

    if is_pandas_series(series):
        method = getattr(np, op_to_series_func[method_name]["numpy"])
        result = method(series, *args)

    elif is_dask_series(series):
        def func(_series, _method, args):
            return _method(_series, *args)

        method = getattr(np, op_to_series_func[method_name]["numpy"])
        result = dd.map_partitions(func, series, method, args, meta=float)
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
    return call(series, method_name="abs")


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
    return series.str.upper()


def lower(series):
    return series.str.lower()


def proper(series):
    return series.str.title()


def trim(series):
    return series.str.strip()


def remove_white_spaces(series):
    return series.str.replace(" ", "")


def remove_special_chars(series):
    pass


def len(series):
    return series.str.len()


def remove_accents(series):
    pass


def find(series, sub, start=0, end=None):
    return series.str.find(sub, start, end)


def rfind(series, sub, start=0, end=None):
    return series.str.rfind(sub, start, end)


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
