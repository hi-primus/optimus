# Aggregations
import math
import numpy as np
from optimus.engines.base.functions import Functions
from optimus.infer import is_numeric


def min(series):
    return Functions.min(series)


def max(series):
    return Functions.max(series)


def kurtosis(series):
    return series.functions.kurtosis()

    # return Functions.kurtosis(series)


def skew(series):
    return series.functions.skew()
    # return Functions.kurtosis(series)


def mean(series):
    return Functions.mean(series)


def mad(series, *args):
    return Functions.mad(series, *args)


def mode(series):
    return Functions.mode(series)


def std(series):
    return Functions.std(series)


def sum(series):
    return Functions.sum(series)


def var(series):
    return Functions.var(series)


# Math
def abs(series):
    return Functions.abs(series)


def exp(series):
    if is_numeric(series):
        return math.exp(series)
    return series.functions.exp()


def sqrt(series):
    if is_numeric(series):
        return math.sqrt(series)
    return series.functions.sqrt()


def mod(series, other):
    return Functions.mod(series, other)


def pow(series, other):
    return Functions.pow(series, other)


def floor(series):
    if is_numeric(series):
        return math.floor(series)
    return series.functions.floor()


def ceil(series):
    if is_numeric(series):
        return math.ceil(series)
    return series.functions.ceil()


def round(series, decimals):
    return Functions.round(series, decimals)


def range(series):
    return series.functions.range()


def radians(series):
    if is_numeric(series):
        return math.radians(series)
    return series.functions.radian()


def degrees(series):
    return series.functions.degrees()


def ln(series):
    if is_numeric(series):
        return np.log(series)
    return series.functions.ln()


def log(series):
    if is_numeric(series):
        return np.log10(series)
    return series.functions.log()


# Trigonometrics
def sin(series):
    if is_numeric(series):
        return np.sin(series)
    return series.functions.sin()


def cos(series):
    if is_numeric(series):
        return np.cos(series)
    return series.functions.cos()


def tan(series):
    if is_numeric(series):
        return np.tan(series)
    return series.functions.tan()


def asin(series):
    if is_numeric(series):
        return np.arcsin(series)
    return series.functions.asin()


def acos(series):
    if is_numeric(series):
        return np.arccos(series)
    return series.functions.acos()


def atan(series):
    if is_numeric(series):
        return np.arctan(series)
    return series.functions.atan()


def sinh(series):
    if is_numeric(series):
        return np.sinh(series)
    return series.functions.sinh()


def cosh(series):
    if is_numeric(series):
        return np.cosh(series)
    return series.functions.cosh()


def tanh(series):
    if is_numeric(series):
        return np.tanh(series)
    return series.functions.tanh()


def asinh(series):
    if is_numeric(series):
        return np.arcsinh(series)
    return series.functions.asinh()


def acosh(series):
    if is_numeric(series):
        return np.arccosh(series)
    return series.functions.acosh()


def atanh(series):
    if is_numeric(series):
        return np.arctanh(series)
    return series.functions.atanh()


# strings
def lower(series):
    return Functions.lower(series)


def upper(series):
    return Functions.upper(series)


def extract(series, regex):
    return Functions.extract(series, regex)


def slice(series, start, stop,step):
    return Functions.slice(series, start, stop, step)


def percentile(series, *args):
    return Functions.percentile(series, *args)


def proper(series):
    return Functions.proper(series)


def trim(series):
    return Functions.trim(series)


def remove_white_spaces(series):
    return Functions.remove_white_spaces(series)


def len(series):
    return Functions.len(series)


def find(series):
    return Functions.find(series)


def rfind(series):
    return Functions.rfind(series)


def left(series):
    return Functions.left(series)


def right(series):
    return Functions.right(series)


def starts_with(series):
    return Functions.starts_with(series)


def ends_with(series):
    return Functions.ends_with(series)


def char(series):
    return Functions.char(series)


def unicode(series):
    return Functions.unicode(series)


def exact(series):
    return Functions.exact(series)


# dates

def date_format(series, current_format=None, output_format=None):
    return series.functions.date_format(current_format=current_format, output_format=output_format)


def year(series, format):
    return Functions.year(series, format)


def month(series, format):
    return Functions.month(series, format)


def day(series, format):
    return Functions.day(series, format)


def hour(series, format):
    return Functions.hour(series, format)


def minute(series, format):
    return Functions.minute(series, format)


def second(series, format):
    return Functions.second(series, format)


def weekday(series, format):
    return Functions.weekday(series, format)


def years_between(series, date_format):
    return series.functions.years_between(date_format)


# other
def count_na(series):
    return Functions.count_na(series)


def count_zeros(series):
    return Functions.count_zeros(series)


def count_uniques(series, *args):
    return Functions.count_uniques(series, *args)


def unique(series, *args):
    return Functions.unique(series, *args)


def replace_string(series, *args):
    return series.functions.replace_string(*args)


def replace_words(series, *args):
    return Functions.replace_words(series, *args)


def replace_match(series, *args):
    return Functions.replace_match(series, *args)


def remove_special_chars(series, *args):
    return series.functions.remove_special_chars()
    # return Functions.remove_special_chars(series, *args)


def remove_accents(series):
    return series.functions.remove_accents()


def clip(series, lower_bound, upper_bound):
    return series.functions.clip(lower_bound, upper_bound)
