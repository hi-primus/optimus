# Aggregations
from optimus.engines.base.functions import Functions


def min(series):
    return Functions.min(series)


def max(series):
    return Functions.max(series)


def kurtosis(series):
    return Functions.kurtosis(series)


def skew(series):
    return Functions.kurtosis(series)


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
    return Functions.exp(series)


def sqrt(series):
    return Functions.sqrt(series)


def mod(series):
    return Functions.mod(series)


def pow(series):
    return Functions.pow(series)


def floor(series):
    return Functions.floor(series)


def range(series):
    return Functions.range(series)


def radians(series):
    return Functions.radians(series)


def degrees(series):
    return Functions.degrees(series)


def ln(series):
    return Functions.ln(series)


def log(series):
    return Functions.log(series)


def ceil(series):
    return Functions.ceil(series)


# Trigonometrics
def sin(series):
    return series.functions.sin()


def cos(series):
    return series.functions.cos()


def tan(series):
    return series.functions.tan()


def asin(series):
    return series.functions.asin()


def acos(series):
    return series.functions.acos()


def atan(series):
    return series.functions.atan()


def sinh(series):
    return series.functions.sinh()


def cosh(series):
    return series.functions.cosh()


def tanh(series):
    return series.functions.tanh()


def asinh(series):
    return series.functions.asinh()


def acosh(series):
    return series.functions.acosh()


def atanh(series):
    return series.functions.atanh()


# strings
def lower(series):
    return Functions.lower(series)


def upper(series):
    return Functions.upper(series)


def extract(series):
    return Functions.extract(series)


def slice(series):
    return Functions.slice(series)


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


def year(series):
    return Functions.year(series)


def month(series):
    return Functions.month(series)


def day(series):
    return Functions.day(series)


def hour(series):
    return Functions.hour(series)


def minute(series):
    return Functions.minute(series)


def second(series):
    return Functions.second(series)


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
