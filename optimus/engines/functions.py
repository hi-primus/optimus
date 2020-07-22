# Aggregations
def min(series):
    return series.functions.min()


def max(series):
    return series.functions.max()


def mean(series):
    return series.functions.mean()


def mode(series):
    return series.functions.mode()


def std(series):
    return series.functions.std()


def sum(series):
    return series.functions.sum()


def var(series):
    return series.functions.var()


# Math
def abs(series):
    return series.functions.abs()


def exp(series):
    return series.functions.exp()


def sqrt(series):
    return series.functions.sqrt()


def mod(series):
    return series.functions.mod()


def pow(series):
    return series.functions.pow()


def floor(series):
    return series.functions.floor()


def radians(series):
    return series.functions.radians()


def degrees(series):
    return series.functions.degrees()


def ln(series):
    return series.functions.ln()


def log(series):
    return series.functions.log()


def ceil(series):
    return series.functions.ceil()


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
    return series.functions.asin()


def acosh(series):
    return series.functions.acos()


def atanh(series):
    return series.functions.atan()


# strings
def lower(series):
    return series.functions.lower()


def upper(series):
    return series.functions.upper()


def extract(series):
    return series.functions.extract()


def slice(series):
    return series.functions.slice()


def proper(series):
    return series.functions.proper()


def trim(series):
    return series.functions.trim()


def remove_white_spaces(series):
    return series.functions.remove_white_spaces()


def len(series):
    return series.functions.len()


def remove_accents(series):
    return series.functions.remove_accents()


def find(series):
    return series.functions.find()


def rfind(series):
    return series.functions.rfind()


def left(series):
    return series.functions.left()


def right(series):
    return series.functions.right()


def starts_with(series):
    return series.functions.starts_with()


def ends_with(series):
    return series.functions.ends_with()


def char(series):
    return series.functions.char()


def unicode(series):
    return series.functions.unicode()


def exact(series):
    return series.functions.exact()


# dates
def year(series):
    return series.functions.year()


def month(series):
    return series.functions.month()


def day(series):
    return series.functions.day()


def hour(series):
    return series.functions.hour()


def minute(series):
    return series.functions.minute()


def second(series):
    return series.functions.second()
