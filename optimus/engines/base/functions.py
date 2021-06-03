import re
from abc import abstractmethod, ABC

import jellyfish
import numpy as np
import pandas as pd
from jsonschema._format import is_email
from url_parser import UrlObject

from optimus.helpers.constants import ProfilerDataTypes
from optimus.helpers.core import val_to_list
from optimus.infer import is_list, is_null, is_bool, \
    is_credit_card_number, is_zip_code, is_int, is_decimal, is_datetime, is_object_value, is_ip, is_url, is_missing, \
    is_gender, is_list_of_int, is_list_of_str, is_str, is_phone_number, is_int_like


# ^(?:(?P<protocol>[\w\d]+)(?:\:\/\/))?(?P<sub_domain>(?P<www>(?:www)?)(?:\.?)(?:(?:[\w\d-]+|\.)*?)?)(?:\.?)(?P<domain>[^./]+(?=\.))\.(?P<top_domain>com(?![^/|:?#]))?(?P<port>(:)(\d+))?(?P<path>(?P<dir>\/(?:[^/\r\n]+(?:/))+)?(?:\/?)(?P<file>[^?#\r\n]+)?)?(?:\#(?P<fragment>[^#?\r\n]*))?(?:\?(?P<query>.*(?=$)))*$

class Functions(ABC):
    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    def from_delayed(self, delayed):
        return delayed[0]

    def to_delayed(self, delayed):
        return [delayed]

    # def _to_float(self, series):
    #     return series

    def to_integer(self, series):
        return series

    def _to_float_partition(self, series):
        return self.to_float(series)

    def _to_integer_partition(self, series):
        return self.to_integer(series)

    def to_string(self, series):
        return series

    def to_string_accessor(self, series):
        return self.to_string(series).str

    # Aggregation
    def min(self, series):
        return series.min()

    def max(self, series):
        return series.max()

    def mean(self, series):
        return self.to_float(series).mean()

    def mode(self, series):
        return self.to_float(series).mode().to_dict()

    def std(self, series):
        return self.to_float(series).std()

    def sum(self, series):
        return self.to_float(series).sum()

    def cumsum(self, series):
        return self.to_float(series).cumsum()

    def cumprod(self, series):
        return self.to_float(series).cumprod()

    def cummax(self, series):
        return self.to_float(series).cummax()

    def cummin(self, series):
        return self.to_float(series).cummin()

    def var(self, series):
        return self.to_float(series).var()

    def count_uniques(self, series, values=None, estimate: bool = True):
        return self.to_string(series).nunique()

    def unique(self, series, *args):
        # Cudf can not handle null so we fill it with non zero values.
        return list(self.to_string(series).unique())

    @staticmethod
    def count_na(series):
        return series.isna().sum()
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

    def mad(self, series, error, more):
        series = self.to_float(series)
        series = series[series.notnull()]
        median_value = series.quantile(0.5)
        mad_value = {"mad": (series - median_value).abs().quantile(0.5)}
        if more:
            mad_value.update({"median": median_value})

        return mad_value

    # TODO: dask seems more efficient triggering multiple .min() task, one for every column
    # cudf seems to be calculate faster in on pass using df.min()
    def range(self, series):

        return {"min": self.to_float(series).min(), "max": self.to_float(series).max()}

    def var(self, series):
        return self.to_float(series).var()

    def percentile(self, series, values, error):

        series = self.to_float(series)

        @self.delayed
        def to_dict(_result):
            ## In pandas if all values are none it return {} on dict
            # Dask raise an exception is all values in the series are np.nan
            if _result.isnull().all():
                return np.nan
            else:
                return _result.quantile(values).to_dict()

        return to_dict(series)

    # def radians(series):
    #     return series.to_float().radians()
    #
    # def degrees(series, *args):
    #     return call(series, method_name="degrees")

    ###########################

    def z_score(self, series):
        t = self.to_float(series)
        return t - t.mean() / t.std(ddof=0)

    def modified_z_score(self, series):
        mad_median = self.mad(series, True, True)
        median = mad_median["median"]
        mad = mad_median["mad"]

        return abs(0.6745 * (series - median) / mad)

    def clip(self, series, lower_bound, upper_bound):
        return self.to_float(series).clip(float(lower_bound), float(upper_bound))

    def cut(self, series, bins, labels, default):
        if is_list_of_int(bins):
            return pd.cut(self.to_float(series), bins, include_lowest=True, labels=labels)
        elif is_list_of_str(bins):
            conditions = [series.str.contains(i) for i in bins]

            return np.select(conditions, labels, default=default)

    def abs(self, series):
        return self.to_float(series).abs()

    def exp(self, series):
        return self.to_float(series).exp()

    @abstractmethod
    def word_tokenize(self, series):
        pass

    def len(self, series):
        return self.to_string_accessor(series).len()

    @staticmethod
    @abstractmethod
    def sqrt(series):
        pass

    def mod(self, series, other):
        return self.to_float(series).mod(other)

    def round(self, series, decimals):
        return self.to_float(series).round(decimals)

    def pow(self, series, exponent):
        return self.to_float(series).pow(exponent)

    def floor(self, series):
        return self.to_float(series).floor()

    # def trunc(self):
    #     series = self.series
    #     return series.to_float().truncate()

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
    def log(series, base):
        pass

    @staticmethod
    @abstractmethod
    def ceil(series):
        pass

    @abstractmethod
    def sin(self, series):
        pass

    @abstractmethod
    def cos(self, series):
        pass

    @abstractmethod
    def tan(self, series):
        pass

    @abstractmethod
    def asin(self, series):
        pass

    @abstractmethod
    def acos(self, series):
        pass

    @abstractmethod
    def atan(self, series):
        pass

    @abstractmethod
    def sinh(self, series):
        pass

    @abstractmethod
    def cosh(self, series):
        pass

    @abstractmethod
    def tanh(self, series):
        pass

    @abstractmethod
    def asinh(self, series):
        pass

    @abstractmethod
    def acosh(self, series):
        pass

    @abstractmethod
    def atanh(self, series):
        pass

    # Strings
    def match(self, series, regex):
        return self.to_string_accessor(series).match(regex)

    def lower(self, series):
        return self.to_string_accessor(series).lower()

    def upper(self, series):
        return self.to_string_accessor(series).upper()

    def title(self, series):
        return self.to_string_accessor(series).title()

    def capitalize(self, series):
        return self.to_string_accessor(series).capitalize()

    def pad(self, series, width, side, fillchar=""):
        return self.to_string_accessor(series).pad(width, side, fillchar)

    def extract(self, series, regex):
        return self.to_string_accessor(series).extract(regex)

    def slice(self, series, start, stop, step):
        return self.to_string_accessor(series).slice(start, stop, step)

    def proper(self, series):
        return self.to_string_accessor(series).title()

    def trim(self, series):
        return self.to_string_accessor(series).strip()

    def strip_html(self, value):
        return re.sub('<.*?>', '', value)

    @staticmethod
    @abstractmethod
    def replace_chars(series, search, replace_by):
        pass

    def replace_words(self, series, search, replace_by):
        search = val_to_list(search)
        str_regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
        return self.to_string_accessor(series).replace(str_regex, replace_by, regex= True)

    def replace_full(self, series, search, replace_by):
        search = val_to_list(search)
        str_regex = (r'^%s$' % r'$|^'.join(map(re.escape, search)))
        return self.to_string_accessor(series).replace(str_regex, replace_by, regex=True)

    def replace_values(self, series, search, replace_by):
        search = val_to_list(search)
        return series.mask(series.isin(search), replace_by)

    def remove_white_spaces(self, series):
        return self.to_string_accessor(series).replace(" ", "")

    def remove_urls(self, series):
        return self.to_string_accessor(series).replace(r"https?://\S+|www\.\S+", "", regex=True)

    def normalize_spaces(self, series):
        return self.to_string_accessor(series).replace(r" +", " ", regex=True)

    def expand_contractions(self, series):

        pass
    # @staticmethod
    # def len(series):
    #     return series.str.len()

    def to_datetime(self, series, format):
        pass

    def normalize_chars(self, series):
        pass

    def find(self, sub, start=0, end=None):
        series = self.series
        return self.to_string_accessor(series).find(sub, start, end)

    def rfind(self, series, sub, start=0, end=None):
        return self.to_string_accessor(series).rfind(sub, start, end)

    def left(self, series, position):
        return self.to_string_accessor(series)[:position]

    def right(self, series, position):
        return self.to_string_accessor(series)[-1 * position:]

    def mid(self, series, _start, _n):
        return self.to_string_accessor(series)[_start:_n]

    def starts_with(self, series, pat):
        return self.to_string_accessor(series).startswith(pat)

    def ends_with(self, series, pat):
        return self.to_string_accessor(series).endswith(pat)

    def contains(self, series, pat):
        return self.to_string_accessor(series).contains(pat)

    def char(self, series, _n):
        return self.to_string_accessor(series)[_n]

    @staticmethod
    def unicode(series):
        pass

    @staticmethod
    def exact(series, pat):
        return series == pat

    # dates
    def year(self, series, format):
        """
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        # return self.to_datetime(format=format).strftime('%Y').to_self().reset_index(drop=True)
        return self.to_datetime(series, format=format).dt.year

    @staticmethod
    def month(series, format):
        return series.to_datetime(format=format).dt.month

    @staticmethod
    def day(series, format):
        return series.to_datetime(format=format).dt.day

    @staticmethod
    def hour(series, format):
        return series.to_datetime(format=format).dt.hour

    @staticmethod
    def minute(series, format):
        return series.to_datetime(format=format).dt.minute

    @staticmethod
    def second(series, format):
        return series.to_datetime(format=format).dt.second

    @staticmethod
    def weekday(series, format):
        return series.to_datetime(format=format).dt.weekday

    @staticmethod
    @abstractmethod
    def date_format(self, current_format=None, output_format=None):
        pass

    @staticmethod
    @abstractmethod
    def years_between(self, date_format=None):
        pass

    def top_domain(self, value):
        return UrlObject.parse_url(value)["domain"]

    def domain(self, value):
        return UrlObject.parse_url(value)["domain"]
        # return UrlObject.parse_url(value)["domain"]

    def url_scheme(self, value):
        return UrlObject.parse_url(value)["protocol"]

    def url_params(self, value):
        return UrlObject.parse_url(value)["params"]

    def url_path(self, value):
        return UrlObject.parse_url(value)["path"]

    def url_query(self, value):
        return UrlObject.parse_url(value)["query"]

    def port(self, value):
        return UrlObject.parse_url(value)["port"]

    def sub_domain(self, series):
        return UrlObject.parse_url(value)["sub_domain"]

    def email_username(self, series):
        return series.str.split('@').str[0]

    def email_domain(self, series):
        return series.str.split('@').str[1]

    def infer_dtypes(self, value, cols_dtype):
        """
        Infer the data types.
        Please be aware that the order in which the value is checked is important and will change the final result
        :param value:
        :param cols_dtype:
        :return:
        """

        if is_list(value):
            dtype = ProfilerDataTypes.ARRAY.value
        elif is_null(value):
            dtype = ProfilerDataTypes.NULL.value
        elif is_bool(value):
            dtype = ProfilerDataTypes.BOOLEAN.value
        elif is_credit_card_number(value):
            dtype = ProfilerDataTypes.CREDIT_CARD_NUMBER.value
        elif is_zip_code(value):
            dtype = ProfilerDataTypes.ZIP_CODE.value
        elif is_int_like(value):
            dtype = ProfilerDataTypes.INT.value
        elif is_decimal(value):
            dtype = ProfilerDataTypes.DECIMAL.value
        elif is_datetime(value):
            dtype = ProfilerDataTypes.DATETIME.value
        elif is_missing(value):
            dtype = ProfilerDataTypes.MISSING.value
        elif is_str(value):
            if is_ip(value):
                dtype = ProfilerDataTypes.IP.value
            elif is_url(value):
                dtype = ProfilerDataTypes.URL.value
            elif is_email(value):
                dtype = ProfilerDataTypes.EMAIL.value
            elif is_gender(value):
                dtype = ProfilerDataTypes.GENDER.value
            elif is_phone_number(value):
                dtype = ProfilerDataTypes.PHONE_NUMBER.value
            else:
                dtype = ProfilerDataTypes.STRING.value
        elif is_object_value(value):
            dtype = ProfilerDataTypes.OBJECT.value

        return dtype

    def levenshtein(self, col_A, col_B):
        return jellyfish.levenshtein_distance(col_A,col_B)