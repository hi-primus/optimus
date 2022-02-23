import re
from abc import abstractmethod, ABC

import hidateinfer
import hiurlparser
import jellyfish
import numpy as np
import pandas as pd
from fastnumbers import fast_float, fast_int
from jsonschema._format import is_email
from metaphone import doublemetaphone

from optimus.helpers.constants import ProfilerDataTypes
from optimus.helpers.core import one_list_to_val, one_tuple_to_val, val_to_list
from optimus.helpers.logger import logger
from optimus.helpers.decorators import apply_to_categories
from optimus.infer import is_datetime_str, is_list, is_list_of_list, is_null, is_bool, \
    is_credit_card_number, is_zip_code, is_float_like, is_datetime, is_valid_datetime_format, \
    is_object_value, is_ip, is_url, is_missing, is_gender, is_list_of_int, is_list_of_str, \
    is_str, is_phone_number, is_int_like


# ^(?:(?P<protocol>[\w\d]+)(?:\:\/\/))?(?P<sub_domain>(?P<www>(?:www)?)(?:\.?)(?:(?:[\w\d-]+|\.)*?)?)(?:\.?)(?P<domain>[^./]+(?=\.))\.(?P<top_domain>com(?![^/|:?#]))?(?P<port>(:)(\d+))?(?P<path>(?P<dir>\/(?:[^/\r\n]+(?:/))+)?(?:\/?)(?P<file>[^?#\r\n]+)?)?(?:\#(?P<fragment>[^#?\r\n]*))?(?:\?(?P<query>.*(?=$)))*$

class BaseFunctions(ABC):
    """
    Functions for internal use or to be called using 'F': `from optimus.functions import F`
    Note: some methods needs to be static so they can be passed to a Dask worker.
    """

    _engine = None
    """The engine to be used for internal use:
        `pd` from `import pandas as pd` or `dask` from `import dask`
    """
    _partition_engine = None
    """(optional, defaults to `_engine`) The engine to be used for internal use
        in a distributed engine (if supported):
        `pd` from `import pandas as pd` which is used by `Dask`
    """

    def __init_subclass__(cls):

        if cls._partition_engine is None:
            cls._partition_engine = cls._engine

    def __init__(self, df=None):

        if self._engine is None:
            raise TypeError("Can't instantiate class without attribute '_engine'")

        if df is not None and getattr(df, "partitions", False):
            self.root = df
        else:
            self.root = None

    def __getattr__(self, name):
        type_msg = "" if self.root is None else f" using {type(self.root).__name__}"
        raise NotImplementedError(f"\"{name}\" is not available" + type_msg)

    @property
    def _functions(self):
        """
        Gets the set of functions available in the engine
        """
        return self._partition_engine

    @staticmethod
    def to_dict(_s):
        if hasattr(_s, "to_dict"):
            result = _s.to_dict()
        else:
            result = _s
        return result

    @staticmethod
    def to_list(_s):
        if hasattr(_s, "tolist"):
            return _s.tolist()
        else:
            return _s

    @staticmethod
    def to_list_one(_s):
        if hasattr(_s, "tolist"):
            return one_list_to_val(_s.tolist())
        else:
            return _s

    @property
    def n_partitions(self):
        if self.root is None:
            return 1
        return self.root.partitions()

    @staticmethod
    def delayed(func):
        """
        Convert function to a delayed function (Dummy method on unsupported engines)

        :param func:
        :return:
        """
        return func

    def from_delayed(self, delayed):
        """
        Convert delayed objects to a DataFrame or Series (Dummy method on unsupported engines)

        :param delayed:
        :return:
        """
        return delayed[0]

    def to_delayed(self, delayed):
        """
        Convert DataFrame or Series to a list of delayed objects (Dummy method on unsupported engines)

        :param delayed:
        :return:
        """
        return [delayed]

    def apply_delayed(self, series, func, *args, **kwargs):
        """
        Applies a function to a Series for every partition using apply

        :param series:
        :param func:
        :param args:
        :param kwargs:
        :return:
        """
        result = self.to_delayed(series)
        result = [partition.apply(func, *args, **kwargs) for partition in result]
        result = self.from_delayed(result)
        result.index = series.index
        return result

    def map_delayed(self, series, func, *args, **kwargs):
        """
        Applies a function to a Series for every partition using map

        :param series:
        :param func:
        :param args:
        :param kwargs:
        :return:
        """
        result = self.to_delayed(series)
        result = [partition.map(func, *args, **kwargs) for partition in result]
        result = self.from_delayed(result)
        result.index = series.index
        return result

    @property
    def constants(self):
        """

        :return:
        """
        from optimus.engines.base.constants import BaseConstants
        return BaseConstants()

    @staticmethod
    def sort_df(dfd, cols, ascending):
        """
        Sort rows taking into account one column

        :param dfd:
        :param cols:
        :param ascending:
        :return:
        """
        return dfd.sort_values(cols, ascending=ascending).reset_index(drop=True)

    @staticmethod
    def reverse_df(dfd):
        """
        Reverse rows
        """
        return dfd[::-1]

    @staticmethod
    def append(dfd, dfd2):
        """
        Append two dataframes
        """
        return dfd.append(dfd2)

    def _new_series(self, *args, **kwargs):
        """
        Creates a new series (also known as column)
        """
        return self._functions.Series(*args, **kwargs)

    def compute(self, *args, **kwargs):
        return one_tuple_to_val((*(a for a in args), *(kwargs[k] for k in kwargs)))

    @staticmethod
    def to_items(series) -> dict:
        """
        Convert series to a list of tuples [(index, value), ...]
        """
        df = series.reset_index()
        return df.to_dict(orient='split')['data']

    def _to_boolean(self, series):
        """
        Converts series to bool
        """
        return series.map(lambda v: bool(v), na_action=None).astype('bool')

    def to_boolean(self, series):
        return self._to_boolean(series)

    def _to_boolean_none(self, series):
        """
        Converts series to boolean
        """
        return series.map(lambda v: bool(v), na_action='ignore').astype('object')

    def to_boolean_none(self, series):
        return self._to_boolean_none(series)

    def _to_float(self, series):
        """
        Converts a series values to floats
        """
        try:
            return self._new_series(np.vectorize(fast_float)(series, default=np.nan).flatten())
        except:
            return self._new_series(self._functions.to_numeric(series, errors='coerce')).astype('float')

    def to_float(self, series):
        """
        Converts a series values to floats
        """
        return self._to_float(series)

    def _to_integer(self, series, default=0):
        """
        Converts a series values to integers
        :param series:
        :param default:
        :return:
        """
        try:
            return self._new_series(np.vectorize(fast_int)(series, default=default).flatten())
        except:
            return self._new_series(self._functions.to_numeric(series, errors='coerce').fillna(default)).astype('int')

    def to_integer(self, series, default=0):
        """
        Converts a series values to integers
        """
        return self._to_integer(series, default)

    def _to_datetime(self, series, format):
        """
        Converts a series values to datetimes
        """
        return series

    def to_datetime(self, series, format):
        """
        Converts a series values to datetimes
        """
        return self._to_datetime(series, format)

    @staticmethod
    def to_string(series):
        """
        Converts a series values to strings
        """
        return series.astype(str)

    @staticmethod
    def to_string_accessor(series):
        """
        Converts a series values to strings and returns it's main functions
        accessor
        """
        return series.astype(str).str

    @staticmethod
    def duplicated(dfd, keep, subset):
        """
        Mark the duplicated values in a DataFrame using a subset of columns
        """
        return dfd.duplicated(keep=keep, subset=subset)

    def impute(self, series, strategy, fill_value):
        """
        Impute missing values in a series
        """
        from sklearn.impute import SimpleImputer
        imputer = SimpleImputer(strategy=strategy, fill_value=fill_value)
        series_fit = series.dropna()
        if str(series.dtype) in self.constants.OBJECT_TYPES:
            series_fit = series_fit.astype(str)
        values = series_fit.values.reshape(-1, 1)
        if len(values):
            imputer.fit(values)
            return imputer.transform(series.fillna(np.nan).values.reshape(-1, 1))
        else:
            logger.warn("list to fit imputer is empty, try cols.fill_na instead.")
            return series

    # Aggregation
    def date_format(self, series):
        """
        Get the date format of a series
        """
        dtype = str(series.dtype)
        if dtype in self.constants.STRING_TYPES:
            series = series.astype(str)
            result = hidateinfer.infer(self.compute(series).values)
            if not is_valid_datetime_format(result) or True:
                result_series = self.date_formats(series)
                result = result_series.mode().head(1)[0]

                if not is_valid_datetime_format(result):
                    return False

            return result
        elif dtype in self.constants.DATETIME_INTERNAL_TYPES:
            return True

        return False

    def min(self, series, numeric=False, string=False):
        """
        Get the minimum value of a series
        """
        if numeric:
            series = self.to_float(series)

        if string or str(series.dtype) in self.constants.STRING_TYPES:
            return self.to_string(series.dropna()).min()
        else:
            return series.min()

    def max(self, series, numeric=False, string=False):
        """
        Get the maximum value of a series
        """
        if numeric:
            series = self.to_float(series)

        if string or str(series.dtype) in self.constants.STRING_TYPES:
            return self.to_string(series.dropna()).max()
        else:
            return series.max()

    def mean(self, series):
        """
        Get the mean value of a series
        """
        return self.to_float(series).mean()

    def mode(self, series):
        """
        Get the modal value of a series
        """
        if str(series.dtype) not in self.constants.NUMERIC_INTERNAL_TYPES:
            return self.delayed(self.to_list_one)(self.to_string(series.dropna()).mode())
        else:
            return self.delayed(self.to_list_one)(series.mode())

    @staticmethod
    def crosstab(series, other):
        """
        Compute a cross tabulation of two series
        """
        return pd.crosstab(series, other)

    def std(self, series):
        """
        Get the standard deviation of a series
        """
        return self.to_float(series).std()

    def sum(self, series):
        """
        Get the sum of a series
        """
        return self.to_float(series).sum()

    def prod(self, series):
        """
        Get the sum of a series
        """
        return self.to_float(series).prod()

    def cumsum(self, series):
        """
        Get the cumulative sum of a series
        """
        return self.to_float(series).cumsum()

    def cumprod(self, series):
        """
        Get the cumulative product of a series
        """
        return self.to_float(series).cumprod()

    def cummax(self, series):
        """
        Get the cumulative maximum of a series
        """
        return self.to_float(series).cummax()

    def cummin(self, series):
        """
        Get the cumulative minimum of a series
        """
        return self.to_float(series).cummin()

    def var(self, series):
        """
        Get the variance of a series
        """
        return self.to_float(series).var()

    def count_uniques(self, series, estimate=False):
        """
        Get the count of unique values in a series
        """
        return self.to_string(series).nunique()

    def unique_values(self, series, estimate=False):
        """
        Get the unique values in a series
        """
        # Cudf can not handle null so we fill it with non zero values.
        return self.to_string(series).unique()

    @staticmethod
    def count_na(series):
        """
        Get the count of missing values in a series
        """
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

    # import dask.dataframe as dd
    def mad(self, series, error=False, more=False, estimate=False):

        _series = self.to_float(series).dropna()

        if not estimate:
            _series = self.compute(_series)

        if not len(_series):
            return np.nan
        else:
            median_value = _series.quantile(0.5)
            mad_value = (_series - median_value).abs().quantile(0.5)
            if more:
                return {"mad": mad_value, "median": median_value}
            else:
                return mad_value

    # TODO: dask seems more efficient triggering multiple .min() task, one for every column
    # cudf seems to be calculate faster in on pass using df.min()
    def range(self, series):
        series = self.to_float(series)
        return {"min": series.min(), "max": series.max()}

    def percentile(self, series, values=0.5, error=False, estimate=False):

        _series = self.to_float(series).dropna()

        if not estimate:
            _series = self.compute(_series)

        if not len(_series):
            return np.nan
        else:
            return self.delayed(self.to_dict)(_series.quantile(values))

    # def radians(series):
    #     return series.to_float().radians()
    #
    # def degrees(series, *args):
    #     return call(series, method_name="degrees")

    ###########################

    def z_score(self, series):
        t = self.to_float(series)
        return t - t.mean() / t.std(ddof=0)

    def modified_z_score(self, series, estimate):
        series = self.to_float(series)

        _series = series.dropna()

        if not estimate:
            _series = self.compute(_series)

        if not len(_series):
            return np.nan
        else:
            median = _series.quantile(0.5)
            mad = (_series - median).abs().quantile(0.5)

            return abs(0.6745 * (series - median) / mad)

    def clip(self, series, lower_bound, upper_bound):
        return self.to_float(series).clip(float(lower_bound), float(upper_bound))

    @apply_to_categories
    def cut(self, series, bins, labels, default):
        if is_list_of_int(bins):
            return pd.cut(self.to_float(series), bins, include_lowest=True, labels=labels)
        elif is_list_of_str(bins):
            conditions = [series.str.contains(i) for i in bins]

            return np.select(conditions, labels, default=default)

    def qcut(self, series, quantiles):
        return pd.qcut(series, quantiles)

    def abs(self, series):
        return self.to_float(series).abs()

    def exp(self, series):
        return self.to_float(series).exp()

    def lemmatize_verbs(self, series):
        import nltk
        w_tokenizer = nltk.tokenize.WhitespaceTokenizer()
        lemmatizer = nltk.stem.WordNetLemmatizer()

        def lemmatize_verbs_map(text):
            return " ".join([lemmatizer.lemmatize(w, "v") for w in w_tokenizer.tokenize(text)])

        return self.map_delayed(self.to_string(series), lemmatize_verbs_map, na_action=None)

    def word_tokenize(self, series):
        import nltk
        w_tokenizer = nltk.tokenize.WhitespaceTokenizer()

        def lemmatize_verbs_map(text):
            return w_tokenizer.tokenize(text)

        return self.map_delayed(self.to_string(series), lemmatize_verbs_map, na_action=None, meta="object")

    @apply_to_categories
    def word_count(self, series):
        return self.word_tokenize(series).str.len()

    @apply_to_categories
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
    @apply_to_categories
    def match(self, series, regex):
        return self.to_string_accessor(series).match(regex)

    @apply_to_categories
    def lower(self, series):
        return self.to_string_accessor(series).lower()

    @apply_to_categories
    def upper(self, series):
        return self.to_string_accessor(series).upper()

    @apply_to_categories
    def title(self, series):
        return self.to_string_accessor(series).title()

    @apply_to_categories
    def capitalize(self, series):
        return self.to_string_accessor(series).capitalize()

    @apply_to_categories
    def pad(self, series, width, side, fillchar=""):
        return self.to_string_accessor(series).pad(width, side, fillchar)

    @apply_to_categories
    def extract(self, series, regex):
        return self.to_string_accessor(series).extract(regex)

    @apply_to_categories
    def slice(self, series, start, stop, step):
        return self.to_string_accessor(series).slice(start, stop, step)

    @apply_to_categories
    def trim(self, series):
        return self.to_string_accessor(series).strip()

    @apply_to_categories
    def strip_html(self, series):
        return self.to_string(series).replace('<.*?>', '', regex=True)

    def replace_chars(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        if ignore_case:
            str_regex = [r'(?i)%s' % re.escape(s) for s in search]
        else:
            str_regex = [r'%s' % re.escape(s) for s in search]
        return self.to_string(series).replace(str_regex, replace_by, regex=True)

    def replace_words(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        if ignore_case:
            str_regex = [r'(?i)\b%s\b' % re.escape(s) for s in search]
        else:
            str_regex = [r'\b%s\b' % re.escape(s) for s in search]
        return self.to_string(series).replace(str_regex, replace_by, regex=True)

    def replace_full(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        if ignore_case:
            str_regex = [r'(?i)^%s$' % re.escape(s) for s in search]
        else:
            str_regex = [r'^%s$' % re.escape(s) for s in search]
        return series.replace(str_regex, replace_by, regex=True)

    def replace_values(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)

        if ignore_case:
            regex = True
            search = [(r'(?i)%s' % re.escape(s)) for s in search]
        else:
            regex = False

        if is_list(replace_by) and is_list_of_list(search):
            for _s, _r in zip(search, replace_by):
                series = series.replace(_s, _r, regex=regex)

        else:
            series = series.replace(search, replace_by, regex=regex)

        return series

    def replace_regex_chars(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        if ignore_case:
            str_regex = [r'(?i)%s' % s for s in search]
        else:
            str_regex = [r'%s' % s for s in search]
        return self.to_string(series).replace(str_regex, replace_by, regex=True)

    def replace_regex_words(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        if ignore_case:
            str_regex = [r'(?i)\b%s\b' % s for s in search]
        else:
            str_regex = [r'\b%s\b' % s for s in search]
        return self.to_string(series).replace(str_regex, replace_by, regex=True)

    def replace_regex_full(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        if ignore_case:
            str_regex = [r'(?i)^%s$' % s for s in search]
        else:
            str_regex = [r'^%s$' % s for s in search]
        return series.replace(str_regex, replace_by, regex=True)

    @apply_to_categories
    def remove_numbers(self, series):
        return self.to_string_accessor(series).replace(r'\d+', '', regex=True)

    @apply_to_categories
    def remove_white_spaces(self, series):
        return self.to_string_accessor(series).replace(" ", "")

    @apply_to_categories
    def remove_urls(self, series):
        return self.to_string_accessor(series).replace(r"https?://\S+|www\.\S+", "", regex=True)

    @apply_to_categories
    def normalize_spaces(self, series):
        return self.to_string_accessor(series).replace(r" +", " ", regex=True)

    def expand_contractions(self, series):
        pass

    # @staticmethod
    # def len(series):
    #     return series.str.len()

    def normalize_chars(self, series):
        pass

    @apply_to_categories
    def find(self, sub, start=0, end=None):
        series = self.series
        return self.to_string_accessor(series).find(sub, start, end)

    @apply_to_categories
    def rfind(self, series, sub, start=0, end=None):
        return self.to_string_accessor(series).rfind(sub, start, end)

    @apply_to_categories
    def left(self, series, position):
        return self.to_string_accessor(series)[:position]

    @apply_to_categories
    def right(self, series, position):
        return self.to_string_accessor(series)[-1 * position:]

    @apply_to_categories
    def mid(self, series, start, end):
        return self.to_string_accessor(series)[start:end]

    @apply_to_categories
    def starts_with(self, series, pat):
        return self.to_string_accessor(series).startswith(pat)

    @apply_to_categories
    def ends_with(self, series, pat):
        return self.to_string_accessor(series).endswith(pat)

    @apply_to_categories
    def contains(self, series, value, case, flags, na, regex):
        return self.to_string_accessor(series).contains(value, case=case, flags=flags, na=na, regex=regex)

    @apply_to_categories
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
        Extract the year from a series of dates
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        return self.to_datetime(series, format=format).dt.year

    def month(self, series, format):
        """
        Extract the month from a series of dates
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        return self.to_datetime(series, format=format).dt.month

    def day(self, series, format):
        """
        Extract the day from a series of dates
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        return self.to_datetime(series, format=format).dt.day

    def hour(self, series, format):
        """
        Extract the hour from a series of dates
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        return self.to_datetime(series, format=format).dt.hour

    def minute(self, series, format):
        """
        Extract the minute from a series of dates
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        return self.to_datetime(series, format=format).dt.minute

    def second(self, series, format):
        """
        Extract the second from a series of dates
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        return self.to_datetime(series, format=format).dt.second

    def weekday(self, series, format):
        """
        Extract the weekday from a series of dates
        :param series:
        :param format: "%Y-%m-%d HH:mm:ss"
        :return:
        """
        return self.to_datetime(series, format=format).dt.weekday

    @staticmethod
    @abstractmethod
    def format_date(self, current_format=None, output_format=None):
        pass

    @staticmethod
    @abstractmethod
    def time_between(self, value=None, date_format=None):
        pass

    def years_between(self, series, value=None, date_format=None):
        return self.time_between(series, value, date_format).dt.days / 365.25

    def months_between(self, series, value=None, date_format=None):
        return self.time_between(series, value, date_format).dt.days / 30.436875

    def days_between(self, series, value=None, date_format=None):
        return self.time_between(series, value, date_format).dt.days

    def hours_between(self, series, value=None, date_format=None):
        series = self.time_between(series, value, date_format)
        return series.dt.days * 24.0 + series.dt.seconds / 3600.0

    def minutes_between(self, series, value=None, date_format=None):
        series = self.time_between(series, value, date_format)
        return series.dt.days * 1440.0 + series.dt.seconds / 60.0

    def seconds_between(self, series, value=None, date_format=None):
        series = self.time_between(series, value, date_format)
        return series.dt.days * 86400 + series.dt.seconds

    def domain(self, series):

        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["domain"], na_action=None)

    def top_domain(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["top_domain"], na_action=None)

    def sub_domain(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["sub_domain"], na_action=None)

    def url_scheme(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["protocol"], na_action=None)

    def url_path(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["path"], na_action=None)

    def url_file(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["file"], na_action=None)

    def url_query(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["query"], na_action=None)

    def url_fragment(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["fragment"], na_action=None)

    def host(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["host"], na_action=None)

    def port(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["port"], na_action=None)

    @apply_to_categories
    def email_username(self, series):
        return self.to_string_accessor(series).split('@').str[0]

    @apply_to_categories
    def email_domain(self, series):
        return self.to_string_accessor(series).split('@').str[1]

    def infer_data_types(self, value, cols_data_types):
        """
        Infer the data types.
        Please be aware that the order in which the value is checked is important and will change the final result
        :param value:
        :param cols_data_types:
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
        elif is_float_like(value):
            dtype = ProfilerDataTypes.FLOAT.value
        elif is_datetime(value):
            dtype = ProfilerDataTypes.DATETIME.value
        elif is_missing(value):
            dtype = ProfilerDataTypes.MISSING.value
        elif is_str(value):
            if is_datetime_str(value):
                dtype = ProfilerDataTypes.DATETIME.value
            elif is_ip(value):
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

    def date_formats(self, series):
        return series.map(lambda v: hidateinfer.infer([v]))

    def metaphone(self, series):
        return self.to_string(series).map(jellyfish.metaphone, na_action='ignore')

    def double_metaphone(self, series):
        return self.to_string(series).map(doublemetaphone, na_action='ignore')

    def nysiis(self, series):
        return self.to_string(series).map(jellyfish.nysiis, na_action='ignore')

    def match_rating_codex(self, series):
        return self.to_string(series).map(jellyfish.match_rating_codex, na_action='ignore')

    def soundex(self, series):
        return self.to_string(series).map(jellyfish.soundex, na_action='ignore')

    def levenshtein(self, series, other):
        if isinstance(other, str):
            return self.to_string(series).map(lambda v: jellyfish.levenshtein_distance(v, other))
        else:
            col_name = series.name
            other_name = other.name
            dfd = self.to_string(series).to_frame()
            dfd[other_name] = self.to_string(other)

            return dfd.apply(lambda d: jellyfish.levenshtein_distance(d[col_name], d[other_name]), axis=1).rename(
                col_name)
