import re
from abc import abstractmethod, ABC

import jellyfish
from metaphone import doublemetaphone
import numpy as np
import pandas as pd
from jsonschema._format import is_email
from fastnumbers import fast_float, fast_int

from optimus.helpers.constants import ProfilerDataTypes
from optimus.helpers.core import one_tuple_to_val, val_to_list
from optimus.infer import is_list, is_list_of_list, is_null, is_bool, \
    is_credit_card_number, is_zip_code, is_int, is_decimal, is_datetime, is_object_value, is_ip, is_url, is_missing, \
    is_gender, is_list_of_int, is_list_of_str, is_str, is_phone_number, is_int_like


# ^(?:(?P<protocol>[\w\d]+)(?:\:\/\/))?(?P<sub_domain>(?P<www>(?:www)?)(?:\.?)(?:(?:[\w\d-]+|\.)*?)?)(?:\.?)(?P<domain>[^./]+(?=\.))\.(?P<top_domain>com(?![^/|:?#]))?(?P<port>(:)(\d+))?(?P<path>(?P<dir>\/(?:[^/\r\n]+(?:/))+)?(?:\/?)(?P<file>[^?#\r\n]+)?)?(?:\#(?P<fragment>[^#?\r\n]*))?(?:\?(?P<query>.*(?=$)))*$

class BaseFunctions(ABC):
    """
    Functions for internal use or to be called using 'F': `from optimus.functions import F`
    Note: some methods needs to be static so they can be passed to a Dask worker.
    """

    def __init__(self, df):
        if df is not None and getattr(df, "partitions", False):
            self.root = df
        else:
            self.root = None

    def __getattr__(self, name):
        type_msg = "" if self.root is None else f" using {type(self.root).__name__}"
        raise NotImplementedError(f"\"{name}\" is not available"+type_msg)

    @property
    def n_partitions(self):
        if self.root is None:
            return 1
        return self.root.partitions()

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    def from_delayed(self, delayed):
        return delayed[0]

    def to_delayed(self, delayed):
        return [delayed]

    
    @property
    def constants(self):
        from optimus.engines.base.constants import BaseConstants
        return BaseConstants()

    @property
    @abstractmethod
    def _engine(self):
        """
        Gets the engine used
        """
        pass

    @property
    def _partition_engine(self):
        """
        Gets the internal engine used in partitioned DataFrame technologies
        """
        return self._engine

    @property
    def _functions(self):
        """
        Gets the set of functions available in the engine
        """
        return self._partition_engine

    @staticmethod
    def sort_df(dfd, cols, ascending):
        """
        Sort rows taking into account one column
        """
        return dfd.sort_values(cols, ascending=ascending)

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
    def to_dict(series) -> dict:
        """
        Convert series to a Python dictionary
        """
        return series.to_dict()

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
        return self._to_float(series)

    def _to_integer(self, series, default=0):
        """
        Converts a series values to integers
        """
        try:
            return self._new_series(np.vectorize(fast_int)(series, default=default).flatten())
        except:
            return self._new_series(self._functions.to_numeric(series, errors='coerce').fillna(default)).astype('int')

    def to_integer(self, series, default=0):
        return self._to_integer(series, default)

    def _to_datetime(self, series, format):
        return series
    
    def to_datetime(self, series, format):
        return self._to_datetime(series, format)

    def to_string(self, series):
        return series.astype(str)

    def to_string_accessor(self, series):
        return series.astype(str).str


    @staticmethod
    def duplicated(dfd, keep, subset):
        return dfd.duplicated(keep=keep, subset=subset)

    @staticmethod
    def impute(series, strategy, fill_value):
        from dask_ml.impute import SimpleImputer
        imputer = SimpleImputer(strategy=strategy, fill_value=fill_value)
        return imputer.fit_transform(series.values.reshape(-1, 1))

    # Aggregation

    def date_format(self, series):
        dtype = str(series.dtype)
        if dtype in self.constants.STRING_TYPES:
            import pydateinfer
            return pydateinfer.infer(self.compute(series).values)
        elif dtype in self.constants.DATETIME_TYPES:
            return True

        return False
        
    def min(self, series, numeric=False, string=False):
        if numeric:
            series = self.to_float(series)
        
        if string or str(series.dtype) in self.constants.STRING_TYPES:
            return self.to_string(series.dropna()).min()
        else:
            return series.min()

    def max(self, series, numeric=False, string=False):
        
        if numeric:
            series = self.to_float(series)

        if string or str(series.dtype) in self.constants.STRING_TYPES:
            return self.to_string(series.dropna()).max()
        else:
            return series.max()

    def mean(self, series):
        return self.to_float(series).mean()

    def mode(self, series):
        return self.to_float(series).mode().tolist()

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

    def unique_values(self, series, *args):
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
        series = self.to_float(series)
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
            return " ".join([lemmatizer.lemmatize(w, 'v') for w in w_tokenizer.tokenize(text)])

        return self.to_string(series).map(lemmatize_verbs_map, na_action=None)

    @abstractmethod
    def word_tokenize(self, series):
        pass

    def word_count(self, series):
        return self.word_tokenize(series).str.len()

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

    def trim(self, series):
        return self.to_string_accessor(series).strip()

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
                series.replace(_s, _r, inplace=True, regex=regex)
                
        else:
            series.replace(search, replace_by, inplace=True, regex=regex)

        return series

    def replace_regex_chars(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        return self.to_string_accessor(series).replace(search, replace_by, regex=True, case=not ignore_case)

    def replace_regex_words(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        str_regex = [r'\b%s\b' % s for s in search]
        return self.to_string_accessor(series).replace(str_regex, replace_by, regex=True, case=not ignore_case)

    def replace_regex_full(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        str_regex = [r'^%s$' % s for s in search]
        return self.to_string_accessor(series).replace(str_regex, replace_by, regex=True, case=not ignore_case)

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
    def format_date(self, current_format=None, output_format=None):
        pass

    @staticmethod
    @abstractmethod
    def days_between(self, date_format=None):
        pass

    def domain(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["domain"], na_action=None) 

    def top_domain(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["top_domain"], na_action=None) 

    def sub_domain(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["sub_domain"], na_action=None) 

    def url_scheme(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["protocol"], na_action=None) 

    def url_path(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["path"], na_action=None) 

    def url_file(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["file"], na_action=None) 

    def url_query(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["query"], na_action=None) 

    def url_fragment(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["fragment"], na_action=None) 

    def host(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["host"], na_action=None) 

    def port(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["port"], na_action=None) 

    def email_username(self, series):
        return self.to_string_accessor(series).split('@').str[0]

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

    def date_formats(self, series):
        import pydateinfer
        return series.map(lambda v: pydateinfer.infer([v]))
    
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

            return dfd.apply(lambda d: jellyfish.levenshtein_distance(d[col_name], d[other_name]), axis=1).rename(col_name)
            
