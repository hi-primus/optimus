import functools
from abc import abstractmethod
import re
import dask
import dask.dataframe as dd
from dask.delayed import delayed
from dask.dataframe.core import map_partitions
from sklearn.preprocessing import MaxAbsScaler
from dask_ml.preprocessing import MinMaxScaler, StandardScaler

from optimus.helpers.core import one_tuple_to_val, val_to_list

from optimus.engines.base.distributed.functions import DistributedBaseFunctions


class DaskBaseFunctions(DistributedBaseFunctions):

    @property
    def _engine(self):
        return dask

    @property
    def _functions(self):
        return dd

    def _new_series(self, series, *args, **kwargs):
        if isinstance(series, dd.Series):
            return series
        return dd.from_array(series, *args, **kwargs)

    def compute(self, *args, **kwargs):
        result = dask.compute(*(*(a for a in args), *(kwargs[k] for k in kwargs)))
        return one_tuple_to_val(result)

    @abstractmethod
    def from_dataframe(self, dfd):
        pass

    @staticmethod
    def to_dataframe(dfd):
        return dfd.compute()

    def sort_df(self, dfd, cols, ascending):
        for c, a in list(zip(cols, ascending))[::-1]:
            dfd = dfd.sort_values(c)
            if not a:
                dfd = self.reverse_df(dfd)
        return dfd.reset_index(drop=True)

    def reverse_df(self, dfd):
        @delayed
        def reverse_pdf(pdf):
            return pdf[::-1]

        ds = dfd.to_delayed()
        ds = [reverse_pdf(d) for d in ds][::-1]
        return dd.from_delayed(ds)

    def to_float(self, series):
        if getattr(series, "map_partitions", False):
            return series.map_partitions(self._to_float)
        else:
            return self._to_float(series)

    def to_integer(self, series):
        if getattr(series, "map_partitions", False):
            return series.map_partitions(self._to_float)
        else:
            return self._to_float(series)

    def to_datetime(self, series):
        if getattr(series, "map_partitions", False):
            return series.map_partitions(self._to_datetime)
        else:
            return self._to_float(series)

    def all(self, series):
        return series.all()

    def any(self, series):
        return series.any()

    def duplicated(self, dfd, keep, subset):
        return self.from_dataframe(self.to_dataframe(dfd).duplicated(keep=keep, subset=subset))

    def word_tokenize(self, series):
        pass

    def delayed(self, func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def from_delayed(self, delayed):
        return dd.from_delayed(delayed)

    def to_delayed(self, value):
        return value.to_delayed()

    def mode(self, series):
        return self.to_float(series).mode().compute().tolist()

    def count_zeros(self, series):
        return int((self.to_float(series).values == 0).sum())

    def standard_scaler(self, series):
        dfd = StandardScaler().fit_transform(self.to_float(series).to_frame())
        return dfd[dfd.columns[0]]

    def max_abs_scaler(self, series):
        return MaxAbsScaler().fit_transform(self.compute(self.to_float(series)).values.reshape(-1,1))

    def min_max_scaler(self, series):
        dfd = MinMaxScaler().fit_transform(self.to_float(series).to_frame())
        return dfd[dfd.columns[0]]

    def _replace_chars(self, series, search, replace_by, ignore_case, is_regex=False):

        regex = is_regex
        replace_by = val_to_list(replace_by)

        if len(search) == 1:
            _search = search[0]
        else:
            regex = True
            if is_regex:
                _search = '|'.join(search)
            else:
                _search = '|'.join(map(re.escape, search))
            if ignore_case:
                _search = f"(?i){_search}"

        if len(replace_by) <= 1:
            _r = replace_by[0]
        else:
            regex = True
            _map = {s: r for s, r in zip(search, replace_by)}

            def _r(value):
                return _map.get(value[0], "ERROR")

        return self.to_string(series).replace(_search, _r, regex=regex)

    def replace_chars(self, series, search, replace_by, ignore_case):
        return self._replace_chars(series, search, replace_by, ignore_case, is_regex=False)
    
    def replace_regex_chars(self, series, search, replace_by, ignore_case):
        return self._replace_chars(series, search, replace_by, ignore_case, is_regex=True)

    def domain(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["domain"], na_action=None, meta=(series.name, "str")) 

    def top_domain(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["top_domain"], na_action=None, meta=(series.name, "str")) 

    def sub_domain(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["sub_domain"], na_action=None, meta=(series.name, "str")) 

    def url_scheme(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["protocol"], na_action=None, meta=(series.name, "str")) 

    def url_path(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["path"], na_action=None, meta=(series.name, "str")) 

    def url_file(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["file"], na_action=None, meta=(series.name, "str")) 

    def url_query(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["query"], na_action=None, meta=(series.name, "str")) 

    def url_fragment(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["fragment"], na_action=None, meta=(series.name, "str")) 

    def host(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["host"], na_action=None, meta=(series.name, "str")) 

    def port(self, series):
        import url_parser
        return self.to_string(series).map(lambda v: url_parser.parse_url(v)["port"], na_action=None, meta=(series.name, "str")) 
