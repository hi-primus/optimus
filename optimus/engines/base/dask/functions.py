from abc import abstractmethod

import dask
import dask.dataframe as dd
import hiurlparser
import numpy as np
from dask.delayed import delayed
from dask_ml.preprocessing import MinMaxScaler, StandardScaler
from sklearn.preprocessing import MaxAbsScaler
from dask_ml.impute import SimpleImputer
from optimus.engines.base.distributed.functions import DistributedBaseFunctions
from optimus.helpers.core import one_tuple_to_val


class DaskBaseFunctions(DistributedBaseFunctions):
    _engine = dask

    @staticmethod
    @property
    def _functions(self):
        return dd

    @staticmethod
    def _new_series(series, *args, **kwargs):
        if isinstance(series, dd.Series):
            return series
        return dd.from_array(series, *args, **kwargs)

    @staticmethod
    def compute(*args, **kwargs):
        result = dask.compute(*(*(a for a in args), *(kwargs[k] for k in kwargs)))
        return one_tuple_to_val(result)

    @abstractmethod
    def from_dataframe(self, dfd):
        pass

    @staticmethod
    def to_dataframe(dfd):
        return dfd.compute()

    @staticmethod
    def df_concat(df_list):
        return dd.concat(df_list, axis=0)

    def new_df(self, *args, **kwargs):
        if len(args) < 4 and all([k not in kwargs for k in ['dsk', 'name', 'meta', 'divisions']]):
            return self.from_dataframe(self._partition_engine.DataFrame(*args, **kwargs))
        return dd.DataFrame(*args, **kwargs)

    @staticmethod
    def dask_to_compatible(dfd):
        return dfd

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
            return self.map_partitions(series, self._to_float)
        else:
            return self._to_float(series)

    def to_integer(self, series, default=0):
        if getattr(series, "map_partitions", False):
            return self.map_partitions(series, self._to_integer, default=default)
        else:
            return self._to_integer(series, default=default)

    def to_datetime(self, series):
        if getattr(series, "map_partitions", False):
            return self.map_partitions(series, self._to_datetime)
        else:
            return self._to_float(series)

    def duplicated(self, dfd, keep, subset):
        return self.from_dataframe(self.to_dataframe(dfd).duplicated(keep=keep, subset=subset))

    def impute(self, series, strategy, fill_value):

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

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def from_delayed(self, delayed):
        return dd.from_delayed(delayed)

    def to_delayed(self, value):
        return value.to_delayed()

    def mode(self, series):
        # Uses delayed wrapper to avoid unexpected Dask errors
        # TODO should this be done in every static function called from Dask instead?
        @self.delayed
        def compute_mode(series):
            # dfd = series.rename(0).value_counts().reset_index(drop=False)
            # _max = dfd[0].max(skipna=True)
            # return dfd[dfd[0] == _max]['index'].rename(series.name)
            return series.mode()

        return compute_mode(series)

    def count_zeros(self, series):
        return int((self.to_float(series).values == 0).sum())

    def standard_scaler(self, series):
        dfd = StandardScaler().fit_transform(self.to_float(series).to_frame())
        return dfd[dfd.columns[0]]

    def max_abs_scaler(self, series):
        return MaxAbsScaler().fit_transform(self.compute(self.to_float(series)).values.reshape(-1, 1))

    def min_max_scaler(self, series):
        dfd = MinMaxScaler().fit_transform(self.to_float(series).to_frame())
        return dfd[dfd.columns[0]]

    # @staticmethod
    # def heatmap(df, bins):
    #     counts, edges = da.histogramdd((df['x'], df['y'].values), bins=bins)
    #     return counts, edges[0], edges[1]

    def domain(self, series):

        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["domain"], na_action=None,
                                          meta=(series.name, "str"))

    def top_domain(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["top_domain"], na_action=None,
                                          meta=(series.name, "str"))

    def sub_domain(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["sub_domain"], na_action=None,
                                          meta=(series.name, "str"))

    def url_scheme(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["protocol"], na_action=None,
                                          meta=(series.name, "str"))

    def url_path(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["path"], na_action=None,
                                          meta=(series.name, "str"))

    def url_file(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["file"], na_action=None,
                                          meta=(series.name, "str"))

    def url_query(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["query"], na_action=None,
                                          meta=(series.name, "str"))

    def url_fragment(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["fragment"], na_action=None,
                                          meta=(series.name, "str"))

    def host(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["host"], na_action=None,
                                          meta=(series.name, "str"))

    def port(self, series):
        return self.to_string(series).map(lambda v: hiurlparser.parse_url(v)["port"], na_action=None,
                                          meta=(series.name, "str"))
