import dask

from optimus.engines.base.functions import Functions
from optimus.helpers.core import val_to_list
import dask.dataframe as dd

class DaskBaseFunctions():
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

    def to_float(self, series, *args):
        return series.map_partitions(self._to_float_partition, meta=float)

    def _to_float(self, series):
        return self.to_float(series)

    def _to_integer(self, series):
        return self.to_integer(series)

    def to_integer(self, series, *args):
        return series.map_partitions(self._to_integer_partition, meta=int)

    def to_string(self, series):
        return series.astype(str)

    def min(self, series):
        return series.min()

    def max(self, series):
        return series.max()

    def count_zeros(self, series):
        return int((self._to_float(series).values == 0).sum())

    def replace_chars(self, series, search, replace_by):
        replace_by = val_to_list(replace_by)
        for i, j in zip(search, replace_by):
            series = self.to_string_accessor(series).replace(i, j)
        return series
