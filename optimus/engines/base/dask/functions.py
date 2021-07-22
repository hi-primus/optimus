import re
import dask

from optimus.helpers.core import val_to_list
import dask.dataframe as dd
from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler, StandardScaler

class DaskBaseFunctions():

    @property
    def _engine(self):
        return dask

    @property
    def _functions(self):
        return self._engine.dataframe

    def _new_series(self, *args, **kwargs):
        return self._functions.from_array(*args, **kwargs)

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

    def min(self, series):
        return series.min()

    def max(self, series):
        return series.max()

    def count_zeros(self, series):
        return int((self.to_float(series).values == 0).sum())

    def standard_scaler(self, series):
        # TO-DO: this creates a numpy array
        return StandardScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))

    def max_abs_scaler(self, series):
        # TO-DO: this creates a numpy array
        return MaxAbsScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))

    def min_max_scaler(self, series):
        # TO-DO: this creates a numpy array
        return MinMaxScaler().fit_transform(self.to_float(series).values.reshape(-1, 1))

    def replace_chars(self, series, search, replace_by):
        regex=False
        replace_by = val_to_list(replace_by)
        if len(search) == 1:
            _search = search[0]
        else:
            regex=True
            _search = ('|'.join(map(re.escape, search)))
        
        if len(replace_by) <= 1:
            _r = replace_by[0]
        else:
            regex=True
            _map = {s: r for s, r in zip(search, replace_by)}
            def _r(value):
                return _map.get(value[0], "ERROR")

        series = self.to_string_accessor(series).replace(_search, _r, regex=regex)
        return series
