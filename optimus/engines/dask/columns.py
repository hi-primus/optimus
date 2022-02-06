import builtins
import numpy as np
import dask.array as da
from dask_ml import preprocessing

from optimus.engines.base.commons.functions import string_to_index, index_to_string
from optimus.engines.base.pandas.columns import PandasBaseColumns
from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns
import dask
from optimus.infer import is_dict, is_list, is_tuple

from optimus.profiler.constants import MAX_BUCKETS


class Cols(PandasBaseColumns, DaskBaseColumns):
    def __init__(self, df):
        super().__init__(df)

    def _series_to_pandas(self, series):
        return series.compute()

    def _names(self):
        return list(self.root.data.columns)

    def string_to_index(self, cols=None, output_cols=None):
        df.le = df.le or preprocessing.LabelEncoder()
        return string_to_index(self, cols, output_cols, df.le)

    def index_to_string(self, cols=None, output_cols=None):
        df.le = df.le or preprocessing.LabelEncoder()
        return index_to_string(self, cols, output_cols, df.le)

    def hist(self, cols="*", buckets=MAX_BUCKETS, range=None, compute=True):
        df = self.root
        cols = parse_columns(df, cols)

        if range is not None and (is_tuple(range) or is_list(range)):
            _min = {col: range[0] for col in cols}
            _max = {col: range[1] for col in cols}
        elif range is not None and is_dict(range):
            _min = {col: range[col][0] for col in cols}
            _max = {col: range[col][1] for col in cols}
        else:
            _min = None
            _max = None

        result = {}

        for col_name in cols:
            series = self.F.to_float(df.data[col_name])
            _range = (_min[col_name], _max[col_name]) if _min is not None and _max is not None else (series.min(), series.max())
            result[col_name] = da.histogram(series, bins=buckets, range=_range)

        @self.F.delayed
        def format_hist(_cols):
            
            _result = {}
            for col_name in _cols:

                _count, _bins = _cols[col_name]

                dr = {}
                for i in builtins.range(len(_count)):
                    key = (float(_bins[i]), float(_bins[i + 1]))
                    if np.isnan(key[0]) and np.isnan(key[1]):
                        continue
                    dr[key] = dr.get(key, 0) + int(_count[i])

                r = [{"lower": k[0], "upper": k[1], "count": count} for k, count in dr.items()]
                if len(r):
                    _result[col_name] = r

            return {"hist": _result}

        result = format_hist(result)

        if compute:
            result = self.F.compute(result)

        return result
