import numpy as np
import dask.array as da
from dask_ml import preprocessing

from optimus.engines.base.commons.functions import string_to_index, index_to_string
from optimus.engines.base.pandas.columns import PandasBaseColumns
from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns
import dask

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

    def hist(self, cols="*", buckets=MAX_BUCKETS, compute=True):
        df = self.root
        cols = parse_columns(df, cols)

        result = {}

        for col_name in cols:
            series = self.F.to_float(df.data[col_name])
            result[col_name] = da.histogram(series, bins=buckets, range=[series.min(), series.max()])

        @self.F.delayed
        def format_hist(_cols):
            
            _result = {}
            for col_name in _cols:

                _count, _bins = _cols[col_name]

                dr = {}
                for i in range(len(_count)):
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
