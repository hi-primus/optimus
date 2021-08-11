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

    def hist(self, columns="*", buckets=MAX_BUCKETS, compute=True):
        df = self.root
        columns = parse_columns(df, columns)

        result = {}
        for col_name in columns:
            # dfd = df[col_name].cols.to_float().data
            dfd = df.data[col_name].astype("float")

            if len(dfd) > 0:
                _count, _bins = dask.compute(da.histogram(dfd, bins=buckets, range=[dfd.min(), dfd.max()]))[0]
                result[col_name] = [
                    {"lower": float(_bins[i]), "upper": float(_bins[i + 1]), "count": _count[i]}
                    for i in range(buckets)]

        return {"hist": result}
