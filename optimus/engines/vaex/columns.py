import numpy as np

from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
# @vaex.register_dataframe_accessor('cols', override=True)
from optimus.helpers.columns import parse_columns
from optimus.profiler.constants import MAX_BUCKETS


class Cols(DataFrameBaseColumns):
    def __init__(self, df):
        super().__init__(df)

    def _map(self, df, input_col, output_col, func, *args):
        return df.apply(func, arguments=(df[input_col], *args,), vectorize=False)

    def _names(self):
        return self.root.data.get_column_names(strings=True)

    def append(self, dfs):
        pass

    @staticmethod
    def impute(input_cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None):
        pass

    @staticmethod
    def string_to_index(cols=None, output_cols=None):
        pass

    @staticmethod
    def index_to_string(cols=None, output_cols=None):
        pass

    def hist(self, cols="*", buckets=20, compute=True):

        df = self.root

        result = []

        cols = parse_columns(df, cols)

        for col_name in cols:
            _min, _max = df.data[col_name].minmax()
            _hist = df.data.count(binby=df.data.MES, limits=[_min, _max], shape=buckets)
            edges = np.linspace(_min, _max, buckets)

            result_col = []
            for i in range(0, len(edges) - 1):
                result_col.append({"lower": edges[i], "upper": edges[i + 1], "count": _hist[i]})
            result = {col_name: result_col}
        return {"hist": result}

    def frequency(self, cols="*", n=MAX_BUCKETS, percentage=False, total_rows=None, count_uniques=False,
                  compute=True, tidy=False) -> dict:
        df = self.root

        result = {}

        cols = parse_columns(df, cols)

        for col_name in cols:
            _freq = df.data[col_name].value_counts(dropna=True, dropnan=True, dropmissing=True, ascending=False,
                                                   progress=False, axis=None)[0:n].to_dict()

            values = list(_freq.keys())
            count = list(_freq.values())

            result_col = []
            for i in range(0, len(values)):
                result_col.append({"values": values[i], "count": count[i]})
            result[col_name] = result_col
        return {"frequency": result}
