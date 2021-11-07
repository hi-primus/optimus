from sklearn import preprocessing

from datatable import dt, cut, by, f
from optimus.engines.base.commons.functions import string_to_index, index_to_string, find
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.engines.base.pandas.columns import PandasBaseColumns
from optimus.helpers.columns import parse_columns
from optimus.profiler.constants import MAX_BUCKETS

DataFrame = dt.Frame


class Cols(PandasBaseColumns, DataFrameBaseColumns):
    def __init__(self, df):
        super().__init__(df)

    def _series_to_pandas(self, series):
        return series

    def _data_type(self):
        df = self.root
        result = {}
        for col_name in df.cols.names():
            result[col_name] = self.root.data[col_name].stype.dtype
        return result

    def _names(self):
        return list(self.root.data.names)

    def _select(self, cols):
        return self.root.data[:, cols]

    def find(self, cols="*", sub=None, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param cols:
        :param ignore_case:
        :param sub:
        :return:
        """
        df = self.root
        return find(df, cols, sub, ignore_case)

    def frequency(self, cols="*", n=MAX_BUCKETS, percentage=False, total_rows=None, count_uniques=False,
                  compute=True, tidy=False) -> dict:
        df = self.root

        result = {}

        cols = parse_columns(df, cols)

        for col_name in cols:
            _freq = df.data[:, dt.count(), by(col_name)].sort(-f.count)[0:10, :].to_dict()

            values = _freq[col_name]
            count = _freq["count"]

            result_col = []
            for i in range(0, len(values)):
                result_col.append({"values": values[i], "count": count[i]})
            result[col_name] = result_col

        return {"frequency": result}

    def hist(self, cols="*", buckets=20, compute=True):
        df = self.root
        dfd = df.data

        result = {}

        cols = parse_columns(df, cols)

        for col_name in cols:
            n = df.cols.to_integer(col_name).data[col_name]
            _hist = dfd[:, [n, cut(n, nbins=buckets)]][:, dt.count(), by(col_name)].sort(-f.count)[0:buckets,
                    :].to_dict()
            edges = _hist[col_name]
            hist = _hist["count"]

            result_col = []
            for i in range(0, len(edges) - 1):
                result_col.append({"lower": edges[i], "upper": edges[i + 1], "count": hist[i]})
            result[col_name] = result_col

        return {"hist": result}

    def to_timestamp(self, cols="*", date_format=None, output_cols=None):
        raise NotImplementedError('Not implemented yet')

    def astype(self, cols="*", output_cols=None, *args, **kwargs):
        raise NotImplementedError('Not implemented yet')

    def string_to_index(self, cols="*", output_cols=None):
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        df = string_to_index(df, cols, output_cols, df.le)

        return df

    def index_to_string(self, cols="*", output_cols=None):
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        df = index_to_string(df, cols, output_cols, df.le)

        return df
