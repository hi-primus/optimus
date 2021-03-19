from dask_ml import preprocessing

from optimus.engines.base.commons.functions import impute, string_to_index, index_to_string
from optimus.engines.base.dask.columns import DaskBaseColumns


class Cols(DaskBaseColumns):
    def __init__(self, df):
        super(DaskBaseColumns, self).__init__(df)

    def _names(self):
        return list(self.root.data.columns)

    def string_to_index(self, input_cols=None, output_cols=None, columns=None):
        le = preprocessing.LabelEncoder()
        return string_to_index(self, input_cols, output_cols, le)

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        le = preprocessing.LabelEncoder()
        return index_to_string(self, input_cols, output_cols, le)

    # def hist(self, columns="*", buckets=20, compute=True):
    #     df = self.df
    #     columns = parse_columns(df, columns)
    #
    #     result = {}
    #     for col_name in columns:
    #
    #         df_numeric = df[col_name].to_float()
    #
    #         if len(df_numeric) > 0:
    #             _count, _bins = da.histogram(df["id"].astype(int), bins=buckets, range=[1, 19])
    #             # _count, _bins = cp.histogram(df_numeric, buckets)
    #             result[col_name] = [
    #                 {"lower": float(_bins[i]), "upper": float(_bins[i + 1]), "count": int(_count[i])}
    #                 for i in range(buckets)]
    #
    #     return {"hist":result}

    def impute(self, input_cols, data_type="continuous", strategy="mean", fill_value=None,output_cols=None):
        df = self.root
        return impute(df, input_cols, data_type=data_type, strategy=strategy, output_cols=None)
