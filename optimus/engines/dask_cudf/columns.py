from dask_ml import preprocessing

from optimus.engines.base.commons.functions import string_to_index, index_to_string, impute
from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns
from optimus.infer import Infer
from optimus.profiler.functions import fill_missing_var_types


class Cols(DaskBaseColumns):
    def __init__(self, df):
        super(DaskBaseColumns, self).__init__(df)

    def _names(self):
        return list(self.parent.data.columns)

    def _series_to_dict(self, series):
        return series.to_pandas().to_dict()

    def string_to_index(self, input_cols=None, output_cols=None, columns=None):
        df = self.parent
        le = preprocessing.LabelEncoder()
        return string_to_index(df, input_cols, output_cols, le)

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        df = self.parent
        le = preprocessing.LabelEncoder()
        return index_to_string(df, input_cols, output_cols, le)


    def count_by_dtypes(self, columns, infer=False, str_funcs=None, int_funcs=None, mismatch=None):
        df = self.parent
        columns = parse_columns(df, columns)
        dtypes = df.cols.dtypes()

        result = {}
        for col_name in columns:
            df_result = df[col_name].map_partitions(Infer.parse_dask, col_name, infer, dtypes, str_funcs,
                                                    int_funcs, meta=str).compute()

            result[col_name] = dict(df_result.value_counts())

        if infer is True:
            for k in result.keys():
                result[k] = fill_missing_var_types(result[k])
        else:
            result = self.parse_profiler_dtypes(result)

        return result

    def impute(self, input_cols, data_type="continuous", strategy="mean", output_cols=None):
        df = self.parent
        return impute(df, input_cols, data_type="continuous", strategy="mean", output_cols=None)
