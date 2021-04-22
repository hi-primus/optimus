from optimus.engines.base.dataframe.columns import DataFrameBaseColumns


# @vaex.register_dataframe_accessor('cols', override=True)
class Cols(DataFrameBaseColumns):
    def __init__(self, df):
        super(DataFrameBaseColumns, self).__init__(df)

    def _names(self):
        return self.root.data.get_column_names(strings=True)

    def append(self, dfs):
        pass

    @staticmethod
    def impute(input_cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None):
        pass

    @staticmethod
    def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None):
        pass

    @staticmethod
    def string_to_index(input_cols=None, output_cols=None, columns=None):
        pass

    @staticmethod
    def index_to_string(input_cols=None, output_cols=None, columns=None):
        pass
