import vaex

from optimus.engines.base.dataframe.columns import DataFrameBaseColumns


# @vaex.register_dataframe_accessor('cols', override=True)
from optimus.helpers.columns import parse_columns
from optimus.helpers.core import val_to_list


class Cols(DataFrameBaseColumns):
    def __init__(self, df):
        super().__init__(df)

    def _map(self, df, input_col, output_col, func, *args):
        return df.apply(func, arguments=(df[input_col], *args,), vectorize=False)

    def _names(self):
        dfd = self.root.data
        if isinstance(dfd, vaex.expression.Expression):
            return self.root.data.expression
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

    def agg_exprs(self, cols="*", funcs=None, *args, compute=True, tidy=True, parallel=True):

        df = self.root
        dfd = df.data
        funcs = val_to_list(funcs)
        cols = parse_columns(df, cols)
        print(dfd)
        result = {col:func(dfd[col]) for col in cols for func in funcs}
        # print(result)
        return result