import vaex

from optimus.engines.base.columns import BaseColumns
from optimus.helpers.constants import Actions


@vaex.register_dataframe_accessor('cols', override=True)
class Cols(BaseColumns):
    def __init__(self, df):
        self.df = df


    def frequency(self, columns, n=10, percentage=False, total_rows=None):

        return df

    @staticmethod
    def lower(input_cols, output_cols=None):
        def _lower(col, args):
            return F.lower(F.col(col))

        return Cols.apply(input_cols, _lower, filter_col_by_dtypes="string", output_cols=output_cols,
                          meta=Actions.LOWER.value)

    def mul(self, a):
        df = self.df.copy()
        for col in df.get_column_names(strings=False):
            if df[col].dtype:
                df[col] = df[col] * a
        return df

    def add(self, a):
        df = self.df.copy()
        for col in df.get_column_names(strings=False):
            if df[col].dtype:
                df[col] = df[col] + a
        return df
